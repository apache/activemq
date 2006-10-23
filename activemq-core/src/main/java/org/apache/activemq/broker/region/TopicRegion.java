/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.region;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * @version $Revision: 1.12 $
 */
public class TopicRegion extends AbstractRegion {
    private static final Log log = LogFactory.getLog(TopicRegion.class);
    protected final ConcurrentHashMap durableSubscriptions = new ConcurrentHashMap();
    private final LongSequenceGenerator recoveredDurableSubIdGenerator = new LongSequenceGenerator();
    private final SessionId recoveredDurableSubSessionId = new SessionId(new ConnectionId("OFFLINE"), recoveredDurableSubIdGenerator.getNextSequenceId()); 
    private boolean keepDurableSubsActive=false;

    public TopicRegion(RegionBroker broker,DestinationStatistics destinationStatistics, UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory,
            DestinationFactory destinationFactory) {
        super(broker,destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
        
    }

    public Subscription addConsumer(ConnectionContext context,ConsumerInfo info) throws Exception{
        if(info.isDurable()){
            ActiveMQDestination destination=info.getDestination();
            if(!destination.isPattern()){
                // Make sure the destination is created.
                lookup(context,destination);
            }
            String clientId=context.getClientId();
            String subcriptionName=info.getSubscriptionName();
            SubscriptionKey key=new SubscriptionKey(clientId,subcriptionName);
            DurableTopicSubscription sub=(DurableTopicSubscription)durableSubscriptions.get(key);
            if(sub!=null){
                if(sub.isActive()){
                    throw new JMSException("Durable consumer is in use for client: "+clientId+" and subscriptionName: "
                            +subcriptionName);
                }
                // Has the selector changed??
                if(hasDurableSubChanged(info,sub.getConsumerInfo())){
                    // Remove the consumer first then add it.
                    durableSubscriptions.remove(key);
                    for(Iterator iter=destinations.values().iterator();iter.hasNext();){
                        Topic topic=(Topic)iter.next();
                        topic.deleteSubscription(context,key);
                    }
                    super.removeConsumer(context,sub.getConsumerInfo());
                    super.addConsumer(context,info);
                    sub=(DurableTopicSubscription)durableSubscriptions.get(key);
                }else{
                    // Change the consumer id key of the durable sub.
                    if(sub.getConsumerInfo().getConsumerId()!=null)
                        subscriptions.remove(sub.getConsumerInfo().getConsumerId());
                    subscriptions.put(info.getConsumerId(),sub);
                }
            }else{
                super.addConsumer(context,info);
                sub=(DurableTopicSubscription)durableSubscriptions.get(key);
                if(sub==null){
                    throw new JMSException("Cannot use the same consumerId: "+info.getConsumerId()
                            +" for two different durable subscriptions clientID: "+key.getClientId()
                            +" subscriberName: "+key.getSubscriptionName());
                }
            }
            sub.activate(context,info);
            return sub;
        }else{
            return super.addConsumer(context,info);
        }
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        if (info.isDurable()) {

            SubscriptionKey key = new SubscriptionKey(context.getClientId(), info.getSubscriptionName());
            DurableTopicSubscription sub = (DurableTopicSubscription) durableSubscriptions.get(key);
            if (sub != null) {
                sub.deactivate(keepDurableSubsActive);
            }

        }
        else {
            super.removeConsumer(context, info);
        }
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        SubscriptionKey key = new SubscriptionKey(info.getClientId(), info.getSubcriptionName());
        DurableTopicSubscription sub = (DurableTopicSubscription) durableSubscriptions.get(key);
        if (sub == null) {
            throw new InvalidDestinationException("No durable subscription exists for: " + info.getSubcriptionName());
        }
        if (sub.isActive()) {
            throw new JMSException("Durable consumer is in use");
        }

        durableSubscriptions.remove(key);
        for (Iterator iter = destinations.values().iterator(); iter.hasNext();) {
            Topic topic = (Topic) iter.next();
            topic.deleteSubscription(context, key);
        }
        super.removeConsumer(context, sub.getConsumerInfo());
    }

    public String toString() {
        return "TopicRegion: destinations=" + destinations.size() + ", subscriptions=" + subscriptions.size() + ", memory=" + memoryManager.getPercentUsage()
                + "%";
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Destination createDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        Topic topic = (Topic) super.createDestination(context, destination);
 
        recoverDurableSubscriptions(context, topic);
        
        return topic;
    }

    private void recoverDurableSubscriptions(ConnectionContext context, Topic topic) throws IOException, JMSException, Exception {
        TopicMessageStore store = (TopicMessageStore) topic.getMessageStore();
        // Eagerly recover the durable subscriptions
        if (store != null) {            
            SubscriptionInfo[] infos = store.getAllSubscriptions();
            for (int i = 0; i < infos.length; i++) {
                
                SubscriptionInfo info = infos[i];
                log.debug("Restoring durable subscription: "+infos);
                SubscriptionKey key = new SubscriptionKey(info);
                
                // A single durable sub may be subscribing to multiple topics.  so it might exist already.
                DurableTopicSubscription sub = (DurableTopicSubscription) durableSubscriptions.get(key);
                ConsumerInfo consumerInfo = createInactiveConsumerInfo(info);
                if( sub == null ) { 
                    ConnectionContext c = new ConnectionContext();
                    c.setBroker(context.getBroker());
                    c.setClientId(key.getClientId());
                    c.setConnectionId(consumerInfo.getConsumerId().getParentId().getParentId());
                    sub = (DurableTopicSubscription) createSubscription(c, consumerInfo );
                }
                
                topic.addSubscription(context, sub);
            }            
        }
    }
    
    private ConsumerInfo createInactiveConsumerInfo(SubscriptionInfo info) {
        ConsumerInfo rc = new ConsumerInfo();
        rc.setSelector(info.getSelector());
        rc.setSubscriptionName(info.getSubcriptionName());
        rc.setDestination(info.getDestination());
        rc.setConsumerId(createConsumerId());
        return rc;
    }

    private ConsumerId createConsumerId() {
        return new ConsumerId(recoveredDurableSubSessionId,recoveredDurableSubIdGenerator.getNextSequenceId());
    }

    protected void configureTopic(Topic topic, ActiveMQDestination destination) {
        if (broker.getDestinationPolicy() != null) {
            PolicyEntry entry = broker.getDestinationPolicy().getEntryFor(destination);
            if (entry != null) {
                entry.configure(topic);
            }
        }
    }

    protected Subscription createSubscription(ConnectionContext context, ConsumerInfo info) throws JMSException {
        if (info.isDurable()) {
            if (AdvisorySupport.isAdvisoryTopic(info.getDestination())){
                throw new JMSException("Cannot create a durable subscription for an advisory Topic");
            }
            SubscriptionKey key = new SubscriptionKey(context.getClientId(), info.getSubscriptionName());
            DurableTopicSubscription sub = (DurableTopicSubscription) durableSubscriptions.get(key);
            if(sub==null){
                PendingMessageCursor cursor=broker.getPendingDurableSubscriberPolicy().getSubscriberPendingMessageCursor(
                        context.getClientId(),info.getSubscriptionName(),broker.getTempDataStore(),
                        info.getPrefetchSize());
                sub=new DurableTopicSubscription(broker,context,info,keepDurableSubsActive,cursor);
                durableSubscriptions.put(key,sub);
            }
            else {
                throw new JMSException("That durable subscription is already active.");
            }
            return sub;
        }
        else {
            TopicSubscription answer = new TopicSubscription(broker,context, info, memoryManager);
            
            // lets configure the subscription depending on the destination
            ActiveMQDestination destination = info.getDestination();
            if (destination != null && broker.getDestinationPolicy() != null) {
                PolicyEntry entry = broker.getDestinationPolicy().getEntryFor(destination);
                if (entry != null) {
                    entry.configure(answer);
                }
            }
            return answer;
        }
    }

    /**
     */
    private boolean hasDurableSubChanged(ConsumerInfo info1, ConsumerInfo info2) {
        if (info1.getSelector() != null ^ info2.getSelector() != null)
            return true;
        if (info1.getSelector() != null && !info1.getSelector().equals(info2.getSelector()))
            return true;
        return !info1.getDestination().equals(info2.getDestination());
    }

    protected Set getInactiveDestinations() {
        Set inactiveDestinations = super.getInactiveDestinations();
        for (Iterator iter = inactiveDestinations.iterator(); iter.hasNext();) {
            ActiveMQDestination dest = (ActiveMQDestination) iter.next();
            if (!dest.isTopic())
                iter.remove();
        }
        return inactiveDestinations;
    }

    public boolean isKeepDurableSubsActive() {
        return keepDurableSubsActive;
    }

    public void setKeepDurableSubsActive(boolean keepDurableSubsActive) {
        this.keepDurableSubsActive = keepDurableSubsActive;
    }

}
