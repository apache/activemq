/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import java.util.Iterator;
import java.util.Set;

/**
 * 
 * @version $Revision: 1.12 $
 */
public class TopicRegion extends AbstractRegion {
    private static final Log log = LogFactory.getLog(TopicRegion.class);
    
    protected final ConcurrentHashMap durableSubscriptions = new ConcurrentHashMap();
    private final PolicyMap policyMap;

    public TopicRegion(Broker broker,DestinationStatistics destinationStatistics, UsageManager memoryManager, TaskRunnerFactory taskRunnerFactory,
            PersistenceAdapter persistenceAdapter, PolicyMap policyMap) {
        super(broker,destinationStatistics, memoryManager, taskRunnerFactory, persistenceAdapter);
        this.policyMap = policyMap;
    }

    public void addConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        if (info.isDurable()) {
            SubscriptionKey key = new SubscriptionKey(context.getClientId(), info.getSubcriptionName());
            DurableTopicSubscription sub = (DurableTopicSubscription) durableSubscriptions.get(key);
            if (sub != null) {

                if (sub.isActive()) {
                    throw new JMSException("Durable consumer is in use");
                }

                // Has the selector changed??
                if (hasDurableSubChanged(info, sub.getConsumerInfo())) {

                    // Remove the consumer first then add it.
                    durableSubscriptions.remove(key);
                    for (Iterator iter = destinations.values().iterator(); iter.hasNext();) {
                        Topic topic = (Topic) iter.next();
                        topic.deleteSubscription(context, key);
                    }
                    super.removeConsumer(context, sub.getConsumerInfo());

                    super.addConsumer(context, info);

                }
                else {
                    // Change the consumer id key of the durable sub.
                    if( sub.getConsumerInfo().getConsumerId()!=null )
                        subscriptions.remove(sub.getConsumerInfo().getConsumerId());
                    subscriptions.put(info.getConsumerId(), sub);
                    sub.activate(context, info);
                }
            }
            else {
                super.addConsumer(context, info);
            }
        }
        else {
            super.addConsumer(context, info);
        }
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable {
        if (info.isDurable()) {

            SubscriptionKey key = new SubscriptionKey(context.getClientId(), info.getSubcriptionName());
            DurableTopicSubscription sub = (DurableTopicSubscription) durableSubscriptions.get(key);
            if (sub != null) {
                sub.deactivate();
            }

        }
        else {
            super.removeConsumer(context, info);
        }
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Throwable {
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
    protected Destination createDestination(ActiveMQDestination destination) throws Throwable {
        TopicMessageStore store = persistenceAdapter.createTopicMessageStore((ActiveMQTopic) destination);
        Topic topic = new Topic(destination, store, memoryManager, destinationStatistics, taskRunnerFactory);
        configureTopic(topic, destination);
        
        // Eagerly recover the durable subscriptions
        if (store != null) {            
            SubscriptionInfo[] infos = store.getAllSubscriptions();
            for (int i = 0; i < infos.length; i++) {
                log.info("Restoring durable subscription: "+infos[i]);
                createDurableSubscription(infos[i]);
            }            
        }
        
        return topic;
    }

    protected void configureTopic(Topic topic, ActiveMQDestination destination) {
        if (policyMap != null) {
            PolicyEntry entry = policyMap.getEntryFor(destination);
            if (entry != null) {
                entry.configure(topic);
            }
        }
    }

    protected Subscription createSubscription(ConnectionContext context, ConsumerInfo info) throws JMSException {
        if (info.isDurable()) {
            SubscriptionKey key = new SubscriptionKey(context.getClientId(), info.getSubcriptionName());
            DurableTopicSubscription sub = (DurableTopicSubscription) durableSubscriptions.get(key);
            if (sub == null) {
                sub = new DurableTopicSubscription(broker,context, info);
                durableSubscriptions.put(key, sub);
            }
            else {
                throw new JMSException("That durable subscription is already active.");
            }
            return sub;
        }
        else {
            return new TopicSubscription(broker,context, info, memoryManager);
        }
    }
    
    public Subscription createDurableSubscription(SubscriptionInfo info) throws JMSException {
        SubscriptionKey key = new SubscriptionKey(info.getClientId(), info.getSubcriptionName());
        DurableTopicSubscription sub = (DurableTopicSubscription) durableSubscriptions.get(key);
        sub = new DurableTopicSubscription(broker,info);
        durableSubscriptions.put(key, sub);
        return sub;
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

}
