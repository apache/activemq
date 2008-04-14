/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.region;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.12 $
 */
public class TopicRegion extends AbstractRegion {
    private static final Log LOG = LogFactory.getLog(TopicRegion.class);
    protected final ConcurrentHashMap<SubscriptionKey, DurableTopicSubscription> durableSubscriptions = new ConcurrentHashMap<SubscriptionKey, DurableTopicSubscription>();
    private final LongSequenceGenerator recoveredDurableSubIdGenerator = new LongSequenceGenerator();
    private final SessionId recoveredDurableSubSessionId = new SessionId(new ConnectionId("OFFLINE"), recoveredDurableSubIdGenerator.getNextSequenceId());
    private boolean keepDurableSubsActive;

    public TopicRegion(RegionBroker broker, DestinationStatistics destinationStatistics, SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory,
                       DestinationFactory destinationFactory) {
        super(broker, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);

    }

    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        if (info.isDurable()) {
            ActiveMQDestination destination = info.getDestination();
            if (!destination.isPattern()) {
                // Make sure the destination is created.
                lookup(context, destination);
            }
            String clientId = context.getClientId();
            String subscriptionName = info.getSubscriptionName();
            SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
            DurableTopicSubscription sub = durableSubscriptions.get(key);
            if (sub != null) {
                if (sub.isActive()) {
                    throw new JMSException("Durable consumer is in use for client: " + clientId + " and subscriptionName: " + subscriptionName);
                }
                // Has the selector changed??
                if (hasDurableSubChanged(info, sub.getConsumerInfo())) {
                    // Remove the consumer first then add it.
                    durableSubscriptions.remove(key);
                    synchronized (destinationsMutex) {
                        for (Iterator<Destination> iter = destinations.values().iterator(); iter.hasNext();) {
                            Topic topic = (Topic)iter.next();
                            topic.deleteSubscription(context, key);
                        }
                    }
                    super.removeConsumer(context, sub.getConsumerInfo());
                    super.addConsumer(context, info);
                    sub = durableSubscriptions.get(key);
                } else {
                    // Change the consumer id key of the durable sub.
                    if (sub.getConsumerInfo().getConsumerId() != null) {
                        subscriptions.remove(sub.getConsumerInfo().getConsumerId());
                    }
                    subscriptions.put(info.getConsumerId(), sub);
                }
            } else {
                super.addConsumer(context, info);
                sub = durableSubscriptions.get(key);
                if (sub == null) {
                    throw new JMSException("Cannot use the same consumerId: " + info.getConsumerId() + " for two different durable subscriptions clientID: " + key.getClientId()
                                           + " subscriberName: " + key.getSubscriptionName());
                }
            }
            sub.activate(usageManager, context, info);
            return sub;
        } else {
            return super.addConsumer(context, info);
        }
    }

    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        if (info.isDurable()) {

            SubscriptionKey key = new SubscriptionKey(context.getClientId(), info.getSubscriptionName());
            DurableTopicSubscription sub = durableSubscriptions.get(key);
            if (sub != null) {
                sub.deactivate(keepDurableSubsActive);
            }

        } else {
            super.removeConsumer(context, info);
        }
    }

    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        SubscriptionKey key = new SubscriptionKey(info.getClientId(), info.getSubscriptionName());
        DurableTopicSubscription sub = durableSubscriptions.get(key);
        if (sub == null) {
            throw new InvalidDestinationException("No durable subscription exists for: " + info.getSubscriptionName());
        }
        if (sub.isActive()) {
            throw new JMSException("Durable consumer is in use");
        }

        durableSubscriptions.remove(key);
        synchronized (destinationsMutex) {
            for (Iterator<Destination> iter = destinations.values().iterator(); iter.hasNext();) {
                Topic topic = (Topic)iter.next();
                topic.deleteSubscription(context, key);
            }
        }
        super.removeConsumer(context, sub.getConsumerInfo());
    }

    public String toString() {
        return "TopicRegion: destinations=" + destinations.size() + ", subscriptions=" + subscriptions.size() + ", memory=" + usageManager.getMemoryUsage().getPercentUsage() + "%";
    }

    @Override
    protected List<Subscription> addSubscriptionsForDestination(ConnectionContext context, Destination dest) throws Exception {

        List<Subscription> rc = super.addSubscriptionsForDestination(context, dest);
        Set<Subscription> dupChecker = new HashSet<Subscription>(rc);

        TopicMessageStore store = (TopicMessageStore)dest.getMessageStore();
        // Eagerly recover the durable subscriptions
        if (store != null) {
            SubscriptionInfo[] infos = store.getAllSubscriptions();
            for (int i = 0; i < infos.length; i++) {

                SubscriptionInfo info = infos[i];
                LOG.debug("Restoring durable subscription: " + infos);
                SubscriptionKey key = new SubscriptionKey(info);

                // A single durable sub may be subscribing to multiple topics.
                // so it might exist already.
                DurableTopicSubscription sub = durableSubscriptions.get(key);
                ConsumerInfo consumerInfo = createInactiveConsumerInfo(info);
                if (sub == null) {
                    ConnectionContext c = new ConnectionContext();
                    c.setBroker(context.getBroker());
                    c.setClientId(key.getClientId());
                    c.setConnectionId(consumerInfo.getConsumerId().getParentId().getParentId());
                    sub = (DurableTopicSubscription)createSubscription(c, consumerInfo);
                }

                if (dupChecker.contains(sub)) {
                    continue;
                }

                dupChecker.add(sub);
                rc.add(sub);
                dest.addSubscription(context, sub);
            }

            // Now perhaps there other durable subscriptions (via wild card)
            // that would match this destination..
            durableSubscriptions.values();
            for (Iterator<DurableTopicSubscription> iterator = durableSubscriptions.values().iterator(); iterator.hasNext();) {
                DurableTopicSubscription sub = iterator.next();
                // Skip over subscriptions that we allready added..
                if (dupChecker.contains(sub)) {
                    continue;
                }

                if (sub.matches(dest.getActiveMQDestination())) {
                    rc.add(sub);
                    dest.addSubscription(context, sub);
                }
            }
        }

        return rc;
    }

    private ConsumerInfo createInactiveConsumerInfo(SubscriptionInfo info) {
        ConsumerInfo rc = new ConsumerInfo();
        rc.setSelector(info.getSelector());
        rc.setSubscriptionName(info.getSubscriptionName());
        rc.setDestination(info.getSubscribedDestination());
        rc.setConsumerId(createConsumerId());
        return rc;
    }

    private ConsumerId createConsumerId() {
        return new ConsumerId(recoveredDurableSubSessionId, recoveredDurableSubIdGenerator.getNextSequenceId());
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
        ActiveMQDestination destination = info.getDestination();
        Destination dest=null;
        try {
            dest = lookup(context, destination);
        } catch (Exception e) {
            JMSException jmsEx = new JMSException("Failed to retrieve destination from region "+ e);
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
        if (info.isDurable()) {
            if (AdvisorySupport.isAdvisoryTopic(info.getDestination())) {
                throw new JMSException("Cannot create a durable subscription for an advisory Topic");
            }
            SubscriptionKey key = new SubscriptionKey(context.getClientId(), info.getSubscriptionName());
            DurableTopicSubscription sub = durableSubscriptions.get(key);
            
            if (sub == null) {
                
                sub = new DurableTopicSubscription(broker,dest, usageManager, context, info, keepDurableSubsActive);
                if (destination != null && broker.getDestinationPolicy() != null) {
                    PolicyEntry entry = broker.getDestinationPolicy().getEntryFor(destination);
                    if (entry != null) {
                        entry.configure(broker, usageManager, sub);
                    }
                }
                durableSubscriptions.put(key, sub);
            } else {
                throw new JMSException("That durable subscription is already active.");
            }
            return sub;
        }
        try {
            TopicSubscription answer = new TopicSubscription(broker, dest,context, info, usageManager);
            // lets configure the subscription depending on the destination
            if (destination != null && broker.getDestinationPolicy() != null) {
                PolicyEntry entry = broker.getDestinationPolicy().getEntryFor(destination);
                if (entry != null) {
                    entry.configure(broker, usageManager, answer);
                }
            }
            answer.init();
            return answer;
        } catch (Exception e) {
            LOG.error("Failed to create TopicSubscription ", e);
            JMSException jmsEx = new JMSException("Couldn't create TopicSubscription");
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }

    /**
     */
    private boolean hasDurableSubChanged(ConsumerInfo info1, ConsumerInfo info2) {
        if (info1.getSelector() != null ^ info2.getSelector() != null) {
            return true;
        }
        if (info1.getSelector() != null && !info1.getSelector().equals(info2.getSelector())) {
            return true;
        }
        return !info1.getDestination().equals(info2.getDestination());
    }

    protected Set<ActiveMQDestination> getInactiveDestinations() {
        Set<ActiveMQDestination> inactiveDestinations = super.getInactiveDestinations();
        for (Iterator<ActiveMQDestination> iter = inactiveDestinations.iterator(); iter.hasNext();) {
            ActiveMQDestination dest = iter.next();
            if (!dest.isTopic()) {
                iter.remove();
            }
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
