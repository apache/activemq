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

import java.io.IOException;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.DispatchPolicy;
import org.apache.activemq.broker.region.policy.LastImageSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.SimpleDispatchPolicy;
import org.apache.activemq.broker.region.policy.SubscriptionRecoveryPolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.thread.Valve;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.SubscriptionKey;

import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;

/**
 * The Topic is a destination that sends a copy of a message to every active
 * Subscription registered.
 * 
 * @version $Revision: 1.21 $
 */
public class Topic implements Destination {

    protected final ActiveMQDestination destination;
    protected final CopyOnWriteArrayList consumers = new CopyOnWriteArrayList();
    protected final Valve dispatchValve = new Valve(true);
    protected final TopicMessageStore store;
    protected final UsageManager usageManager;
    protected final DestinationStatistics destinationStatistics = new DestinationStatistics();

    private DispatchPolicy dispatchPolicy = new SimpleDispatchPolicy();
    private SubscriptionRecoveryPolicy subscriptionRecoveryPolicy = new LastImageSubscriptionRecoveryPolicy();
    private boolean sendAdvisoryIfNoConsumers = true;

    public Topic(ActiveMQDestination destination, TopicMessageStore store, UsageManager memoryManager, DestinationStatistics parentStats,
            TaskRunnerFactory taskFactory) {

        this.destination = destination;
        this.store = store;
        this.usageManager = memoryManager;

        // TODO: switch back when cache is working again.
        // this.cache = cache;
        // destinationStatistics.setMessagesCached(cache.getMessagesCached());
        // CacheEvictionUsageListener listener = new
        // CacheEvictionUsageListener(memoryManager, 90, 50, taskFactory);
        // listener.add(cache);
        // this.memoryManager.addUsageListener(listener);

        this.destinationStatistics.setParent(parentStats);
    }

    public boolean lock(MessageReference node, Subscription sub) {
        return true;
    }

    public void addSubscription(ConnectionContext context, final Subscription sub) throws Throwable {
        destinationStatistics.getConsumers().increment();
        sub.add(context, this);
        if (sub.getConsumerInfo().isDurable()) {
            recover((DurableTopicSubscription) sub, true);
        }
        else {
            if (sub.getConsumerInfo().isRetroactive()) {
                subscriptionRecoveryPolicy.recover(context, this, sub);
            }
            consumers.add(sub);
        }
    }

    public void recover(final DurableTopicSubscription sub, boolean initialActivation) throws Throwable {

        // synchronize with dispatch method so that no new messages are sent
        // while
        // we are recovering a subscription to avoid out of order messages.
        dispatchValve.turnOff();
        try {

            if (initialActivation)
                consumers.add(sub);

            if (store != null) {
                String clientId = sub.getClientId();
                String subscriptionName = sub.getSubscriptionName();
                String selector = sub.getConsumerInfo().getSelector();
                SubscriptionInfo info = store.lookupSubscription(clientId, subscriptionName);
                if (info != null) {
                    // Check to see if selector changed.
                    String s1 = info.getSelector();
                    if (s1 == null ^ selector == null || (s1 != null && !s1.equals(selector))) {
                        // Need to delete the subscription
                        store.deleteSubscription(clientId, subscriptionName);
                        info = null;
                    }
                }
                // Do we need to crate the subscription?
                if (info == null) {
                    store.addSubsciption(clientId, subscriptionName, selector, sub.getConsumerInfo().isRetroactive());
                }

                if (sub.isRecovered()) {
                    final MessageEvaluationContext msgContext = new MessageEvaluationContext();
                    msgContext.setDestination(destination);
                    store.recoverSubscription(clientId, subscriptionName, new MessageRecoveryListener() {
                        public void recoverMessage(Message message) throws Throwable {
                            message.setRegionDestination(Topic.this);
                            try {
                                msgContext.setMessageReference(message);
                                if (sub.matches(message, msgContext)) {
                                    sub.add(message);
                                }
                            }
                            catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            catch (IOException e) {
                                // TODO: Need to handle this better.
                                e.printStackTrace();
                            }
                        }

                        public void recoverMessageReference(String messageReference) throws Throwable {
                            throw new RuntimeException("Should not be called.");
                        }
                    });
                }
            }

        }
        finally {
            dispatchValve.turnOn();
        }
    }

    public void removeSubscription(ConnectionContext context, Subscription sub) throws Throwable {
        destinationStatistics.getConsumers().decrement();
        consumers.remove(sub);
        sub.remove(context, this);
    }

    public void send(final ConnectionContext context, final Message message) throws Throwable {

        if (context.isProducerFlowControl())
            usageManager.waitForSpace();

        message.setRegionDestination(this);

        if (store != null && message.isPersistent())
            store.addMessage(context, message);

        message.incrementReferenceCount();
        try {

            if (context.isInTransaction()) {
                context.getTransaction().addSynchronization(new Synchronization() {
                    public void afterCommit() throws Throwable {
                        dispatch(context, message);
                    }
                });

            }
            else {
                dispatch(context, message);
            }

        }
        finally {
            message.decrementReferenceCount();
        }

    }

    public void deleteSubscription(ConnectionContext context, SubscriptionKey key) throws IOException {
        if (store != null) {
            store.deleteSubscription(key.clientId, key.subscriptionName);
        }
    }

    public String toString() {
        return "Topic: destination=" + destination.getPhysicalName() + ", subscriptions=" + consumers.size();
    }

    public void acknowledge(ConnectionContext context, Subscription sub, final MessageAck ack, final MessageReference node) throws IOException {
        if (store != null && node.isPersistent()) {
            DurableTopicSubscription dsub = (DurableTopicSubscription) sub;
            store.acknowledge(context, dsub.getClientId(), dsub.getSubscriptionName(), ack.getLastMessageId());
        }
    }

    public void dispose(ConnectionContext context) throws IOException {
        if (store != null) {
            store.removeAllMessages(context);
        }
        destinationStatistics.setParent(null);
    }

    public void gc() {
    }

    public Message loadMessage(MessageId messageId) throws IOException {
        return store.getMessage(messageId);
    }

    public void start() throws Exception {
        this.subscriptionRecoveryPolicy.start();
    }

    public void stop() throws Exception {
        this.subscriptionRecoveryPolicy.stop();
    }

    // Properties
    // -------------------------------------------------------------------------

    public UsageManager getUsageManager() {
        return usageManager;
    }

    public DestinationStatistics getDestinationStatistics() {
        return destinationStatistics;
    }

    public ActiveMQDestination getActiveMQDestination() {
        return destination;
    }

    public DispatchPolicy getDispatchPolicy() {
        return dispatchPolicy;
    }

    public void setDispatchPolicy(DispatchPolicy dispatchPolicy) {
        this.dispatchPolicy = dispatchPolicy;
    }

    public SubscriptionRecoveryPolicy getSubscriptionRecoveryPolicy() {
        return subscriptionRecoveryPolicy;
    }

    public void setSubscriptionRecoveryPolicy(SubscriptionRecoveryPolicy subscriptionRecoveryPolicy) {
        this.subscriptionRecoveryPolicy = subscriptionRecoveryPolicy;
    }

    public boolean isSendAdvisoryIfNoConsumers() {
        return sendAdvisoryIfNoConsumers;
    }

    public void setSendAdvisoryIfNoConsumers(boolean sendAdvisoryIfNoConsumers) {
        this.sendAdvisoryIfNoConsumers = sendAdvisoryIfNoConsumers;
    }

    public MessageStore getMessageStore() {
        return store;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void dispatch(ConnectionContext context, Message message) throws Throwable {
        destinationStatistics.getEnqueues().increment();
        dispatchValve.increment();
        MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
        try {
            if (!subscriptionRecoveryPolicy.add(context, message)) {
                return;
            }
            if (consumers.isEmpty()) {
                onMessageWithNoConsumers(context, message);
                return;
            }

            msgContext.setDestination(destination);
            msgContext.setMessageReference(message);

            if (!dispatchPolicy.dispatch(context, message, msgContext, consumers)) {
                onMessageWithNoConsumers(context, message);
            }
        }
        finally {
            msgContext.clear();
            dispatchValve.decrement();
        }
    }

    /**
     * Provides a hook to allow messages with no consumer to be processed in
     * some way - such as to send to a dead letter queue or something..
     */
    protected void onMessageWithNoConsumers(ConnectionContext context, Message message) throws Throwable {
        if (!message.isPersistent()) {
            if (sendAdvisoryIfNoConsumers) {
                // allow messages with no consumers to be dispatched to a dead
                // letter queue
                ActiveMQDestination originalDestination = message.getDestination();
                if (!AdvisorySupport.isAdvisoryTopic(originalDestination)) {
                    ActiveMQTopic advisoryTopic = AdvisorySupport.getExpiredTopicMessageAdvisoryTopic(originalDestination);
                    message.setDestination(advisoryTopic);
                    context.getBroker().send(context, message);
                }
            }
        }
    }

}
