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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Future;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.policy.DispatchPolicy;
import org.apache.activemq.broker.region.policy.NoSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.SimpleDispatchPolicy;
import org.apache.activemq.broker.region.policy.SubscriptionRecoveryPolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.thread.Valve;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.SubscriptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Topic is a destination that sends a copy of a message to every active
 * Subscription registered.
 * 
 * 
 */
public class Topic extends BaseDestination implements Task {
    protected static final Logger LOG = LoggerFactory.getLogger(Topic.class);
    private final TopicMessageStore topicStore;
    protected final CopyOnWriteArrayList<Subscription> consumers = new CopyOnWriteArrayList<Subscription>();
    protected final Valve dispatchValve = new Valve(true);
    private DispatchPolicy dispatchPolicy = new SimpleDispatchPolicy();
    private SubscriptionRecoveryPolicy subscriptionRecoveryPolicy;
    private final ConcurrentHashMap<SubscriptionKey, DurableTopicSubscription> durableSubcribers = new ConcurrentHashMap<SubscriptionKey, DurableTopicSubscription>();
    private final TaskRunner taskRunner;
    private final LinkedList<Runnable> messagesWaitingForSpace = new LinkedList<Runnable>();
    private final Runnable sendMessagesWaitingForSpaceTask = new Runnable() {
        public void run() {
            try {
                Topic.this.taskRunner.wakeup();
            } catch (InterruptedException e) {
            }
        };
    };

    public Topic(BrokerService brokerService, ActiveMQDestination destination, TopicMessageStore store,
            DestinationStatistics parentStats, TaskRunnerFactory taskFactory) throws Exception {
        super(brokerService, store, destination, parentStats);
        this.topicStore = store;
        // set default subscription recovery policy
        subscriptionRecoveryPolicy = new NoSubscriptionRecoveryPolicy();
        this.taskRunner = taskFactory.createTaskRunner(this, "Topic  " + destination.getPhysicalName());
    }

    @Override
    public void initialize() throws Exception {
        super.initialize();
        if (store != null) {
            // AMQ-2586: Better to leave this stat at zero than to give the user
            // misleading metrics.
            // int messageCount = store.getMessageCount();
            // destinationStatistics.getMessages().setCount(messageCount);
        }
    }

    public List<Subscription> getConsumers() {
        synchronized (consumers) {
            return new ArrayList<Subscription>(consumers);
        }
    }

    public boolean lock(MessageReference node, LockOwner sub) {
        return true;
    }

    public void addSubscription(ConnectionContext context, final Subscription sub) throws Exception {

       super.addSubscription(context, sub);

        if (!sub.getConsumerInfo().isDurable()) {

            // Do a retroactive recovery if needed.
            if (sub.getConsumerInfo().isRetroactive()) {

                // synchronize with dispatch method so that no new messages are
                // sent
                // while we are recovering a subscription to avoid out of order
                // messages.
                dispatchValve.turnOff();
                try {

                    synchronized (consumers) {
                        sub.add(context, this);
                        consumers.add(sub);
                    }
                    subscriptionRecoveryPolicy.recover(context, this, sub);

                } finally {
                    dispatchValve.turnOn();
                }

            } else {
                synchronized (consumers) {
                    sub.add(context, this);
                    consumers.add(sub);
                }
            }
        } else {
            sub.add(context, this);
            DurableTopicSubscription dsub = (DurableTopicSubscription) sub;
            durableSubcribers.put(dsub.getSubscriptionKey(), dsub);
        }
    }

    public void removeSubscription(ConnectionContext context, Subscription sub, long lastDeliveredSequenceId)
            throws Exception {
        if (!sub.getConsumerInfo().isDurable()) {
            super.removeSubscription(context, sub, lastDeliveredSequenceId);
            synchronized (consumers) {
                consumers.remove(sub);
            }
        }
        sub.remove(context, this);
    }

    public void deleteSubscription(ConnectionContext context, SubscriptionKey key) throws Exception {
        if (topicStore != null) {
            topicStore.deleteSubscription(key.clientId, key.subscriptionName);
            DurableTopicSubscription removed = durableSubcribers.remove(key);
            if (removed != null) {
                destinationStatistics.getConsumers().decrement();
                // deactivate and remove
                removed.deactivate(false);
                consumers.remove(removed);
            }
        }
    }

    public void activate(ConnectionContext context, final DurableTopicSubscription subscription) throws Exception {
        // synchronize with dispatch method so that no new messages are sent
        // while
        // we are recovering a subscription to avoid out of order messages.
        dispatchValve.turnOff();
        try {

            if (topicStore == null) {
                return;
            }

            // Recover the durable subscription.
            String clientId = subscription.getSubscriptionKey().getClientId();
            String subscriptionName = subscription.getSubscriptionKey().getSubscriptionName();
            String selector = subscription.getConsumerInfo().getSelector();
            SubscriptionInfo info = topicStore.lookupSubscription(clientId, subscriptionName);
            if (info != null) {
                // Check to see if selector changed.
                String s1 = info.getSelector();
                if (s1 == null ^ selector == null || (s1 != null && !s1.equals(selector))) {
                    // Need to delete the subscription
                    topicStore.deleteSubscription(clientId, subscriptionName);
                    info = null;
                } else {
                    synchronized (consumers) {
                        consumers.add(subscription);
                    }
                }
            }
            // Do we need to create the subscription?
            if (info == null) {
                info = new SubscriptionInfo();
                info.setClientId(clientId);
                info.setSelector(selector);
                info.setSubscriptionName(subscriptionName);
                info.setDestination(getActiveMQDestination());
                // This destination is an actual destination id.
                info.setSubscribedDestination(subscription.getConsumerInfo().getDestination());
                // This destination might be a pattern
                synchronized (consumers) {
                    consumers.add(subscription);
                    topicStore.addSubsciption(info, subscription.getConsumerInfo().isRetroactive());
                }
            }

            final MessageEvaluationContext msgContext = new NonCachedMessageEvaluationContext();
            msgContext.setDestination(destination);
            if (subscription.isRecoveryRequired()) {
                topicStore.recoverSubscription(clientId, subscriptionName, new MessageRecoveryListener() {
                    public boolean recoverMessage(Message message) throws Exception {
                        message.setRegionDestination(Topic.this);
                        try {
                            msgContext.setMessageReference(message);
                            if (subscription.matches(message, msgContext)) {
                                subscription.add(message);
                            }
                        } catch (IOException e) {
                            LOG.error("Failed to recover this message " + message);
                        }
                        return true;
                    }

                    public boolean recoverMessageReference(MessageId messageReference) throws Exception {
                        throw new RuntimeException("Should not be called.");
                    }

                    public boolean hasSpace() {
                        return true;
                    }

                    public boolean isDuplicate(MessageId id) {
                        return false;
                    }
                });
            }
        } finally {
            dispatchValve.turnOn();
        }
    }

    public void deactivate(ConnectionContext context, DurableTopicSubscription sub) throws Exception {
        synchronized (consumers) {
            consumers.remove(sub);
        }
        sub.remove(context, this);
    }

    protected void recoverRetroactiveMessages(ConnectionContext context, Subscription subscription) throws Exception {
        if (subscription.getConsumerInfo().isRetroactive()) {
            subscriptionRecoveryPolicy.recover(context, this, subscription);
        }
    }

    public void send(final ProducerBrokerExchange producerExchange, final Message message) throws Exception {
        final ConnectionContext context = producerExchange.getConnectionContext();

        final ProducerInfo producerInfo = producerExchange.getProducerState().getInfo();
        final boolean sendProducerAck = !message.isResponseRequired() && producerInfo.getWindowSize() > 0
                && !context.isInRecoveryMode();

        // There is delay between the client sending it and it arriving at the
        // destination.. it may have expired.
        if (message.isExpired()) {
            broker.messageExpired(context, message, null);
            getDestinationStatistics().getExpired().increment();
            if (sendProducerAck) {
                ProducerAck ack = new ProducerAck(producerInfo.getProducerId(), message.getSize());
                context.getConnection().dispatchAsync(ack);
            }
            return;
        }

        if (memoryUsage.isFull()) {
            isFull(context, memoryUsage);
            fastProducer(context, producerInfo);

            if (isProducerFlowControl() && context.isProducerFlowControl()) {

                if (warnOnProducerFlowControl) {
                    warnOnProducerFlowControl = false;
                    LOG
                            .info("Usage Manager memory limit ("
                                    + memoryUsage.getLimit()
                                    + ") reached for "
                                    + getActiveMQDestination().getQualifiedName()
                                    + ". Producers will be throttled to the rate at which messages are removed from this destination to prevent flooding it."
                                    + " See http://activemq.apache.org/producer-flow-control.html for more info");
                }

                if (systemUsage.isSendFailIfNoSpace()) {
                    throw new javax.jms.ResourceAllocationException("Usage Manager memory limit ("
                            + memoryUsage.getLimit() + ") reached. Stopping producer (" + message.getProducerId()
                            + ") to prevent flooding " + getActiveMQDestination().getQualifiedName() + "."
                            + " See http://activemq.apache.org/producer-flow-control.html for more info");
                }

                // We can avoid blocking due to low usage if the producer is
                // sending
                // a sync message or
                // if it is using a producer window
                if (producerInfo.getWindowSize() > 0 || message.isResponseRequired()) {
                    synchronized (messagesWaitingForSpace) {
                        messagesWaitingForSpace.add(new Runnable() {
                            public void run() {
                                try {

                                    // While waiting for space to free up... the
                                    // message may have expired.
                                    if (message.isExpired()) {
                                        broker.messageExpired(context, message, null);
                                        getDestinationStatistics().getExpired().increment();
                                    } else {
                                        doMessageSend(producerExchange, message);
                                    }

                                    if (sendProducerAck) {
                                        ProducerAck ack = new ProducerAck(producerInfo.getProducerId(), message
                                                .getSize());
                                        context.getConnection().dispatchAsync(ack);
                                    } else {
                                        Response response = new Response();
                                        response.setCorrelationId(message.getCommandId());
                                        context.getConnection().dispatchAsync(response);
                                    }

                                } catch (Exception e) {
                                    if (!sendProducerAck && !context.isInRecoveryMode()) {
                                        ExceptionResponse response = new ExceptionResponse(e);
                                        response.setCorrelationId(message.getCommandId());
                                        context.getConnection().dispatchAsync(response);
                                    }
                                }

                            }
                        });

                        registerCallbackForNotFullNotification();
                        context.setDontSendReponse(true);
                        return;
                    }

                } else {
                    // Producer flow control cannot be used, so we have do the
                    // flow
                    // control at the broker
                    // by blocking this thread until there is space available.

                    if (memoryUsage.isFull()) {
                        if (context.isInTransaction()) {

                            int count = 0;
                            while (!memoryUsage.waitForSpace(1000)) {
                                if (context.getStopping().get()) {
                                    throw new IOException("Connection closed, send aborted.");
                                }
                                if (count > 2 && context.isInTransaction()) {
                                    count = 0;
                                    int size = context.getTransaction().size();
                                    LOG.warn("Waiting for space to send  transacted message - transaction elements = "
                                            + size + " need more space to commit. Message = " + message);
                                }
                            }
                        } else {
                            waitForSpace(
                                    context,
                                    memoryUsage,
                                    "Usage Manager memory limit reached. Stopping producer ("
                                            + message.getProducerId()
                                            + ") to prevent flooding "
                                            + getActiveMQDestination().getQualifiedName()
                                            + "."
                                            + " See http://activemq.apache.org/producer-flow-control.html for more info");
                        }
                    }

                    // The usage manager could have delayed us by the time
                    // we unblock the message could have expired..
                    if (message.isExpired()) {
                        getDestinationStatistics().getExpired().increment();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Expired message: " + message);
                        }
                        return;
                    }
                }
            }
        }

        doMessageSend(producerExchange, message);
        messageDelivered(context, message);
        if (sendProducerAck) {
            ProducerAck ack = new ProducerAck(producerInfo.getProducerId(), message.getSize());
            context.getConnection().dispatchAsync(ack);
        }
    }

    /**
     * do send the message - this needs to be synchronized to ensure messages
     * are stored AND dispatched in the right order
     * 
     * @param producerExchange
     * @param message
     * @throws IOException
     * @throws Exception
     */
    synchronized void doMessageSend(final ProducerBrokerExchange producerExchange, final Message message)
            throws IOException, Exception {
        final ConnectionContext context = producerExchange.getConnectionContext();
        message.setRegionDestination(this);
        message.getMessageId().setBrokerSequenceId(getDestinationSequenceId());
        Future<Object> result = null;

        if (topicStore != null && message.isPersistent() && !canOptimizeOutPersistence()) {
            if (systemUsage.getStoreUsage().isFull(getStoreUsageHighWaterMark())) {
                final String logMessage = "Usage Manager Store is Full, " + getStoreUsageHighWaterMark() + "% of "
                        + systemUsage.getStoreUsage().getLimit() + ". Stopping producer (" + message.getProducerId()
                        + ") to prevent flooding " + getActiveMQDestination().getQualifiedName() + "."
                        + " See http://activemq.apache.org/producer-flow-control.html for more info";
                if (systemUsage.isSendFailIfNoSpace()) {
                    throw new javax.jms.ResourceAllocationException(logMessage);
                }

                waitForSpace(context, systemUsage.getStoreUsage(), getStoreUsageHighWaterMark(), logMessage);
            }
            result = topicStore.asyncAddTopicMessage(context, message);
        }

        message.incrementReferenceCount();

        if (context.isInTransaction()) {
            context.getTransaction().addSynchronization(new Synchronization() {
                @Override
                public void afterCommit() throws Exception {
                    // It could take while before we receive the commit
                    // operation.. by that time the message could have
                    // expired..
                    if (broker.isExpired(message)) {
                        getDestinationStatistics().getExpired().increment();
                        broker.messageExpired(context, message, null);
                        message.decrementReferenceCount();
                        return;
                    }
                    try {
                        dispatch(context, message);
                    } finally {
                        message.decrementReferenceCount();
                    }
                }
            });

        } else {
            try {
                dispatch(context, message);
            } finally {
                message.decrementReferenceCount();
            }
        }
        if (result != null && !result.isCancelled()) {
            try {
                result.get();
            } catch (CancellationException e) {
                // ignore - the task has been cancelled if the message
                // has already been deleted
            }
        }

    }

    private boolean canOptimizeOutPersistence() {
        return durableSubcribers.size() == 0;
    }

    @Override
    public String toString() {
        return "Topic: destination=" + destination.getPhysicalName() + ", subscriptions=" + consumers.size();
    }

    public void acknowledge(ConnectionContext context, Subscription sub, final MessageAck ack,
            final MessageReference node) throws IOException {
        if (topicStore != null && node.isPersistent()) {
            DurableTopicSubscription dsub = (DurableTopicSubscription) sub;
            SubscriptionKey key = dsub.getSubscriptionKey();
            topicStore.acknowledge(context, key.getClientId(), key.getSubscriptionName(), node.getMessageId(), ack);
        }
        messageConsumed(context, node);
    }

    public void gc() {
    }

    public Message loadMessage(MessageId messageId) throws IOException {
        return topicStore != null ? topicStore.getMessage(messageId) : null;
    }

    public void start() throws Exception {
        this.subscriptionRecoveryPolicy.start();
        if (memoryUsage != null) {
            memoryUsage.start();
        }

    }

    public void stop() throws Exception {
        if (taskRunner != null) {
            taskRunner.shutdown();
        }
        this.subscriptionRecoveryPolicy.stop();
        if (memoryUsage != null) {
            memoryUsage.stop();
        }
        if (this.topicStore != null) {
            this.topicStore.stop();
        }
    }

    public Message[] browse() {
        final Set<Message> result = new CopyOnWriteArraySet<Message>();
        try {
            if (topicStore != null) {
                topicStore.recover(new MessageRecoveryListener() {
                    public boolean recoverMessage(Message message) throws Exception {
                        result.add(message);
                        return true;
                    }

                    public boolean recoverMessageReference(MessageId messageReference) throws Exception {
                        return true;
                    }

                    public boolean hasSpace() {
                        return true;
                    }

                    public boolean isDuplicate(MessageId id) {
                        return false;
                    }
                });
                Message[] msgs = subscriptionRecoveryPolicy.browse(getActiveMQDestination());
                if (msgs != null) {
                    for (int i = 0; i < msgs.length; i++) {
                        result.add(msgs[i]);
                    }
                }
            }
        } catch (Throwable e) {
            LOG.warn("Failed to browse Topic: " + getActiveMQDestination().getPhysicalName(), e);
        }
        return result.toArray(new Message[result.size()]);
    }

    public boolean iterate() {
        synchronized (messagesWaitingForSpace) {
            while (!memoryUsage.isFull() && !messagesWaitingForSpace.isEmpty()) {
                Runnable op = messagesWaitingForSpace.removeFirst();
                op.run();
            }

            if (!messagesWaitingForSpace.isEmpty()) {
                registerCallbackForNotFullNotification();
            }
        }
        return false;
    }

    private void registerCallbackForNotFullNotification() {
        // If the usage manager is not full, then the task will not
        // get called..
        if (!memoryUsage.notifyCallbackWhenNotFull(sendMessagesWaitingForSpaceTask)) {
            // so call it directly here.
            sendMessagesWaitingForSpaceTask.run();
        }
    }

    // Properties
    // -------------------------------------------------------------------------

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

    // Implementation methods
    // -------------------------------------------------------------------------

    public final void wakeup() {
    }

    protected void dispatch(final ConnectionContext context, Message message) throws Exception {
        // AMQ-2586: Better to leave this stat at zero than to give the user
        // misleading metrics.
        // destinationStatistics.getMessages().increment();
        destinationStatistics.getEnqueues().increment();
        dispatchValve.increment();
        MessageEvaluationContext msgContext = null;
        try {
            if (!subscriptionRecoveryPolicy.add(context, message)) {
                return;
            }
            synchronized (consumers) {
                if (consumers.isEmpty()) {
                    onMessageWithNoConsumers(context, message);
                    return;
                }
            }
            msgContext = context.getMessageEvaluationContext();
            msgContext.setDestination(destination);
            msgContext.setMessageReference(message);
            if (!dispatchPolicy.dispatch(message, msgContext, consumers)) {
                onMessageWithNoConsumers(context, message);
            }

        } finally {
            dispatchValve.decrement();
            if (msgContext != null) {
                msgContext.clear();
            }
        }
    }

    public void messageExpired(ConnectionContext context, Subscription subs, MessageReference reference) {
        broker.messageExpired(context, reference, subs);
        // AMQ-2586: Better to leave this stat at zero than to give the user
        // misleading metrics.
        // destinationStatistics.getMessages().decrement();
        destinationStatistics.getEnqueues().decrement();
        destinationStatistics.getExpired().increment();
        MessageAck ack = new MessageAck();
        ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
        ack.setDestination(destination);
        ack.setMessageID(reference.getMessageId());
        try {
            acknowledge(context, subs, ack, reference);
        } catch (IOException e) {
            LOG.error("Failed to remove expired Message from the store ", e);
        }
    }

    @Override
    protected Logger getLog() {
        return LOG;
    }

}
