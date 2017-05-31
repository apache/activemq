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
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.policy.DispatchPolicy;
import org.apache.activemq.broker.region.policy.LastImageSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.RetainedMessageSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.SimpleDispatchPolicy;
import org.apache.activemq.broker.region.policy.SubscriptionRecoveryPolicy;
import org.apache.activemq.broker.util.InsertionCountList;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
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
import org.apache.activemq.store.NoLocalSubscriptionAware;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.SubscriptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Topic is a destination that sends a copy of a message to every active
 * Subscription registered.
 */
public class Topic extends BaseDestination implements Task {
    protected static final Logger LOG = LoggerFactory.getLogger(Topic.class);
    private final TopicMessageStore topicStore;
    protected final CopyOnWriteArrayList<Subscription> consumers = new CopyOnWriteArrayList<Subscription>();
    private final ReentrantReadWriteLock dispatchLock = new ReentrantReadWriteLock();
    private DispatchPolicy dispatchPolicy = new SimpleDispatchPolicy();
    private SubscriptionRecoveryPolicy subscriptionRecoveryPolicy;
    private final ConcurrentMap<SubscriptionKey, DurableTopicSubscription> durableSubscribers = new ConcurrentHashMap<SubscriptionKey, DurableTopicSubscription>();
    private final TaskRunner taskRunner;
    private final LinkedList<Runnable> messagesWaitingForSpace = new LinkedList<Runnable>();
    private final Runnable sendMessagesWaitingForSpaceTask = new Runnable() {
        @Override
        public void run() {
            try {
                Topic.this.taskRunner.wakeup();
            } catch (InterruptedException e) {
            }
        }
    };

    public Topic(BrokerService brokerService, ActiveMQDestination destination, TopicMessageStore store,
            DestinationStatistics parentStats, TaskRunnerFactory taskFactory) throws Exception {
        super(brokerService, store, destination, parentStats);
        this.topicStore = store;
        subscriptionRecoveryPolicy = new RetainedMessageSubscriptionRecoveryPolicy(null);
        this.taskRunner = taskFactory.createTaskRunner(this, "Topic  " + destination.getPhysicalName());
    }

    @Override
    public void initialize() throws Exception {
        super.initialize();
        // set non default subscription recovery policy (override policyEntries)
        if (AdvisorySupport.isMasterBrokerAdvisoryTopic(destination)) {
            subscriptionRecoveryPolicy = new LastImageSubscriptionRecoveryPolicy();
            setAlwaysRetroactive(true);
        }
        if (store != null) {
            // AMQ-2586: Better to leave this stat at zero than to give the user
            // misleading metrics.
            // int messageCount = store.getMessageCount();
            // destinationStatistics.getMessages().setCount(messageCount);
            store.start();
        }
    }

    @Override
    public List<Subscription> getConsumers() {
        synchronized (consumers) {
            return new ArrayList<Subscription>(consumers);
        }
    }

    public boolean lock(MessageReference node, LockOwner sub) {
        return true;
    }

    @Override
    public void addSubscription(ConnectionContext context, final Subscription sub) throws Exception {
        if (!sub.getConsumerInfo().isDurable()) {

            // Do a retroactive recovery if needed.
            if (sub.getConsumerInfo().isRetroactive() || isAlwaysRetroactive()) {

                // synchronize with dispatch method so that no new messages are sent
                // while we are recovering a subscription to avoid out of order messages.
                dispatchLock.writeLock().lock();
                try {
                    boolean applyRecovery = false;
                    synchronized (consumers) {
                        if (!consumers.contains(sub)){
                            sub.add(context, this);
                            consumers.add(sub);
                            applyRecovery=true;
                            super.addSubscription(context, sub);
                        }
                    }
                    if (applyRecovery){
                        subscriptionRecoveryPolicy.recover(context, this, sub);
                    }
                } finally {
                    dispatchLock.writeLock().unlock();
                }

            } else {
                synchronized (consumers) {
                    if (!consumers.contains(sub)){
                        sub.add(context, this);
                        consumers.add(sub);
                        super.addSubscription(context, sub);
                    }
                }
            }
        } else {
            DurableTopicSubscription dsub = (DurableTopicSubscription) sub;
            super.addSubscription(context, sub);
            sub.add(context, this);
            if(dsub.isActive()) {
                synchronized (consumers) {
                    boolean hasSubscription = false;

                    if (consumers.size() == 0) {
                        hasSubscription = false;
                    } else {
                        for (Subscription currentSub : consumers) {
                            if (currentSub.getConsumerInfo().isDurable()) {
                                DurableTopicSubscription dcurrentSub = (DurableTopicSubscription) currentSub;
                                if (dcurrentSub.getSubscriptionKey().equals(dsub.getSubscriptionKey())) {
                                    hasSubscription = true;
                                    break;
                                }
                            }
                        }
                    }

                    if (!hasSubscription) {
                        consumers.add(sub);
                    }
                }
            }
            durableSubscribers.put(dsub.getSubscriptionKey(), dsub);
        }
    }

    @Override
    public void removeSubscription(ConnectionContext context, Subscription sub, long lastDeliveredSequenceId) throws Exception {
        if (!sub.getConsumerInfo().isDurable()) {
            boolean removed = false;
            synchronized (consumers) {
                removed = consumers.remove(sub);
            }
            if (removed) {
                super.removeSubscription(context, sub, lastDeliveredSequenceId);
            }
        }
        sub.remove(context, this);
    }

    public void deleteSubscription(ConnectionContext context, SubscriptionKey key) throws Exception {
        if (topicStore != null) {
            topicStore.deleteSubscription(key.clientId, key.subscriptionName);
            DurableTopicSubscription removed = durableSubscribers.remove(key);
            if (removed != null) {
                destinationStatistics.getConsumers().decrement();
                // deactivate and remove
                removed.deactivate(false, 0l);
                consumers.remove(removed);
            }
        }
    }

    private boolean hasDurableSubChanged(SubscriptionInfo info1, ConsumerInfo info2) throws IOException {
        if (hasSelectorChanged(info1, info2)) {
            return true;
        }

        return hasNoLocalChanged(info1, info2);
    }

    private boolean hasNoLocalChanged(SubscriptionInfo info1, ConsumerInfo info2) throws IOException {
        //Not all persistence adapters store the noLocal value for a subscription
        PersistenceAdapter adapter = broker.getBrokerService().getPersistenceAdapter();
        if (adapter instanceof NoLocalSubscriptionAware) {
            if (info1.isNoLocal() ^ info2.isNoLocal()) {
                return true;
            }
        }

        return false;
    }

    private boolean hasSelectorChanged(SubscriptionInfo info1, ConsumerInfo info2) {
        if (info1.getSelector() != null ^ info2.getSelector() != null) {
            return true;
        }

        if (info1.getSelector() != null && !info1.getSelector().equals(info2.getSelector())) {
            return true;
        }

        return false;
    }

    public void activate(ConnectionContext context, final DurableTopicSubscription subscription) throws Exception {
        // synchronize with dispatch method so that no new messages are sent
        // while we are recovering a subscription to avoid out of order messages.
        dispatchLock.writeLock().lock();
        try {

            if (topicStore == null) {
                return;
            }

            // Recover the durable subscription.
            String clientId = subscription.getSubscriptionKey().getClientId();
            String subscriptionName = subscription.getSubscriptionKey().getSubscriptionName();
            SubscriptionInfo info = topicStore.lookupSubscription(clientId, subscriptionName);
            if (info != null) {
                // Check to see if selector changed.
                if (hasDurableSubChanged(info, subscription.getConsumerInfo())) {
                    // Need to delete the subscription
                    topicStore.deleteSubscription(clientId, subscriptionName);
                    info = null;
                    // Force a rebuild of the selector chain for the subscription otherwise
                    // the stored subscription is updated but the selector expression is not
                    // and the subscription will not behave according to the new configuration.
                    subscription.setSelector(subscription.getConsumerInfo().getSelector());
                    synchronized (consumers) {
                        consumers.remove(subscription);
                    }
                } else {
                    synchronized (consumers) {
                        if (!consumers.contains(subscription)) {
                            consumers.add(subscription);
                        }
                    }
                }
            }

            // Do we need to create the subscription?
            if (info == null) {
                info = new SubscriptionInfo();
                info.setClientId(clientId);
                info.setSelector(subscription.getConsumerInfo().getSelector());
                info.setSubscriptionName(subscriptionName);
                info.setDestination(getActiveMQDestination());
                info.setNoLocal(subscription.getConsumerInfo().isNoLocal());
                // This destination is an actual destination id.
                info.setSubscribedDestination(subscription.getConsumerInfo().getDestination());
                // This destination might be a pattern
                synchronized (consumers) {
                    consumers.add(subscription);
                    topicStore.addSubscription(info, subscription.getConsumerInfo().isRetroactive());
                }
            }

            final MessageEvaluationContext msgContext = new NonCachedMessageEvaluationContext();
            msgContext.setDestination(destination);
            if (subscription.isRecoveryRequired()) {
                topicStore.recoverSubscription(clientId, subscriptionName, new MessageRecoveryListener() {
                    @Override
                    public boolean recoverMessage(Message message) throws Exception {
                        message.setRegionDestination(Topic.this);
                        try {
                            msgContext.setMessageReference(message);
                            if (subscription.matches(message, msgContext)) {
                                subscription.add(message);
                            }
                        } catch (IOException e) {
                            LOG.error("Failed to recover this message {}", message, e);
                        }
                        return true;
                    }

                    @Override
                    public boolean recoverMessageReference(MessageId messageReference) throws Exception {
                        throw new RuntimeException("Should not be called.");
                    }

                    @Override
                    public boolean hasSpace() {
                        return true;
                    }

                    @Override
                    public boolean isDuplicate(MessageId id) {
                        return false;
                    }
                });
            }
        } finally {
            dispatchLock.writeLock().unlock();
        }
    }

    public void deactivate(ConnectionContext context, DurableTopicSubscription sub, List<MessageReference> dispatched) throws Exception {
        synchronized (consumers) {
            consumers.remove(sub);
        }
        sub.remove(context, this, dispatched);
    }

    public void recoverRetroactiveMessages(ConnectionContext context, Subscription subscription) throws Exception {
        if (subscription.getConsumerInfo().isRetroactive()) {
            subscriptionRecoveryPolicy.recover(context, this, subscription);
        }
    }

    @Override
    public void send(final ProducerBrokerExchange producerExchange, final Message message) throws Exception {
        final ConnectionContext context = producerExchange.getConnectionContext();

        final ProducerInfo producerInfo = producerExchange.getProducerState().getInfo();
        producerExchange.incrementSend();
        final boolean sendProducerAck = !message.isResponseRequired() && producerInfo.getWindowSize() > 0
                && !context.isInRecoveryMode();

        message.setRegionDestination(this);

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

                if (isFlowControlLogRequired()) {
                    LOG.info("{}, Usage Manager memory limit reached {}. Producers will be throttled to the rate at which messages are removed from this destination to prevent flooding it. See http://activemq.apache.org/producer-flow-control.html for more info.",
                            getActiveMQDestination().getQualifiedName(), memoryUsage.getLimit());
                }

                if (!context.isNetworkConnection() && systemUsage.isSendFailIfNoSpace()) {
                    throw new javax.jms.ResourceAllocationException("Usage Manager memory limit ("
                            + memoryUsage.getLimit() + ") reached. Rejecting send for producer (" + message.getProducerId()
                            + ") to prevent flooding " + getActiveMQDestination().getQualifiedName() + "."
                            + " See http://activemq.apache.org/producer-flow-control.html for more info");
                }

                // We can avoid blocking due to low usage if the producer is sending a sync message or
                // if it is using a producer window
                if (producerInfo.getWindowSize() > 0 || message.isResponseRequired()) {
                    synchronized (messagesWaitingForSpace) {
                        messagesWaitingForSpace.add(new Runnable() {
                            @Override
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
                    // Producer flow control cannot be used, so we have do the flow control
                    // at the broker by blocking this thread until there is space available.

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
                                    LOG.warn("Waiting for space to send transacted message - transaction elements = {} need more space to commit. Message = {}", size, message);
                                }
                                count++;
                            }
                        } else {
                            waitForSpace(
                                    context,
                                    producerExchange,
                                    memoryUsage,
                                    "Usage Manager Memory Usage limit reached. Stopping producer ("
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
                        LOG.debug("Expired message: {}", message);
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
        message.getMessageId().setBrokerSequenceId(getDestinationSequenceId());
        Future<Object> result = null;

        if (topicStore != null && message.isPersistent() && !canOptimizeOutPersistence()) {
            if (systemUsage.getStoreUsage().isFull(getStoreUsageHighWaterMark())) {
                final String logMessage = "Persistent store is Full, " + getStoreUsageHighWaterMark() + "% of "
                        + systemUsage.getStoreUsage().getLimit() + ". Stopping producer (" + message.getProducerId()
                        + ") to prevent flooding " + getActiveMQDestination().getQualifiedName() + "."
                        + " See http://activemq.apache.org/producer-flow-control.html for more info";
                if (!context.isNetworkConnection() && systemUsage.isSendFailIfNoSpace()) {
                    throw new javax.jms.ResourceAllocationException(logMessage);
                }

                waitForSpace(context,producerExchange, systemUsage.getStoreUsage(), getStoreUsageHighWaterMark(), logMessage);
            }
            result = topicStore.asyncAddTopicMessage(context, message,isOptimizeStorage());

            //Moved the reduceMemoryfootprint clearing to the dispatch method
        }

        message.incrementReferenceCount();

        if (context.isInTransaction()) {
            context.getTransaction().addSynchronization(new Synchronization() {
                @Override
                public void afterCommit() throws Exception {
                    // It could take while before we receive the commit
                    // operation.. by that time the message could have
                    // expired..
                    if (message.isExpired()) {
                        if (broker.isExpired(message)) {
                            getDestinationStatistics().getExpired().increment();
                            broker.messageExpired(context, message, null);
                        }
                        message.decrementReferenceCount();
                        return;
                    }
                    try {
                        dispatch(context, message);
                    } finally {
                        message.decrementReferenceCount();
                    }
                }

                @Override
                public void afterRollback() throws Exception {
                    message.decrementReferenceCount();
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
        return durableSubscribers.size() == 0;
    }

    @Override
    public String toString() {
        return "Topic: destination=" + destination.getPhysicalName() + ", subscriptions=" + consumers.size();
    }

    @Override
    public void acknowledge(ConnectionContext context, Subscription sub, final MessageAck ack,
            final MessageReference node) throws IOException {
        if (topicStore != null && node.isPersistent()) {
            DurableTopicSubscription dsub = (DurableTopicSubscription) sub;
            SubscriptionKey key = dsub.getSubscriptionKey();
            topicStore.acknowledge(context, key.getClientId(), key.getSubscriptionName(), node.getMessageId(),
                    convertToNonRangedAck(ack, node));
        }
        messageConsumed(context, node);
    }

    @Override
    public void gc() {
    }

    public Message loadMessage(MessageId messageId) throws IOException {
        return topicStore != null ? topicStore.getMessage(messageId) : null;
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            this.subscriptionRecoveryPolicy.start();
            if (memoryUsage != null) {
                memoryUsage.start();
            }

            if (getExpireMessagesPeriod() > 0 && !AdvisorySupport.isAdvisoryTopic(getActiveMQDestination())) {
                scheduler.executePeriodically(expireMessagesTask, getExpireMessagesPeriod());
            }
        }
    }

    @Override
    public void stop() throws Exception {
        if (started.compareAndSet(true, false)) {
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

            scheduler.cancel(expireMessagesTask);
        }
    }

    @Override
    public Message[] browse() {
        final List<Message> result = new ArrayList<Message>();
        doBrowse(result, getMaxBrowsePageSize());
        return result.toArray(new Message[result.size()]);
    }

    private void doBrowse(final List<Message> browseList, final int max) {
        try {
            if (topicStore != null) {
                final List<Message> toExpire = new ArrayList<Message>();
                topicStore.recover(new MessageRecoveryListener() {
                    @Override
                    public boolean recoverMessage(Message message) throws Exception {
                        if (message.isExpired()) {
                            toExpire.add(message);
                        }
                        browseList.add(message);
                        return true;
                    }

                    @Override
                    public boolean recoverMessageReference(MessageId messageReference) throws Exception {
                        return true;
                    }

                    @Override
                    public boolean hasSpace() {
                        return browseList.size() < max;
                    }

                    @Override
                    public boolean isDuplicate(MessageId id) {
                        return false;
                    }
                });
                final ConnectionContext connectionContext = createConnectionContext();
                for (Message message : toExpire) {
                    for (DurableTopicSubscription sub : durableSubscribers.values()) {
                        if (!sub.isActive()) {
                            message.setRegionDestination(this);
                            messageExpired(connectionContext, sub, message);
                        }
                    }
                }
                Message[] msgs = subscriptionRecoveryPolicy.browse(getActiveMQDestination());
                if (msgs != null) {
                    for (int i = 0; i < msgs.length && browseList.size() < max; i++) {
                        browseList.add(msgs[i]);
                    }
                }
            }
        } catch (Throwable e) {
            LOG.warn("Failed to browse Topic: {}", getActiveMQDestination().getPhysicalName(), e);
        }
    }

    @Override
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

    public void setSubscriptionRecoveryPolicy(SubscriptionRecoveryPolicy recoveryPolicy) {
        if (this.subscriptionRecoveryPolicy != null && this.subscriptionRecoveryPolicy instanceof RetainedMessageSubscriptionRecoveryPolicy) {
            // allow users to combine retained message policy with other ActiveMQ policies
            RetainedMessageSubscriptionRecoveryPolicy policy = (RetainedMessageSubscriptionRecoveryPolicy) this.subscriptionRecoveryPolicy;
            policy.setWrapped(recoveryPolicy);
        } else {
            this.subscriptionRecoveryPolicy = recoveryPolicy;
        }
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    @Override
    public final void wakeup() {
    }

    protected void dispatch(final ConnectionContext context, Message message) throws Exception {
        // AMQ-2586: Better to leave this stat at zero than to give the user
        // misleading metrics.
        // destinationStatistics.getMessages().increment();
        destinationStatistics.getEnqueues().increment();
        destinationStatistics.getMessageSize().addSize(message.getSize());
        MessageEvaluationContext msgContext = null;

        dispatchLock.readLock().lock();
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

            // Clear memory before dispatch - need to clear here because the call to
            //subscriptionRecoveryPolicy.add() will unmarshall the state
            if (isReduceMemoryFootprint() && message.isMarshalled()) {
                message.clearUnMarshalledState();
            }

            msgContext = context.getMessageEvaluationContext();
            msgContext.setDestination(destination);
            msgContext.setMessageReference(message);
            if (!dispatchPolicy.dispatch(message, msgContext, consumers)) {
                onMessageWithNoConsumers(context, message);
            }

        } finally {
            dispatchLock.readLock().unlock();
            if (msgContext != null) {
                msgContext.clear();
            }
        }
    }

    private final Runnable expireMessagesTask = new Runnable() {
        @Override
        public void run() {
            List<Message> browsedMessages = new InsertionCountList<Message>();
            doBrowse(browsedMessages, getMaxExpirePageSize());
        }
    };

    @Override
    public void messageExpired(ConnectionContext context, Subscription subs, MessageReference reference) {
        broker.messageExpired(context, reference, subs);
        // AMQ-2586: Better to leave this stat at zero than to give the user
        // misleading metrics.
        // destinationStatistics.getMessages().decrement();
        destinationStatistics.getExpired().increment();
        MessageAck ack = new MessageAck();
        ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
        ack.setDestination(destination);
        ack.setMessageID(reference.getMessageId());
        try {
            if (subs instanceof DurableTopicSubscription) {
                ((DurableTopicSubscription)subs).removePending(reference);
            }
            acknowledge(context, subs, ack, reference);
        } catch (Exception e) {
            LOG.error("Failed to remove expired Message from the store ", e);
        }
    }

    @Override
    protected Logger getLog() {
        return LOG;
    }

    protected boolean isOptimizeStorage(){
        boolean result = false;

        if (isDoOptimzeMessageStorage() && durableSubscribers.isEmpty()==false){
                result = true;
                for (DurableTopicSubscription s : durableSubscribers.values()) {
                    if (s.isActive()== false){
                        result = false;
                        break;
                    }
                    if (s.getPrefetchSize()==0){
                        result = false;
                        break;
                    }
                    if (s.isSlowConsumer()){
                        result = false;
                        break;
                    }
                    if (s.getInFlightUsage() > getOptimizeMessageStoreInFlightLimit()){
                        result = false;
                        break;
                    }
                }
        }
        return result;
    }

    /**
     * force a reread of the store - after transaction recovery completion
     */
    @Override
    public void clearPendingMessages() {
        dispatchLock.readLock().lock();
        try {
            for (DurableTopicSubscription durableTopicSubscription : durableSubscribers.values()) {
                clearPendingAndDispatch(durableTopicSubscription);
            }
        } finally {
            dispatchLock.readLock().unlock();
        }
    }

    private void clearPendingAndDispatch(DurableTopicSubscription durableTopicSubscription) {
        synchronized (durableTopicSubscription.pendingLock) {
            durableTopicSubscription.pending.clear();
            try {
                durableTopicSubscription.dispatchPending();
            } catch (IOException exception) {
                LOG.warn("After clear of pending, failed to dispatch to: {}, for: {}, pending: {}", new Object[]{
                        durableTopicSubscription,
                        destination,
                        durableTopicSubscription.pending }, exception);
            }
        }
    }

    public Map<SubscriptionKey, DurableTopicSubscription> getDurableTopicSubs() {
        return durableSubscribers;
    }
}
