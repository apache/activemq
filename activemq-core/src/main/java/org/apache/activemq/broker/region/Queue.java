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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.ResourceAllocationException;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.StoreQueueCursor;
import org.apache.activemq.broker.region.cursors.VMPendingMessageCursor;
import org.apache.activemq.broker.region.group.MessageGroupHashBucketFactory;
import org.apache.activemq.broker.region.group.MessageGroupMap;
import org.apache.activemq.broker.region.group.MessageGroupMapFactory;
import org.apache.activemq.broker.region.policy.DispatchPolicy;
import org.apache.activemq.broker.region.policy.RoundRobinDispatchPolicy;
import org.apache.activemq.command.*;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.usage.UsageListener;
import org.apache.activemq.util.BrokerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * The Queue is a List of MessageEntry objects that are dispatched to matching
 * subscriptions.
 *
 *
 */
public class Queue extends BaseDestination implements Task, UsageListener {
    protected static final Logger LOG = LoggerFactory.getLogger(Queue.class);
    protected final TaskRunnerFactory taskFactory;
    protected TaskRunner taskRunner;
    private final ReentrantReadWriteLock consumersLock = new ReentrantReadWriteLock();
    protected final List<Subscription> consumers = new ArrayList<Subscription>(50);
    private final ReentrantReadWriteLock messagesLock = new ReentrantReadWriteLock();
    protected PendingMessageCursor messages;
    private final ReentrantReadWriteLock pagedInMessagesLock = new ReentrantReadWriteLock();
    private final LinkedHashMap<MessageId, QueueMessageReference> pagedInMessages = new LinkedHashMap<MessageId, QueueMessageReference>();
    // Messages that are paged in but have not yet been targeted at a
    // subscription
    private final ReentrantReadWriteLock pagedInPendingDispatchLock = new ReentrantReadWriteLock();
    private List<QueueMessageReference> pagedInPendingDispatch = new ArrayList<QueueMessageReference>(100);
    private List<QueueMessageReference> redeliveredWaitingDispatch = new ArrayList<QueueMessageReference>();
    private MessageGroupMap messageGroupOwners;
    private DispatchPolicy dispatchPolicy = new RoundRobinDispatchPolicy();
    private MessageGroupMapFactory messageGroupMapFactory = new MessageGroupHashBucketFactory();
    final Lock sendLock = new ReentrantLock();
    private ExecutorService executor;
    protected final Map<MessageId, Runnable> messagesWaitingForSpace = Collections
            .synchronizedMap(new LinkedHashMap<MessageId, Runnable>());
    private boolean useConsumerPriority = true;
    private boolean strictOrderDispatch = false;
    private final QueueDispatchSelector dispatchSelector;
    private boolean optimizedDispatch = false;
    private boolean firstConsumer = false;
    private int timeBeforeDispatchStarts = 0;
    private int consumersBeforeDispatchStarts = 0;
    private CountDownLatch consumersBeforeStartsLatch;
    private final AtomicLong pendingWakeups = new AtomicLong();
    private boolean allConsumersExclusiveByDefault = false;

    private final Runnable sendMessagesWaitingForSpaceTask = new Runnable() {
        public void run() {
            asyncWakeup();
        }
    };
    private final Runnable expireMessagesTask = new Runnable() {
        public void run() {
            expireMessages();
        }
    };

    private final Object iteratingMutex = new Object() {
    };


    class TimeoutMessage implements Delayed {

        Message message;
        ConnectionContext context;
        long trigger;

        public TimeoutMessage(Message message, ConnectionContext context, long delay) {
            this.message = message;
            this.context = context;
            this.trigger = System.currentTimeMillis() + delay;
        }

        public long getDelay(TimeUnit unit) {
            long n = trigger - System.currentTimeMillis();
            return unit.convert(n, TimeUnit.MILLISECONDS);
        }

        public int compareTo(Delayed delayed) {
            long other = ((TimeoutMessage) delayed).trigger;
            int returnValue;
            if (this.trigger < other) {
                returnValue = -1;
            } else if (this.trigger > other) {
                returnValue = 1;
            } else {
                returnValue = 0;
            }
            return returnValue;
        }

    }

    DelayQueue<TimeoutMessage> flowControlTimeoutMessages = new DelayQueue<TimeoutMessage>();

    class FlowControlTimeoutTask extends Thread {

        @Override
        public void run() {
            TimeoutMessage timeout;
            try {
                while (true) {
                    timeout = flowControlTimeoutMessages.take();
                    if (timeout != null) {
                        synchronized (messagesWaitingForSpace) {
                            if (messagesWaitingForSpace.remove(timeout.message.getMessageId()) != null) {
                                ExceptionResponse response = new ExceptionResponse(
                                        new ResourceAllocationException(
                                                "Usage Manager Memory Limit reached. Stopping producer ("
                                                        + timeout.message.getProducerId()
                                                        + ") to prevent flooding "
                                                        + getActiveMQDestination().getQualifiedName()
                                                        + "."
                                                        + " See http://activemq.apache.org/producer-flow-control.html for more info"));
                                response.setCorrelationId(timeout.message.getCommandId());
                                timeout.context.getConnection().dispatchAsync(response);
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(getName() + "Producer Flow Control Timeout Task is stopping");
                }
            }
        }
    };

    private final FlowControlTimeoutTask flowControlTimeoutTask = new FlowControlTimeoutTask();

    private static final Comparator<Subscription> orderedCompare = new Comparator<Subscription>() {

        public int compare(Subscription s1, Subscription s2) {
            // We want the list sorted in descending order
            return s2.getConsumerInfo().getPriority() - s1.getConsumerInfo().getPriority();
        }
    };

    public Queue(BrokerService brokerService, final ActiveMQDestination destination, MessageStore store,
            DestinationStatistics parentStats, TaskRunnerFactory taskFactory) throws Exception {
        super(brokerService, store, destination, parentStats);
        this.taskFactory = taskFactory;
        this.dispatchSelector = new QueueDispatchSelector(destination);
    }

    public List<Subscription> getConsumers() {
        consumersLock.readLock().lock();
        try {
            return new ArrayList<Subscription>(consumers);
        }finally {
            consumersLock.readLock().unlock();
        }
    }

    // make the queue easily visible in the debugger from its task runner
    // threads
    final class QueueThread extends Thread {
        final Queue queue;

        public QueueThread(Runnable runnable, String name, Queue queue) {
            super(runnable, name);
            this.queue = queue;
        }
    }

    @Override
    public void initialize() throws Exception {
        if (this.messages == null) {
            if (destination.isTemporary() || broker == null || store == null) {
                this.messages = new VMPendingMessageCursor(isPrioritizedMessages());
            } else {
                this.messages = new StoreQueueCursor(broker, this);
            }
        }
        // If a VMPendingMessageCursor don't use the default Producer System
        // Usage
        // since it turns into a shared blocking queue which can lead to a
        // network deadlock.
        // If we are cursoring to disk..it's not and issue because it does not
        // block due
        // to large disk sizes.
        if (messages instanceof VMPendingMessageCursor) {
            this.systemUsage = brokerService.getSystemUsage();
            memoryUsage.setParent(systemUsage.getMemoryUsage());
        }

        this.taskRunner = taskFactory.createTaskRunner(this, "Queue:" + destination.getPhysicalName());

        super.initialize();
        if (store != null) {
            // Restore the persistent messages.
            messages.setSystemUsage(systemUsage);
            messages.setEnableAudit(isEnableAudit());
            messages.setMaxAuditDepth(getMaxAuditDepth());
            messages.setMaxProducersToAudit(getMaxProducersToAudit());
            messages.setUseCache(isUseCache());
            messages.setMemoryUsageHighWaterMark(getCursorMemoryHighWaterMark());
            if (messages.isRecoveryRequired()) {
                store.recover(new MessageRecoveryListener() {
                    double totalMessageCount = store.getMessageCount();
                    int recoveredMessageCount = 0;

                    public boolean recoverMessage(Message message) {
                        // Message could have expired while it was being
                        // loaded..
                        if ((++recoveredMessageCount % 50000) == 0) {
                            LOG.info("cursor for " + getActiveMQDestination().getQualifiedName() + " has recovered "
                                    + recoveredMessageCount + " messages. " +
                                    (int)(recoveredMessageCount*100/totalMessageCount) + "% complete");
                        }
                        if (message.isExpired()) {
                            if (broker.isExpired(message)) {
                                messageExpired(createConnectionContext(), createMessageReference(message));
                                // drop message will decrement so counter
                                // balance here
                                destinationStatistics.getMessages().increment();
                            }
                            return true;
                        }
                        if (hasSpace()) {
                            message.setRegionDestination(Queue.this);
                            messagesLock.writeLock().lock();
                            try{
                                try {
                                    messages.addMessageLast(message);
                                } catch (Exception e) {
                                    LOG.error("Failed to add message to cursor", e);
                                }
                            }finally {
                                messagesLock.writeLock().unlock();
                            }
                            destinationStatistics.getMessages().increment();
                            return true;
                        }
                        return false;
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
            } else {
                int messageCount = store.getMessageCount();
                destinationStatistics.getMessages().setCount(messageCount);
            }
        }
    }

    /*
     * Holder for subscription that needs attention on next iterate browser
     * needs access to existing messages in the queue that have already been
     * dispatched
     */
    class BrowserDispatch {
        QueueBrowserSubscription browser;

        public BrowserDispatch(QueueBrowserSubscription browserSubscription) {
            browser = browserSubscription;
            browser.incrementQueueRef();
        }

        void done() {
            try {
                browser.decrementQueueRef();
            } catch (Exception e) {
                LOG.warn("decrement ref on browser: " + browser, e);
            }
        }

        public QueueBrowserSubscription getBrowser() {
            return browser;
        }
    }

    LinkedList<BrowserDispatch> browserDispatches = new LinkedList<BrowserDispatch>();

    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug(getActiveMQDestination().getQualifiedName() + " add sub: " + sub + ", dequeues: "
                    + getDestinationStatistics().getDequeues().getCount() + ", dispatched: "
                    + getDestinationStatistics().getDispatched().getCount() + ", inflight: "
                    + getDestinationStatistics().getInflight().getCount());
        }

        super.addSubscription(context, sub);
        // synchronize with dispatch method so that no new messages are sent
        // while setting up a subscription. avoid out of order messages,
        // duplicates, etc.
        pagedInPendingDispatchLock.writeLock().lock();
        try {

            sub.add(context, this);

            // needs to be synchronized - so no contention with dispatching
           // consumersLock.
            consumersLock.writeLock().lock();
            try {

                // set a flag if this is a first consumer
                if (consumers.size() == 0) {
                    firstConsumer = true;
                    if (consumersBeforeDispatchStarts != 0) {
                        consumersBeforeStartsLatch = new CountDownLatch(consumersBeforeDispatchStarts - 1);
                    }
                } else {
                    if (consumersBeforeStartsLatch != null) {
                        consumersBeforeStartsLatch.countDown();
                    }
                }

                addToConsumerList(sub);
                if (sub.getConsumerInfo().isExclusive() || isAllConsumersExclusiveByDefault()) {
                    Subscription exclusiveConsumer = dispatchSelector.getExclusiveConsumer();
                    if (exclusiveConsumer == null) {
                        exclusiveConsumer = sub;
                    } else if (sub.getConsumerInfo().getPriority() == Byte.MAX_VALUE ||
                        sub.getConsumerInfo().getPriority() > exclusiveConsumer.getConsumerInfo().getPriority()) {
                        exclusiveConsumer = sub;
                    }
                    dispatchSelector.setExclusiveConsumer(exclusiveConsumer);
                }
            }finally {
                consumersLock.writeLock().unlock();
            }

            if (sub instanceof QueueBrowserSubscription) {
                // tee up for dispatch in next iterate
                QueueBrowserSubscription browserSubscription = (QueueBrowserSubscription) sub;
                pagedInMessagesLock.readLock().lock();
                try{
                    BrowserDispatch browserDispatch = new BrowserDispatch(browserSubscription);
                    browserDispatches.addLast(browserDispatch);
                }finally {
                    pagedInMessagesLock.readLock().unlock();
                }
            }

            if (!(this.optimizedDispatch || isSlave())) {
                wakeup();
            }
        }finally {
            pagedInPendingDispatchLock.writeLock().unlock();
        }
        if (this.optimizedDispatch || isSlave()) {
            // Outside of dispatchLock() to maintain the lock hierarchy of
            // iteratingMutex -> dispatchLock. - see
            // https://issues.apache.org/activemq/browse/AMQ-1878
            wakeup();
        }
    }

    public void removeSubscription(ConnectionContext context, Subscription sub, long lastDeiveredSequenceId)
            throws Exception {
        super.removeSubscription(context, sub, lastDeiveredSequenceId);
        // synchronize with dispatch method so that no new messages are sent
        // while removing up a subscription.
        pagedInPendingDispatchLock.writeLock().lock();
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(getActiveMQDestination().getQualifiedName() + " remove sub: " + sub + ", lastDeliveredSeqId: " + lastDeiveredSequenceId + ", dequeues: "
                        + getDestinationStatistics().getDequeues().getCount() + ", dispatched: "
                        + getDestinationStatistics().getDispatched().getCount() + ", inflight: "
                        + getDestinationStatistics().getInflight().getCount());
            }
            consumersLock.writeLock().lock();
            try {
                removeFromConsumerList(sub);
                if (sub.getConsumerInfo().isExclusive()) {
                    Subscription exclusiveConsumer = dispatchSelector.getExclusiveConsumer();
                    if (exclusiveConsumer == sub) {
                        exclusiveConsumer = null;
                        for (Subscription s : consumers) {
                            if (s.getConsumerInfo().isExclusive()
                                    && (exclusiveConsumer == null || s.getConsumerInfo().getPriority() > exclusiveConsumer
                                            .getConsumerInfo().getPriority())) {
                                exclusiveConsumer = s;

                            }
                        }
                        dispatchSelector.setExclusiveConsumer(exclusiveConsumer);
                    }
                } else if (isAllConsumersExclusiveByDefault()) {
                    Subscription exclusiveConsumer = null;
                    for (Subscription s : consumers) {
                        if (exclusiveConsumer == null
                                || s.getConsumerInfo().getPriority() > exclusiveConsumer
                                .getConsumerInfo().getPriority()) {
                            exclusiveConsumer = s;
                                }
                    }
                    dispatchSelector.setExclusiveConsumer(exclusiveConsumer);
                }
                ConsumerId consumerId = sub.getConsumerInfo().getConsumerId();
                getMessageGroupOwners().removeConsumer(consumerId);

                // redeliver inflight messages

                boolean markAsRedelivered = false;
                MessageReference lastDeliveredRef = null;
                List<MessageReference> unAckedMessages = sub.remove(context, this);

                // locate last redelivered in unconsumed list (list in delivery rather than seq order)
                if (lastDeiveredSequenceId != 0) {
                    for (MessageReference ref : unAckedMessages) {
                        if (ref.getMessageId().getBrokerSequenceId() == lastDeiveredSequenceId) {
                            lastDeliveredRef = ref;
                            markAsRedelivered = true;
                            LOG.debug("found lastDeliveredSeqID: " + lastDeiveredSequenceId + ", message reference: " + ref.getMessageId());
                            break;
                        }
                    }
                }
                for (MessageReference ref : unAckedMessages) {
                    QueueMessageReference qmr = (QueueMessageReference) ref;
                    if (qmr.getLockOwner() == sub) {
                        qmr.unlock();

                        // have no delivery information
                        if (lastDeiveredSequenceId == 0) {
                            qmr.incrementRedeliveryCounter();
                        } else {
                            if (markAsRedelivered) {
                                qmr.incrementRedeliveryCounter();
                            }
                            if (ref == lastDeliveredRef) {
                                // all that follow were not redelivered
                                markAsRedelivered = false;
                            }
                        }
                    }
                    redeliveredWaitingDispatch.add(qmr);
                }
                if (!redeliveredWaitingDispatch.isEmpty()) {
                    doDispatch(new ArrayList<QueueMessageReference>());
                }
            }finally {
                consumersLock.writeLock().unlock();
            }
            if (!(this.optimizedDispatch || isSlave())) {
                wakeup();
            }
        }finally {
            pagedInPendingDispatchLock.writeLock().unlock();
        }
        if (this.optimizedDispatch || isSlave()) {
            // Outside of dispatchLock() to maintain the lock hierarchy of
            // iteratingMutex -> dispatchLock. - see
            // https://issues.apache.org/activemq/browse/AMQ-1878
            wakeup();
        }
    }

    public void send(final ProducerBrokerExchange producerExchange, final Message message) throws Exception {
        final ConnectionContext context = producerExchange.getConnectionContext();
        // There is delay between the client sending it and it arriving at the
        // destination.. it may have expired.
        message.setRegionDestination(this);
        final ProducerInfo producerInfo = producerExchange.getProducerState().getInfo();
        final boolean sendProducerAck = !message.isResponseRequired() && producerInfo.getWindowSize() > 0
                && !context.isInRecoveryMode();
        if (message.isExpired()) {
            // message not stored - or added to stats yet - so chuck here
            broker.getRoot().messageExpired(context, message, null);
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
                            .info("Usage Manager Memory Limit ("
                                    + memoryUsage.getLimit()
                                    + ") reached on "
                                    + getActiveMQDestination().getQualifiedName()
                                    + ". Producers will be throttled to the rate at which messages are removed from this destination to prevent flooding it."
                                    + " See http://activemq.apache.org/producer-flow-control.html for more info");
                }

                if (systemUsage.isSendFailIfNoSpace()) {
                    throw new ResourceAllocationException("Usage Manager Memory Limit reached. Stopping producer ("
                            + message.getProducerId() + ") to prevent flooding "
                            + getActiveMQDestination().getQualifiedName() + "."
                            + " See http://activemq.apache.org/producer-flow-control.html for more info");
                }

                // We can avoid blocking due to low usage if the producer is
                // sending
                // a sync message or if it is using a producer window
                if (producerInfo.getWindowSize() > 0 || message.isResponseRequired()) {
                    // copy the exchange state since the context will be
                    // modified while we are waiting
                    // for space.
                    final ProducerBrokerExchange producerExchangeCopy = producerExchange.copy();
                    synchronized (messagesWaitingForSpace) {
                     // Start flow control timeout task
                        // Prevent trying to start it multiple times
                        if (!flowControlTimeoutTask.isAlive()) {
                            flowControlTimeoutTask.setName(getName()+" Producer Flow Control Timeout Task");
                            flowControlTimeoutTask.start();
                        }
                        messagesWaitingForSpace.put(message.getMessageId(), new Runnable() {
                            public void run() {

                                try {
                                    // While waiting for space to free up... the
                                    // message may have expired.
                                    if (message.isExpired()) {
                                        LOG.error("expired waiting for space..");
                                        broker.messageExpired(context, message, null);
                                        destinationStatistics.getExpired().increment();
                                    } else {
                                        doMessageSend(producerExchangeCopy, message);
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
                                    } else {
                                        LOG.debug("unexpected exception on deferred send of :" + message, e);
                                    }
                                }
                            }
                        });

                        if (systemUsage.getSendFailIfNoSpaceAfterTimeout() != 0) {
                            flowControlTimeoutMessages.add(new TimeoutMessage(message, context, systemUsage
                                    .getSendFailIfNoSpaceAfterTimeout()));
                        }

                        registerCallbackForNotFullNotification();
                        context.setDontSendReponse(true);
                        return;
                    }

                } else {

                    if (memoryUsage.isFull()) {
                        waitForSpace(context, memoryUsage, "Usage Manager Memory Limit reached. Producer ("
                                + message.getProducerId() + ") stopped to prevent flooding "
                                + getActiveMQDestination().getQualifiedName() + "."
                                + " See http://activemq.apache.org/producer-flow-control.html for more info");
                    }

                    // The usage manager could have delayed us by the time
                    // we unblock the message could have expired..
                    if (message.isExpired()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Expired message: " + message);
                        }
                        broker.getRoot().messageExpired(context, message, null);
                        return;
                    }
                }
            }
        }
        doMessageSend(producerExchange, message);
        if (sendProducerAck) {
            ProducerAck ack = new ProducerAck(producerInfo.getProducerId(), message.getSize());
            context.getConnection().dispatchAsync(ack);
        }
    }

    private void registerCallbackForNotFullNotification() {
        // If the usage manager is not full, then the task will not
        // get called..
        if (!memoryUsage.notifyCallbackWhenNotFull(sendMessagesWaitingForSpaceTask)) {
            // so call it directly here.
            sendMessagesWaitingForSpaceTask.run();
        }
    }

    void doMessageSend(final ProducerBrokerExchange producerExchange, final Message message) throws IOException,
            Exception {
        final ConnectionContext context = producerExchange.getConnectionContext();
        Future<Object> result = null;

        checkUsage(context, message);
        sendLock.lockInterruptibly();
        try {
            if (store != null && message.isPersistent()) {
                message.getMessageId().setBrokerSequenceId(getDestinationSequenceId());
                if (messages.isCacheEnabled()) {
                    result = store.asyncAddQueueMessage(context, message);
                } else {
                    store.addMessage(context, message);
                }
                if (isReduceMemoryFootprint()) {
                    message.clearMarshalledState();
                }
            }
            if (context.isInTransaction()) {
                // If this is a transacted message.. increase the usage now so that
                // a big TX does not blow up
                // our memory. This increment is decremented once the tx finishes..
                message.incrementReferenceCount();

                context.getTransaction().addSynchronization(new Synchronization() {
                    @Override
                    public void afterCommit() throws Exception {
                        sendLock.lockInterruptibly();
                        try {
                            // It could take while before we receive the commit
                            // op, by that time the message could have expired..
                            if (broker.isExpired(message)) {
                                broker.messageExpired(context, message, null);
                                destinationStatistics.getExpired().increment();
                                return;
                            }
                            sendMessage(message);
                        } finally {
                            sendLock.unlock();
                            message.decrementReferenceCount();
                        }
                        messageSent(context, message);
                    }
                    @Override
                    public void afterRollback() throws Exception {
                        message.decrementReferenceCount();
                    }
                });
            } else {
                // Add to the pending list, this takes care of incrementing the
                // usage manager.
                sendMessage(message);
            }
        } finally {
            sendLock.unlock();
        }
        if (!context.isInTransaction()) {
            messageSent(context, message);
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

    private void checkUsage(ConnectionContext context, Message message) throws ResourceAllocationException, IOException, InterruptedException {
        if (message.isPersistent()) {
            if (store != null && systemUsage.getStoreUsage().isFull(getStoreUsageHighWaterMark())) {
                final String logMessage = "Persistent store is Full, " + getStoreUsageHighWaterMark() + "% of "
                    + systemUsage.getStoreUsage().getLimit() + ". Stopping producer ("
                    + message.getProducerId() + ") to prevent flooding "
                    + getActiveMQDestination().getQualifiedName() + "."
                    + " See http://activemq.apache.org/producer-flow-control.html for more info";

                waitForSpace(context, systemUsage.getStoreUsage(), getStoreUsageHighWaterMark(), logMessage);
            }
        } else if (messages.getSystemUsage() != null && systemUsage.getTempUsage().isFull()) {
            final String logMessage = "Temp Store is Full ("
                    + systemUsage.getTempUsage().getPercentUsage() + "% of " + systemUsage.getTempUsage().getLimit()
                    +"). Stopping producer (" + message.getProducerId()
                + ") to prevent flooding " + getActiveMQDestination().getQualifiedName() + "."
                + " See http://activemq.apache.org/producer-flow-control.html for more info";

            waitForSpace(context, messages.getSystemUsage().getTempUsage(), logMessage);
        }
    }

    private void expireMessages() {
        if (LOG.isDebugEnabled()) {
            LOG.debug(getActiveMQDestination().getQualifiedName() + " expiring messages ..");
        }

        // just track the insertion count
        List<Message> browsedMessages = new AbstractList<Message>() {
            int size = 0;

            @Override
            public void add(int index, Message element) {
                size++;
            }

            @Override
            public int size() {
                return size;
            }

            @Override
            public Message get(int index) {
                return null;
            }
        };
        doBrowse(browsedMessages, this.getMaxExpirePageSize());
        asyncWakeup();
        if (LOG.isDebugEnabled()) {
            LOG.debug(getActiveMQDestination().getQualifiedName() + " expiring messages done.");
        }
    }

    public void gc() {
    }

    public void acknowledge(ConnectionContext context, Subscription sub, MessageAck ack, MessageReference node)
            throws IOException {
        messageConsumed(context, node);
        if (store != null && node.isPersistent()) {
            // the original ack may be a ranged ack, but we are trying to delete
            // a specific
            // message store here so we need to convert to a non ranged ack.
            if (ack.getMessageCount() > 0) {
                // Dup the ack
                MessageAck a = new MessageAck();
                ack.copy(a);
                ack = a;
                // Convert to non-ranged.
                ack.setFirstMessageId(node.getMessageId());
                ack.setLastMessageId(node.getMessageId());
                ack.setMessageCount(1);
            }

            store.removeAsyncMessage(context, ack);
        }
    }

    Message loadMessage(MessageId messageId) throws IOException {
        Message msg = null;
        if (store != null) { // can be null for a temp q
            msg = store.getMessage(messageId);
            if (msg != null) {
                msg.setRegionDestination(this);
            }
        }
        return msg;
    }

    @Override
    public String toString() {
        int size = 0;
        messagesLock.readLock().lock();
        try{
            size = messages.size();
        }finally {
            messagesLock.readLock().unlock();
        }
        return destination.getQualifiedName() + ", subscriptions=" + consumers.size()
                + ", memory=" + memoryUsage.getPercentUsage() + "%, size=" + size + ", in flight groups="
                + messageGroupOwners;
    }

    public void start() throws Exception {
        if (memoryUsage != null) {
            memoryUsage.start();
        }
        if (systemUsage.getStoreUsage() != null) {
            systemUsage.getStoreUsage().start();
        }
        systemUsage.getMemoryUsage().addUsageListener(this);
        messages.start();
        if (getExpireMessagesPeriod() > 0) {
            scheduler.schedualPeriodically(expireMessagesTask, getExpireMessagesPeriod());
        }
        doPageIn(false);
    }

    public void stop() throws Exception {
        if (taskRunner != null) {
            taskRunner.shutdown();
        }
        if (this.executor != null) {
            this.executor.shutdownNow();
        }

        scheduler.cancel(expireMessagesTask);

        if (flowControlTimeoutTask.isAlive()) {
            flowControlTimeoutTask.interrupt();
        }

        if (messages != null) {
            messages.stop();
        }

        systemUsage.getMemoryUsage().removeUsageListener(this);
        if (memoryUsage != null) {
            memoryUsage.stop();
        }
        if (store != null) {
            store.stop();
        }
    }

    // Properties
    // -------------------------------------------------------------------------
    @Override
    public ActiveMQDestination getActiveMQDestination() {
        return destination;
    }

    public MessageGroupMap getMessageGroupOwners() {
        if (messageGroupOwners == null) {
            messageGroupOwners = getMessageGroupMapFactory().createMessageGroupMap();
        }
        return messageGroupOwners;
    }

    public DispatchPolicy getDispatchPolicy() {
        return dispatchPolicy;
    }

    public void setDispatchPolicy(DispatchPolicy dispatchPolicy) {
        this.dispatchPolicy = dispatchPolicy;
    }

    public MessageGroupMapFactory getMessageGroupMapFactory() {
        return messageGroupMapFactory;
    }

    public void setMessageGroupMapFactory(MessageGroupMapFactory messageGroupMapFactory) {
        this.messageGroupMapFactory = messageGroupMapFactory;
    }

    public PendingMessageCursor getMessages() {
        return this.messages;
    }

    public void setMessages(PendingMessageCursor messages) {
        this.messages = messages;
    }

    public boolean isUseConsumerPriority() {
        return useConsumerPriority;
    }

    public void setUseConsumerPriority(boolean useConsumerPriority) {
        this.useConsumerPriority = useConsumerPriority;
    }

    public boolean isStrictOrderDispatch() {
        return strictOrderDispatch;
    }

    public void setStrictOrderDispatch(boolean strictOrderDispatch) {
        this.strictOrderDispatch = strictOrderDispatch;
    }

    public boolean isOptimizedDispatch() {
        return optimizedDispatch;
    }

    public void setOptimizedDispatch(boolean optimizedDispatch) {
        this.optimizedDispatch = optimizedDispatch;
    }

    public int getTimeBeforeDispatchStarts() {
        return timeBeforeDispatchStarts;
    }

    public void setTimeBeforeDispatchStarts(int timeBeforeDispatchStarts) {
        this.timeBeforeDispatchStarts = timeBeforeDispatchStarts;
    }

    public int getConsumersBeforeDispatchStarts() {
        return consumersBeforeDispatchStarts;
    }

    public void setConsumersBeforeDispatchStarts(int consumersBeforeDispatchStarts) {
        this.consumersBeforeDispatchStarts = consumersBeforeDispatchStarts;
    }

    public void setAllConsumersExclusiveByDefault(boolean allConsumersExclusiveByDefault) {
        this.allConsumersExclusiveByDefault = allConsumersExclusiveByDefault;
    }

    public boolean isAllConsumersExclusiveByDefault() {
        return allConsumersExclusiveByDefault;
    }


    // Implementation methods
    // -------------------------------------------------------------------------
    private QueueMessageReference createMessageReference(Message message) {
        QueueMessageReference result = new IndirectMessageReference(message);
        return result;
    }

    public Message[] browse() {
        List<Message> browseList = new ArrayList<Message>();
        doBrowse(browseList, getMaxBrowsePageSize());
        return browseList.toArray(new Message[browseList.size()]);
    }

    public void doBrowse(List<Message> browseList, int max) {
        final ConnectionContext connectionContext = createConnectionContext();
        try {
            pageInMessages(false);
            List<MessageReference> toExpire = new ArrayList<MessageReference>();

            pagedInPendingDispatchLock.writeLock().lock();
            try {
                addAll(pagedInPendingDispatch, browseList, max, toExpire);
                for (MessageReference ref : toExpire) {
                    pagedInPendingDispatch.remove(ref);
                    if (broker.isExpired(ref)) {
                        LOG.debug("expiring from pagedInPending: " + ref);
                        messageExpired(connectionContext, ref);
                    }
                }
            } finally {
                pagedInPendingDispatchLock.writeLock().unlock();
            }
            toExpire.clear();
            pagedInMessagesLock.readLock().lock();
            try {
                addAll(pagedInMessages.values(), browseList, max, toExpire);
            } finally {
                pagedInMessagesLock.readLock().unlock();
            }
            for (MessageReference ref : toExpire) {
                if (broker.isExpired(ref)) {
                    LOG.debug("expiring from pagedInMessages: " + ref);
                    messageExpired(connectionContext, ref);
                } else {
                    pagedInMessagesLock.writeLock().lock();
                    try {
                        pagedInMessages.remove(ref.getMessageId());
                    } finally {
                        pagedInMessagesLock.writeLock().unlock();
                    }
                }
            }

            if (browseList.size() < getMaxBrowsePageSize()) {
                messagesLock.writeLock().lock();
                try {
                    try {
                        messages.reset();
                        while (messages.hasNext() && browseList.size() < max) {
                            MessageReference node = messages.next();
                            if (node.isExpired()) {
                                if (broker.isExpired(node)) {
                                    LOG.debug("expiring from messages: " + node);
                                    messageExpired(connectionContext, createMessageReference(node.getMessage()));
                                }
                                messages.remove();
                            } else {
                                messages.rollback(node.getMessageId());
                                if (browseList.contains(node.getMessage()) == false) {
                                    browseList.add(node.getMessage());
                                }
                            }
                            node.decrementReferenceCount();
                        }
                    } finally {
                        messages.release();
                    }
                } finally {
                    messagesLock.writeLock().unlock();
                }
            }

        } catch (Exception e) {
            LOG.error("Problem retrieving message for browse", e);
        }
    }

    private void addAll(Collection<QueueMessageReference> refs, List<Message> l, int maxBrowsePageSize,
            List<MessageReference> toExpire) throws Exception {
        for (Iterator<QueueMessageReference> i = refs.iterator(); i.hasNext() && l.size() < getMaxBrowsePageSize();) {
            QueueMessageReference ref = i.next();
            if (ref.isExpired()) {
                toExpire.add(ref);
            } else if (l.contains(ref.getMessage()) == false) {
                l.add(ref.getMessage());
            }
        }
    }

    public QueueMessageReference getMessage(String id) {
        MessageId msgId = new MessageId(id);
        pagedInMessagesLock.readLock().lock();
        try{
            QueueMessageReference ref = this.pagedInMessages.get(msgId);
            if (ref != null) {
                return ref;
            }
        }finally {
            pagedInMessagesLock.readLock().unlock();
        }
        messagesLock.readLock().lock();
        try{
            try {
                messages.reset();
                while (messages.hasNext()) {
                    MessageReference mr = messages.next();
                    QueueMessageReference qmr = createMessageReference(mr.getMessage());
                    qmr.decrementReferenceCount();
                    messages.rollback(qmr.getMessageId());
                    if (msgId.equals(qmr.getMessageId())) {
                        return qmr;
                    }
                }
            } finally {
                messages.release();
            }
        }finally {
            messagesLock.readLock().unlock();
        }
        return null;
    }

    public void purge() throws Exception {
        ConnectionContext c = createConnectionContext();
        List<MessageReference> list = null;
        do {
            doPageIn(true);
            pagedInMessagesLock.readLock().lock();
            try {
                list = new ArrayList<MessageReference>(pagedInMessages.values());
            }finally {
                pagedInMessagesLock.readLock().unlock();
            }

            for (MessageReference ref : list) {
                try {
                    QueueMessageReference r = (QueueMessageReference) ref;
                    removeMessage(c, r);
                } catch (IOException e) {
                }
            }
            // don't spin/hang if stats are out and there is nothing left in the
            // store
        } while (!list.isEmpty() && this.destinationStatistics.getMessages().getCount() > 0);
        if (this.destinationStatistics.getMessages().getCount() > 0) {
            LOG.warn(getActiveMQDestination().getQualifiedName()
                    + " after purge complete, message count stats report: "
                    + this.destinationStatistics.getMessages().getCount());
        }
        gc();
        this.destinationStatistics.getMessages().setCount(0);
        getMessages().clear();
    }

    public void clearPendingMessages() {
        messagesLock.writeLock().lock();
        try {
            if (store != null) {
                store.resetBatching();
            }
            messages.gc();
            asyncWakeup();
        } finally {
            messagesLock.writeLock().unlock();
        }
    }

    /**
     * Removes the message matching the given messageId
     */
    public boolean removeMessage(String messageId) throws Exception {
        return removeMatchingMessages(createMessageIdFilter(messageId), 1) > 0;
    }

    /**
     * Removes the messages matching the given selector
     *
     * @return the number of messages removed
     */
    public int removeMatchingMessages(String selector) throws Exception {
        return removeMatchingMessages(selector, -1);
    }

    /**
     * Removes the messages matching the given selector up to the maximum number
     * of matched messages
     *
     * @return the number of messages removed
     */
    public int removeMatchingMessages(String selector, int maximumMessages) throws Exception {
        return removeMatchingMessages(createSelectorFilter(selector), maximumMessages);
    }

    /**
     * Removes the messages matching the given filter up to the maximum number
     * of matched messages
     *
     * @return the number of messages removed
     */
    public int removeMatchingMessages(MessageReferenceFilter filter, int maximumMessages) throws Exception {
        int movedCounter = 0;
        Set<MessageReference> set = new HashSet<MessageReference>();
        ConnectionContext context = createConnectionContext();
        do {
            doPageIn(true);
            pagedInMessagesLock.readLock().lock();
            try{
                set.addAll(pagedInMessages.values());
            }finally {
                pagedInMessagesLock.readLock().unlock();
            }
            List<MessageReference> list = new ArrayList<MessageReference>(set);
            for (MessageReference ref : list) {
                IndirectMessageReference r = (IndirectMessageReference) ref;
                if (filter.evaluate(context, r)) {

                    removeMessage(context, r);
                    set.remove(r);
                    if (++movedCounter >= maximumMessages && maximumMessages > 0) {
                        return movedCounter;
                    }
                }
            }
        } while (set.size() < this.destinationStatistics.getMessages().getCount());
        return movedCounter;
    }

    /**
     * Copies the message matching the given messageId
     */
    public boolean copyMessageTo(ConnectionContext context, String messageId, ActiveMQDestination dest)
            throws Exception {
        return copyMatchingMessages(context, createMessageIdFilter(messageId), dest, 1) > 0;
    }

    /**
     * Copies the messages matching the given selector
     *
     * @return the number of messages copied
     */
    public int copyMatchingMessagesTo(ConnectionContext context, String selector, ActiveMQDestination dest)
            throws Exception {
        return copyMatchingMessagesTo(context, selector, dest, -1);
    }

    /**
     * Copies the messages matching the given selector up to the maximum number
     * of matched messages
     *
     * @return the number of messages copied
     */
    public int copyMatchingMessagesTo(ConnectionContext context, String selector, ActiveMQDestination dest,
            int maximumMessages) throws Exception {
        return copyMatchingMessages(context, createSelectorFilter(selector), dest, maximumMessages);
    }

    /**
     * Copies the messages matching the given filter up to the maximum number of
     * matched messages
     *
     * @return the number of messages copied
     */
    public int copyMatchingMessages(ConnectionContext context, MessageReferenceFilter filter, ActiveMQDestination dest,
            int maximumMessages) throws Exception {
        int movedCounter = 0;
        int count = 0;
        Set<MessageReference> set = new HashSet<MessageReference>();
        do {
            int oldMaxSize = getMaxPageSize();
            setMaxPageSize((int) this.destinationStatistics.getMessages().getCount());
            doPageIn(true);
            setMaxPageSize(oldMaxSize);
            pagedInMessagesLock.readLock().lock();
            try {
                set.addAll(pagedInMessages.values());
            }finally {
                pagedInMessagesLock.readLock().unlock();
            }
            List<MessageReference> list = new ArrayList<MessageReference>(set);
            for (MessageReference ref : list) {
                IndirectMessageReference r = (IndirectMessageReference) ref;
                if (filter.evaluate(context, r)) {

                    r.incrementReferenceCount();
                    try {
                        Message m = r.getMessage();
                        BrokerSupport.resend(context, m, dest);
                        if (++movedCounter >= maximumMessages && maximumMessages > 0) {
                            return movedCounter;
                        }
                    } finally {
                        r.decrementReferenceCount();
                    }
                }
                count++;
            }
        } while (count < this.destinationStatistics.getMessages().getCount());
        return movedCounter;
    }

    /**
     * Move a message
     *
     * @param context
     *            connection context
     * @param m
     *            QueueMessageReference
     * @param dest
     *            ActiveMQDestination
     * @throws Exception
     */
    public boolean moveMessageTo(ConnectionContext context, QueueMessageReference m, ActiveMQDestination dest) throws Exception {
        BrokerSupport.resend(context, m.getMessage(), dest);
        removeMessage(context, m);
        messagesLock.writeLock().lock();
        try{
            messages.rollback(m.getMessageId());
        }finally {
            messagesLock.writeLock().unlock();
        }
        return true;
    }

    /**
     * Moves the message matching the given messageId
     */
    public boolean moveMessageTo(ConnectionContext context, String messageId, ActiveMQDestination dest)
            throws Exception {
        return moveMatchingMessagesTo(context, createMessageIdFilter(messageId), dest, 1) > 0;
    }

    /**
     * Moves the messages matching the given selector
     *
     * @return the number of messages removed
     */
    public int moveMatchingMessagesTo(ConnectionContext context, String selector, ActiveMQDestination dest)
            throws Exception {
        return moveMatchingMessagesTo(context, selector, dest, Integer.MAX_VALUE);
    }

    /**
     * Moves the messages matching the given selector up to the maximum number
     * of matched messages
     */
    public int moveMatchingMessagesTo(ConnectionContext context, String selector, ActiveMQDestination dest,
            int maximumMessages) throws Exception {
        return moveMatchingMessagesTo(context, createSelectorFilter(selector), dest, maximumMessages);
    }

    /**
     * Moves the messages matching the given filter up to the maximum number of
     * matched messages
     */
    public int moveMatchingMessagesTo(ConnectionContext context, MessageReferenceFilter filter,
            ActiveMQDestination dest, int maximumMessages) throws Exception {
        int movedCounter = 0;
        Set<QueueMessageReference> set = new HashSet<QueueMessageReference>();
        do {
            doPageIn(true);
            pagedInMessagesLock.readLock().lock();
            try{
                set.addAll(pagedInMessages.values());
            }finally {
                pagedInMessagesLock.readLock().unlock();
            }
            List<QueueMessageReference> list = new ArrayList<QueueMessageReference>(set);
            for (QueueMessageReference ref : list) {
                if (filter.evaluate(context, ref)) {
                    // We should only move messages that can be locked.
                    moveMessageTo(context, ref, dest);
                    set.remove(ref);
                    if (++movedCounter >= maximumMessages && maximumMessages > 0) {
                        return movedCounter;
                    }
                }
            }
        } while (set.size() < this.destinationStatistics.getMessages().getCount() && set.size() < maximumMessages);
        return movedCounter;
    }

    BrowserDispatch getNextBrowserDispatch() {
        pagedInMessagesLock.readLock().lock();
        try{
            if (browserDispatches.isEmpty()) {
                return null;
            }
            return browserDispatches.removeFirst();
        }finally {
            pagedInMessagesLock.readLock().unlock();
        }

    }

    /**
     * @return true if we would like to iterate again
     * @see org.apache.activemq.thread.Task#iterate()
     */
    public boolean iterate() {
        MDC.put("activemq.destination", getName());
        boolean pageInMoreMessages = false;
        synchronized (iteratingMutex) {

            // do early to allow dispatch of these waiting messages
            synchronized (messagesWaitingForSpace) {
                Iterator<Runnable> it = messagesWaitingForSpace.values().iterator();
                while (it.hasNext()) {
                    if (!memoryUsage.isFull()) {
                        Runnable op = it.next();
                        it.remove();
                        op.run();
                    } else {
                        registerCallbackForNotFullNotification();
                        break;
                    }
                }
            }

            if (firstConsumer) {
                firstConsumer = false;
                try {
                    if (consumersBeforeDispatchStarts > 0) {
                        int timeout = 1000; // wait one second by default if
                                            // consumer count isn't reached
                        if (timeBeforeDispatchStarts > 0) {
                            timeout = timeBeforeDispatchStarts;
                        }
                        if (consumersBeforeStartsLatch.await(timeout, TimeUnit.MILLISECONDS)) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(consumers.size() + " consumers subscribed. Starting dispatch.");
                            }
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(timeout + " ms elapsed and " + consumers.size()
                                        + " consumers subscribed. Starting dispatch.");
                            }
                        }
                    }
                    if (timeBeforeDispatchStarts > 0 && consumersBeforeDispatchStarts <= 0) {
                        iteratingMutex.wait(timeBeforeDispatchStarts);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(timeBeforeDispatchStarts + " ms elapsed. Starting dispatch.");
                        }
                    }
                } catch (Exception e) {
                    LOG.error(e.toString());
                }
            }

            BrowserDispatch pendingBrowserDispatch = getNextBrowserDispatch();

            messagesLock.readLock().lock();
            try{
                pageInMoreMessages |= !messages.isEmpty();
            }finally {
                messagesLock.readLock().unlock();
            }

            pagedInPendingDispatchLock.readLock().lock();
            try {
                pageInMoreMessages |= !pagedInPendingDispatch.isEmpty();
            }finally {
                pagedInPendingDispatchLock.readLock().unlock();
            }

            // Perhaps we should page always into the pagedInPendingDispatch
            // list if
            // !messages.isEmpty(), and then if
            // !pagedInPendingDispatch.isEmpty()
            // then we do a dispatch.
            if (pageInMoreMessages || pendingBrowserDispatch != null || !redeliveredWaitingDispatch.isEmpty()) {
                try {
                    pageInMessages(pendingBrowserDispatch != null);

                } catch (Throwable e) {
                    LOG.error("Failed to page in more queue messages ", e);
                }
            }

            if (pendingBrowserDispatch != null) {
                ArrayList<QueueMessageReference> alreadyDispatchedMessages = null;
                pagedInMessagesLock.readLock().lock();
                try{
                    alreadyDispatchedMessages = new ArrayList<QueueMessageReference>(pagedInMessages.values());
                }finally {
                    pagedInMessagesLock.readLock().unlock();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("dispatch to browser: " + pendingBrowserDispatch.getBrowser()
                            + ", already dispatched/paged count: " + alreadyDispatchedMessages.size());
                }
                do {
                    try {
                        MessageEvaluationContext msgContext = new NonCachedMessageEvaluationContext();
                        msgContext.setDestination(destination);

                        QueueBrowserSubscription browser = pendingBrowserDispatch.getBrowser();
                        for (QueueMessageReference node : alreadyDispatchedMessages) {
                            if (!node.isAcked()) {
                                msgContext.setMessageReference(node);
                                if (browser.matches(node, msgContext)) {
                                    browser.add(node);
                                }
                            }
                        }
                        pendingBrowserDispatch.done();
                    } catch (Exception e) {
                        LOG.warn("exception on dispatch to browser: " + pendingBrowserDispatch.getBrowser(), e);
                    }

                } while ((pendingBrowserDispatch = getNextBrowserDispatch()) != null);
            }

            if (pendingWakeups.get() > 0) {
                pendingWakeups.decrementAndGet();
            }
            MDC.remove("activemq.destination");
            return pendingWakeups.get() > 0;
        }
    }

    protected MessageReferenceFilter createMessageIdFilter(final String messageId) {
        return new MessageReferenceFilter() {
            public boolean evaluate(ConnectionContext context, MessageReference r) {
                return messageId.equals(r.getMessageId().toString());
            }

            @Override
            public String toString() {
                return "MessageIdFilter: " + messageId;
            }
        };
    }

    protected MessageReferenceFilter createSelectorFilter(String selector) throws InvalidSelectorException {
        final BooleanExpression selectorExpression = SelectorParser.parse(selector);

        return new MessageReferenceFilter() {
            public boolean evaluate(ConnectionContext context, MessageReference r) throws JMSException {
                MessageEvaluationContext messageEvaluationContext = context.getMessageEvaluationContext();

                messageEvaluationContext.setMessageReference(r);
                if (messageEvaluationContext.getDestination() == null) {
                    messageEvaluationContext.setDestination(getActiveMQDestination());
                }

                return selectorExpression.matches(messageEvaluationContext);
            }
        };
    }

    protected void removeMessage(ConnectionContext c, QueueMessageReference r) throws IOException {
        removeMessage(c, null, r);
        pagedInPendingDispatchLock.writeLock().lock();
        try {
            pagedInPendingDispatch.remove(r);
        } finally {
            pagedInPendingDispatchLock.writeLock().unlock();
        }

    }

    protected void removeMessage(ConnectionContext c, Subscription subs, QueueMessageReference r) throws IOException {
        MessageAck ack = new MessageAck();
        ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
        ack.setDestination(destination);
        ack.setMessageID(r.getMessageId());
        removeMessage(c, subs, r, ack);
    }

    protected void removeMessage(ConnectionContext context, Subscription sub, final QueueMessageReference reference,
            MessageAck ack) throws IOException {
        reference.setAcked(true);
        // This sends the ack the the journal..
        if (!ack.isInTransaction()) {
            acknowledge(context, sub, ack, reference);
            getDestinationStatistics().getDequeues().increment();
            dropMessage(reference);
        } else {
            try {
                acknowledge(context, sub, ack, reference);
            } finally {
                context.getTransaction().addSynchronization(new Synchronization() {

                    @Override
                    public void afterCommit() throws Exception {
                        getDestinationStatistics().getDequeues().increment();
                        dropMessage(reference);
                        wakeup();
                    }

                    @Override
                    public void afterRollback() throws Exception {
                        reference.setAcked(false);
                    }
                });
            }
        }
        if (ack.isPoisonAck()) {
            // message gone to DLQ, is ok to allow redelivery
            messagesLock.writeLock().lock();
            try{
                messages.rollback(reference.getMessageId());
            }finally {
                messagesLock.writeLock().unlock();
            }
        }

    }

    private void dropMessage(QueueMessageReference reference) {
        reference.drop();
        destinationStatistics.getMessages().decrement();
        pagedInMessagesLock.writeLock().lock();
        try{
            pagedInMessages.remove(reference.getMessageId());
        }finally {
            pagedInMessagesLock.writeLock().unlock();
        }
    }

    public void messageExpired(ConnectionContext context, MessageReference reference) {
        messageExpired(context, null, reference);
    }

    public void messageExpired(ConnectionContext context, Subscription subs, MessageReference reference) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("message expired: " + reference);
        }
        broker.messageExpired(context, reference, subs);
        destinationStatistics.getExpired().increment();
        try {
            removeMessage(context, subs, (QueueMessageReference) reference);
        } catch (IOException e) {
            LOG.error("Failed to remove expired Message from the store ", e);
        }
    }

    final void sendMessage(final Message msg) throws Exception {
        messagesLock.writeLock().lock();
        try{
            messages.addMessageLast(msg);
        }finally {
            messagesLock.writeLock().unlock();
        }
    }

    final void messageSent(final ConnectionContext context, final Message msg) throws Exception {
        destinationStatistics.getEnqueues().increment();
        destinationStatistics.getMessages().increment();
        messageDelivered(context, msg);
        consumersLock.readLock().lock();
        try {
            if (consumers.isEmpty()) {
                onMessageWithNoConsumers(context, msg);
            }
        }finally {
            consumersLock.readLock().unlock();
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Message " + msg.getMessageId() + " sent to " + this.destination);
        }
        wakeup();
    }

    public void wakeup() {
        if (optimizedDispatch || isSlave()) {
            iterate();
            pendingWakeups.incrementAndGet();
        } else {
            asyncWakeup();
        }
    }

    private void asyncWakeup() {
        try {
            pendingWakeups.incrementAndGet();
            this.taskRunner.wakeup();
        } catch (InterruptedException e) {
            LOG.warn("Async task tunner failed to wakeup ", e);
        }
    }

    private boolean isSlave() {
        return broker.getBrokerService().isSlave();
    }

    private void doPageIn(boolean force) throws Exception {
        List<QueueMessageReference> newlyPaged = doPageInForDispatch(force);
        pagedInPendingDispatchLock.writeLock().lock();
        try {
            if (pagedInPendingDispatch.isEmpty()) {
                pagedInPendingDispatch.addAll(newlyPaged);
            } else {
                for (QueueMessageReference qmr : newlyPaged) {
                    if (!pagedInPendingDispatch.contains(qmr)) {
                        pagedInPendingDispatch.add(qmr);
                    }
                }
            }
        } finally {
            pagedInPendingDispatchLock.writeLock().unlock();
        }
    }

    private List<QueueMessageReference> doPageInForDispatch(boolean force) throws Exception {
        List<QueueMessageReference> result = null;
        List<QueueMessageReference> resultList = null;

        int toPageIn = Math.min(getMaxPageSize(), messages.size());
        if (LOG.isDebugEnabled()) {
            LOG.debug(destination.getPhysicalName() + " toPageIn: " + toPageIn + ", Inflight: "
                    + destinationStatistics.getInflight().getCount() + ", pagedInMessages.size "
                    + pagedInMessages.size() + ", enqueueCount: " + destinationStatistics.getEnqueues().getCount()
                    + ", dequeueCount: " + destinationStatistics.getDequeues().getCount());
        }

        if (isLazyDispatch() && !force) {
            // Only page in the minimum number of messages which can be
            // dispatched immediately.
            toPageIn = Math.min(getConsumerMessageCountBeforeFull(), toPageIn);
        }
        int pagedInPendingSize = 0;
        pagedInPendingDispatchLock.readLock().lock();
        try {
            pagedInPendingSize = pagedInPendingDispatch.size();
        } finally {
            pagedInPendingDispatchLock.readLock().unlock();
        }
        if (toPageIn > 0 && (force || (!consumers.isEmpty() && pagedInPendingSize < getMaxPageSize()))) {
            int count = 0;
            result = new ArrayList<QueueMessageReference>(toPageIn);
            messagesLock.writeLock().lock();
            try {
                try {
                    messages.setMaxBatchSize(toPageIn);
                    messages.reset();
                    while (messages.hasNext() && count < toPageIn) {
                        MessageReference node = messages.next();
                        messages.remove();

                        QueueMessageReference ref = createMessageReference(node.getMessage());
                        if (ref.isExpired()) {
                            if (broker.isExpired(ref)) {
                                messageExpired(createConnectionContext(), ref);
                            } else {
                                ref.decrementReferenceCount();
                            }
                        } else {
                            result.add(ref);
                            count++;
                        }
                    }
                } finally {
                    messages.release();
                }
            } finally {
                messagesLock.writeLock().unlock();
            }
            // Only add new messages, not already pagedIn to avoid multiple
            // dispatch attempts
            pagedInMessagesLock.writeLock().lock();
            try {
                resultList = new ArrayList<QueueMessageReference>(result.size());
                for (QueueMessageReference ref : result) {
                    if (!pagedInMessages.containsKey(ref.getMessageId())) {
                        pagedInMessages.put(ref.getMessageId(), ref);
                        resultList.add(ref);
                    } else {
                        ref.decrementReferenceCount();
                    }
                }
            } finally {
                pagedInMessagesLock.writeLock().unlock();
            }
        } else {
            // Avoid return null list, if condition is not validated
            resultList = new ArrayList<QueueMessageReference>();
        }

        return resultList;
    }

    private void doDispatch(List<QueueMessageReference> list) throws Exception {
        boolean doWakeUp = false;

        pagedInPendingDispatchLock.writeLock().lock();
        try {
            if (!redeliveredWaitingDispatch.isEmpty()) {
                // Try first to dispatch redelivered messages to keep an
                // proper order
                redeliveredWaitingDispatch = doActualDispatch(redeliveredWaitingDispatch);
            }
            if (!pagedInPendingDispatch.isEmpty()) {
                // Next dispatch anything that had not been
                // dispatched before.
                pagedInPendingDispatch = doActualDispatch(pagedInPendingDispatch);
            }
            // and now see if we can dispatch the new stuff.. and append to
            // the pending
            // list anything that does not actually get dispatched.
            if (list != null && !list.isEmpty()) {
                if (pagedInPendingDispatch.isEmpty()) {
                    pagedInPendingDispatch.addAll(doActualDispatch(list));
                } else {
                    for (QueueMessageReference qmr : list) {
                        if (!pagedInPendingDispatch.contains(qmr)) {
                            pagedInPendingDispatch.add(qmr);
                        }
                    }
                    doWakeUp = true;
                }
            }
        } finally {
            pagedInPendingDispatchLock.writeLock().unlock();
        }

        if (doWakeUp) {
            // avoid lock order contention
            asyncWakeup();
        }
    }

    /**
     * @return list of messages that could get dispatched to consumers if they
     *         were not full.
     */
    private List<QueueMessageReference> doActualDispatch(List<QueueMessageReference> list) throws Exception {
        List<Subscription> consumers;
        consumersLock.writeLock().lock();
        try {
            if (this.consumers.isEmpty() || isSlave()) {
                // slave dispatch happens in processDispatchNotification
                return list;
            }
            consumers = new ArrayList<Subscription>(this.consumers);
        }finally {
            consumersLock.writeLock().unlock();
        }

        List<QueueMessageReference> rc = new ArrayList<QueueMessageReference>(list.size());
        Set<Subscription> fullConsumers = new HashSet<Subscription>(this.consumers.size());

        for (MessageReference node : list) {
            Subscription target = null;
            int interestCount = 0;
            for (Subscription s : consumers) {
                if (s instanceof QueueBrowserSubscription) {
                    interestCount++;
                    continue;
                }
                if (dispatchSelector.canSelect(s, node)) {
                    if (!fullConsumers.contains(s)) {
                        if (!s.isFull()) {
                            if (assignMessageGroup(s, (QueueMessageReference)node)) {
                                // Dispatch it.
                                s.add(node);
                                target = s;
                                break;
                            }
                        } else {
                            // no further dispatch of list to a full consumer to
                            // avoid out of order message receipt
                            fullConsumers.add(s);
                        }
                    }
                    interestCount++;
                } else {
                    // makes sure it gets dispatched again
                    if (!node.isDropped() && !((QueueMessageReference) node).isAcked()
                            && (!node.isDropped() || s.getConsumerInfo().isBrowser())) {
                        interestCount++;
                    }
                }
            }

            if ((target == null && interestCount > 0) || consumers.size() == 0) {
                // This means all subs were full or that there are no
                // consumers...
                rc.add((QueueMessageReference) node);
            }

            // If it got dispatched, rotate the consumer list to get round robin
            // distribution.
            if (target != null && !strictOrderDispatch && consumers.size() > 1
                    && !dispatchSelector.isExclusiveConsumer(target)) {
                consumersLock.writeLock().lock();
                try {
                    if (removeFromConsumerList(target)) {
                        addToConsumerList(target);
                        consumers = new ArrayList<Subscription>(this.consumers);
                    }
                }finally {
                    consumersLock.writeLock().unlock();
                }
            }
        }

        return rc;
    }

    protected boolean assignMessageGroup(Subscription subscription, QueueMessageReference node) throws Exception {
        //QueueMessageReference node = (QueueMessageReference) m;
        boolean result = true;
        // Keep message groups together.
        String groupId = node.getGroupID();
        int sequence = node.getGroupSequence();
        if (groupId != null) {
            //MessageGroupMap messageGroupOwners = ((Queue) node
            //        .getRegionDestination()).getMessageGroupOwners();

            MessageGroupMap messageGroupOwners = getMessageGroupOwners();
            // If we can own the first, then no-one else should own the
            // rest.
            if (sequence == 1) {
                assignGroup(subscription, messageGroupOwners, node, groupId);
            } else {

                // Make sure that the previous owner is still valid, we may
                // need to become the new owner.
                ConsumerId groupOwner;

                groupOwner = messageGroupOwners.get(groupId);
                if (groupOwner == null) {
                    assignGroup(subscription, messageGroupOwners, node, groupId);
                } else {
                    if (groupOwner.equals(subscription.getConsumerInfo().getConsumerId())) {
                        // A group sequence < 1 is an end of group signal.
                        if (sequence < 0) {
                            messageGroupOwners.removeGroup(groupId);
                        }
                    } else {
                        result = false;
                    }
                }
            }
        }

        return result;

    }

    protected void assignGroup(Subscription subs, MessageGroupMap messageGroupOwners, MessageReference n, String groupId) throws IOException {
        messageGroupOwners.put(groupId, subs.getConsumerInfo().getConsumerId());
        Message message = n.getMessage();
        if (message instanceof ActiveMQMessage) {
            ActiveMQMessage activeMessage = (ActiveMQMessage) message;
            try {
                activeMessage.setBooleanProperty("JMSXGroupFirstForConsumer", true, false);
            } catch (JMSException e) {
                LOG.warn("Failed to set boolean header: " + e, e);
            }
        }
    }

    protected void pageInMessages(boolean force) throws Exception {
        doDispatch(doPageInForDispatch(force));
    }

    private void addToConsumerList(Subscription sub) {
        if (useConsumerPriority) {
            consumers.add(sub);
            Collections.sort(consumers, orderedCompare);
        } else {
            consumers.add(sub);
        }
    }

    private boolean removeFromConsumerList(Subscription sub) {
        return consumers.remove(sub);
    }

    private int getConsumerMessageCountBeforeFull() throws Exception {
        int total = 0;
        boolean zeroPrefetch = false;
        consumersLock.readLock().lock();
        try{
            for (Subscription s : consumers) {
                zeroPrefetch |= s.getPrefetchSize() == 0;
                int countBeforeFull = s.countBeforeFull();
                total += countBeforeFull;
            }
        }finally {
            consumersLock.readLock().unlock();
        }
        if (total == 0 && zeroPrefetch) {
            total = 1;
        }
        return total;
    }

    /*
     * In slave mode, dispatch is ignored till we get this notification as the
     * dispatch process is non deterministic between master and slave. On a
     * notification, the actual dispatch to the subscription (as chosen by the
     * master) is completed. (non-Javadoc)
     * @see
     * org.apache.activemq.broker.region.BaseDestination#processDispatchNotification
     * (org.apache.activemq.command.MessageDispatchNotification)
     */
    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        // do dispatch
        Subscription sub = getMatchingSubscription(messageDispatchNotification);
        if (sub != null) {
            MessageReference message = getMatchingMessage(messageDispatchNotification);
            sub.add(message);
            sub.processMessageDispatchNotification(messageDispatchNotification);
        }
    }

    private QueueMessageReference getMatchingMessage(MessageDispatchNotification messageDispatchNotification)
            throws Exception {
        QueueMessageReference message = null;
        MessageId messageId = messageDispatchNotification.getMessageId();

        pagedInPendingDispatchLock.writeLock().lock();
        try {
            for (QueueMessageReference ref : pagedInPendingDispatch) {
                if (messageId.equals(ref.getMessageId())) {
                    message = ref;
                    pagedInPendingDispatch.remove(ref);
                    break;
                }
            }
        } finally {
            pagedInPendingDispatchLock.writeLock().unlock();
        }

        if (message == null) {
            pagedInMessagesLock.readLock().lock();
            try {
                message = pagedInMessages.get(messageId);
            } finally {
                pagedInMessagesLock.readLock().unlock();
            }
        }

        if (message == null) {
            messagesLock.writeLock().lock();
            try {
                try {
                    messages.setMaxBatchSize(getMaxPageSize());
                    messages.reset();
                    while (messages.hasNext()) {
                        MessageReference node = messages.next();
                        messages.remove();
                        if (messageId.equals(node.getMessageId())) {
                            message = this.createMessageReference(node.getMessage());
                            break;
                        }
                    }
                } finally {
                    messages.release();
                }
            } finally {
                messagesLock.writeLock().unlock();
            }
        }

        if (message == null) {
            Message msg = loadMessage(messageId);
            if (msg != null) {
                message = this.createMessageReference(msg);
            }
        }

        if (message == null) {
            throw new JMSException("Slave broker out of sync with master - Message: "
                    + messageDispatchNotification.getMessageId() + " on "
                    + messageDispatchNotification.getDestination() + " does not exist among pending("
                    + pagedInPendingDispatch.size() + ") for subscription: "
                    + messageDispatchNotification.getConsumerId());
        }
        return message;
    }

    /**
     * Find a consumer that matches the id in the message dispatch notification
     *
     * @param messageDispatchNotification
     * @return sub or null if the subscription has been removed before dispatch
     * @throws JMSException
     */
    private Subscription getMatchingSubscription(MessageDispatchNotification messageDispatchNotification)
            throws JMSException {
        Subscription sub = null;
        consumersLock.readLock().lock();
        try {
            for (Subscription s : consumers) {
                if (messageDispatchNotification.getConsumerId().equals(s.getConsumerInfo().getConsumerId())) {
                    sub = s;
                    break;
                }
            }
        }finally {
            consumersLock.readLock().unlock();
        }
        return sub;
    }

    public void onUsageChanged(Usage usage, int oldPercentUsage, int newPercentUsage) {
        if (oldPercentUsage > newPercentUsage) {
            asyncWakeup();
        }
    }

    @Override
    protected Logger getLog() {
        return LOG;
    }
}
