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

import static org.apache.activemq.broker.region.cursors.AbstractStoreCursor.gotToTheStore;
import static org.apache.activemq.transaction.Transaction.IN_USE_STATE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.ResourceAllocationException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerStoppedException;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.cursors.OrderedPendingList;
import org.apache.activemq.broker.region.cursors.PendingList;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.PrioritizedPendingList;
import org.apache.activemq.broker.region.cursors.QueueDispatchPendingList;
import org.apache.activemq.broker.region.cursors.StoreQueueCursor;
import org.apache.activemq.broker.region.cursors.VMPendingMessageCursor;
import org.apache.activemq.broker.region.group.CachedMessageGroupMapFactory;
import org.apache.activemq.broker.region.group.MessageGroupMap;
import org.apache.activemq.broker.region.group.MessageGroupMapFactory;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.DispatchPolicy;
import org.apache.activemq.broker.region.policy.RoundRobinDispatchPolicy;
import org.apache.activemq.broker.util.InsertionCountList;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.store.IndexListener;
import org.apache.activemq.store.ListenableFuture;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.usage.UsageListener;
import org.apache.activemq.util.BrokerSupport;
import org.apache.activemq.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * The Queue is a List of MessageEntry objects that are dispatched to matching
 * subscriptions.
 */
public class Queue extends BaseDestination implements Task, UsageListener, IndexListener {
    protected static final Logger LOG = LoggerFactory.getLogger(Queue.class);
    protected final TaskRunnerFactory taskFactory;
    protected TaskRunner taskRunner;
    private final ReentrantReadWriteLock consumersLock = new ReentrantReadWriteLock();
    protected final List<Subscription> consumers = new ArrayList<Subscription>(50);
    private final ReentrantReadWriteLock messagesLock = new ReentrantReadWriteLock();
    protected PendingMessageCursor messages;
    private final ReentrantReadWriteLock pagedInMessagesLock = new ReentrantReadWriteLock();
    private final PendingList pagedInMessages = new OrderedPendingList();
    // Messages that are paged in but have not yet been targeted at a subscription
    private final ReentrantReadWriteLock pagedInPendingDispatchLock = new ReentrantReadWriteLock();
    protected QueueDispatchPendingList dispatchPendingList = new QueueDispatchPendingList();
    private AtomicInteger pendingSends = new AtomicInteger(0);
    private MessageGroupMap messageGroupOwners;
    private DispatchPolicy dispatchPolicy = new RoundRobinDispatchPolicy();
    private MessageGroupMapFactory messageGroupMapFactory = new CachedMessageGroupMapFactory();
    final Lock sendLock = new ReentrantLock();
    private ExecutorService executor;
    private final Map<MessageId, Runnable> messagesWaitingForSpace = new LinkedHashMap<MessageId, Runnable>();
    private boolean useConsumerPriority = true;
    private boolean strictOrderDispatch = false;
    private final QueueDispatchSelector dispatchSelector;
    private boolean optimizedDispatch = false;
    private boolean iterationRunning = false;
    private boolean firstConsumer = false;
    private int timeBeforeDispatchStarts = 0;
    private int consumersBeforeDispatchStarts = 0;
    private CountDownLatch consumersBeforeStartsLatch;
    private final AtomicLong pendingWakeups = new AtomicLong();
    private boolean allConsumersExclusiveByDefault = false;

    private volatile boolean resetNeeded;

    private final Runnable sendMessagesWaitingForSpaceTask = new Runnable() {
        @Override
        public void run() {
            asyncWakeup();
        }
    };
    private final AtomicBoolean expiryTaskInProgress = new AtomicBoolean(false);
    private final Runnable expireMessagesWork = new Runnable() {
        @Override
        public void run() {
            expireMessages();
            expiryTaskInProgress.set(false);
        }
    };

    private final Runnable expireMessagesTask = new Runnable() {
        @Override
        public void run() {
            if (expiryTaskInProgress.compareAndSet(false, true)) {
                taskFactory.execute(expireMessagesWork);
            }
        }
    };

    private final Object iteratingMutex = new Object();

    // gate on enabling cursor cache to ensure no outstanding sync
    // send before async sends resume
    public boolean singlePendingSend() {
        return pendingSends.get() <= 1;
    }

    class TimeoutMessage implements Delayed {

        Message message;
        ConnectionContext context;
        long trigger;

        public TimeoutMessage(Message message, ConnectionContext context, long delay) {
            this.message = message;
            this.context = context;
            this.trigger = System.currentTimeMillis() + delay;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long n = trigger - System.currentTimeMillis();
            return unit.convert(n, TimeUnit.MILLISECONDS);
        }

        @Override
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
                                                "Usage Manager Memory Limit Wait Timeout. Stopping producer ("
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
                LOG.debug("{} Producer Flow Control Timeout Task is stopping", getName());
            }
        }
    }

    private final FlowControlTimeoutTask flowControlTimeoutTask = new FlowControlTimeoutTask();

    private final Comparator<Subscription> orderedCompare = new Comparator<Subscription>() {

        @Override
        public int compare(Subscription s1, Subscription s2) {
            // We want the list sorted in descending order
            int val = s2.getConsumerInfo().getPriority() - s1.getConsumerInfo().getPriority();
            if (val == 0 && messageGroupOwners != null) {
                // then ascending order of assigned message groups to favour less loaded consumers
                // Long.compare in jdk7
                long x = s1.getConsumerInfo().getAssignedGroupCount(destination);
                long y = s2.getConsumerInfo().getAssignedGroupCount(destination);
                val = (x < y) ? -1 : ((x == y) ? 0 : 1);
            }
            return val;
        }
    };

    public Queue(BrokerService brokerService, final ActiveMQDestination destination, MessageStore store,
            DestinationStatistics parentStats, TaskRunnerFactory taskFactory) throws Exception {
        super(brokerService, store, destination, parentStats);
        this.taskFactory = taskFactory;
        this.dispatchSelector = new QueueDispatchSelector(destination);
        if (store != null) {
            store.registerIndexListener(this);
        }
    }

    @Override
    public List<Subscription> getConsumers() {
        consumersLock.readLock().lock();
        try {
            return new ArrayList<Subscription>(consumers);
        } finally {
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

    class BatchMessageRecoveryListener implements MessageRecoveryListener {
        final LinkedList<Message> toExpire = new LinkedList<Message>();
        final double totalMessageCount;
        int recoveredAccumulator = 0;
        int currentBatchCount;

        BatchMessageRecoveryListener(int totalMessageCount) {
            this.totalMessageCount = totalMessageCount;
            currentBatchCount = recoveredAccumulator;
        }

        @Override
        public boolean recoverMessage(Message message) {
            recoveredAccumulator++;
            if ((recoveredAccumulator % 10000) == 0) {
                LOG.info("cursor for {} has recovered {} messages. {}% complete",
                        getActiveMQDestination().getQualifiedName(), recoveredAccumulator,
                        Integer.valueOf((int) (recoveredAccumulator * 100 / totalMessageCount)));
            }
            // Message could have expired while it was being
            // loaded..
            message.setRegionDestination(Queue.this);
            if (message.isExpired() && broker.isExpired(message)) {
                toExpire.add(message);
                return true;
            }
            if (hasSpace()) {
                messagesLock.writeLock().lock();
                try {
                    try {
                        messages.addMessageLast(message);
                    } catch (Exception e) {
                        LOG.error("Failed to add message to cursor", e);
                    }
                } finally {
                    messagesLock.writeLock().unlock();
                }
                destinationStatistics.getMessages().increment();
                return true;
            }
            return false;
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

        public void reset() {
            currentBatchCount = recoveredAccumulator;
        }

        public void processExpired() {
            for (Message message: toExpire) {
                messageExpired(createConnectionContext(), createMessageReference(message));
                // drop message will decrement so counter
                // balance here
                destinationStatistics.getMessages().increment();
            }
            toExpire.clear();
        }

        public boolean done() {
            return currentBatchCount == recoveredAccumulator;
        }
    }

    @Override
    public void setPrioritizedMessages(boolean prioritizedMessages) {
        super.setPrioritizedMessages(prioritizedMessages);
        dispatchPendingList.setPrioritizedMessages(prioritizedMessages);
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
            store.start();
            final int messageCount = store.getMessageCount();
            if (messageCount > 0 && messages.isRecoveryRequired()) {
                BatchMessageRecoveryListener listener = new BatchMessageRecoveryListener(messageCount);
                do {
                   listener.reset();
                   store.recoverNextMessages(getMaxPageSize(), listener);
                   listener.processExpired();
               } while (!listener.done());
            } else {
                destinationStatistics.getMessages().add(messageCount);
            }
        }
    }

    ConcurrentLinkedQueue<QueueBrowserSubscription> browserSubscriptions = new ConcurrentLinkedQueue<>();

    @Override
    public void addSubscription(ConnectionContext context, Subscription sub) throws Exception {
        LOG.debug("{} add sub: {}, dequeues: {}, dispatched: {}, inflight: {}",
                getActiveMQDestination().getQualifiedName(),
                sub,
                getDestinationStatistics().getDequeues().getCount(),
                getDestinationStatistics().getDispatched().getCount(),
                getDestinationStatistics().getInflight().getCount());

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
            } finally {
                consumersLock.writeLock().unlock();
            }

            if (sub instanceof QueueBrowserSubscription) {
                // tee up for dispatch in next iterate
                QueueBrowserSubscription browserSubscription = (QueueBrowserSubscription) sub;
                browserSubscription.incrementQueueRef();
                browserSubscriptions.add(browserSubscription);
            }

            if (!this.optimizedDispatch) {
                wakeup();
            }
        } finally {
            pagedInPendingDispatchLock.writeLock().unlock();
        }
        if (this.optimizedDispatch) {
            // Outside of dispatchLock() to maintain the lock hierarchy of
            // iteratingMutex -> dispatchLock. - see
            // https://issues.apache.org/activemq/browse/AMQ-1878
            wakeup();
        }
    }

    @Override
    public void removeSubscription(ConnectionContext context, Subscription sub, long lastDeliveredSequenceId)
            throws Exception {
        super.removeSubscription(context, sub, lastDeliveredSequenceId);
        // synchronize with dispatch method so that no new messages are sent
        // while removing up a subscription.
        pagedInPendingDispatchLock.writeLock().lock();
        try {
            LOG.debug("{} remove sub: {}, lastDeliveredSeqId: {}, dequeues: {}, dispatched: {}, inflight: {}, groups: {}", new Object[]{
                    getActiveMQDestination().getQualifiedName(),
                    sub,
                    lastDeliveredSequenceId,
                    getDestinationStatistics().getDequeues().getCount(),
                    getDestinationStatistics().getDispatched().getCount(),
                    getDestinationStatistics().getInflight().getCount(),
                    sub.getConsumerInfo().getAssignedGroupCount(destination)
            });
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
                if (lastDeliveredSequenceId > RemoveInfo.LAST_DELIVERED_UNSET) {
                    for (MessageReference ref : unAckedMessages) {
                        if (ref.getMessageId().getBrokerSequenceId() == lastDeliveredSequenceId) {
                            lastDeliveredRef = ref;
                            markAsRedelivered = true;
                            LOG.debug("found lastDeliveredSeqID: {}, message reference: {}", lastDeliveredSequenceId, ref.getMessageId());
                            break;
                        }
                    }
                }

                for (Iterator<MessageReference> unackedListIterator = unAckedMessages.iterator(); unackedListIterator.hasNext(); ) {
                    MessageReference ref = unackedListIterator.next();
                    // AMQ-5107: don't resend if the broker is shutting down
                    if ( this.brokerService.isStopping() ) {
                        break;
                    }
                    QueueMessageReference qmr = (QueueMessageReference) ref;
                    if (qmr.getLockOwner() == sub) {
                        qmr.unlock();

                        // have no delivery information
                        if (lastDeliveredSequenceId == RemoveInfo.LAST_DELIVERED_UNKNOWN) {
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
                    if (qmr.isDropped()) {
                        unackedListIterator.remove();
                    }
                }
                dispatchPendingList.addForRedelivery(unAckedMessages, strictOrderDispatch && consumers.isEmpty());
                if (sub instanceof QueueBrowserSubscription) {
                    ((QueueBrowserSubscription)sub).decrementQueueRef();
                    browserSubscriptions.remove(sub);
                }
                // AMQ-5107: don't resend if the broker is shutting down
                if (dispatchPendingList.hasRedeliveries() && (! this.brokerService.isStopping())) {
                    doDispatch(new OrderedPendingList());
                }
            } finally {
                consumersLock.writeLock().unlock();
            }
            if (!this.optimizedDispatch) {
                wakeup();
            }
        } finally {
            pagedInPendingDispatchLock.writeLock().unlock();
        }
        if (this.optimizedDispatch) {
            // Outside of dispatchLock() to maintain the lock hierarchy of
            // iteratingMutex -> dispatchLock. - see
            // https://issues.apache.org/activemq/browse/AMQ-1878
            wakeup();
        }
    }

    private volatile ResourceAllocationException sendMemAllocationException = null;
    @Override
    public void send(final ProducerBrokerExchange producerExchange, final Message message) throws Exception {
        final ConnectionContext context = producerExchange.getConnectionContext();
        // There is delay between the client sending it and it arriving at the
        // destination.. it may have expired.
        message.setRegionDestination(this);
        ProducerState state = producerExchange.getProducerState();
        if (state == null) {
            LOG.warn("Send failed for: {}, missing producer state for: {}", message, producerExchange);
            throw new JMSException("Cannot send message to " + getActiveMQDestination() + " with invalid (null) producer state");
        }
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
                if (isFlowControlLogRequired()) {
                    LOG.warn("Usage Manager Memory Limit ({}) reached on {}, size {}. Producers will be throttled to the rate at which messages are removed from this destination to prevent flooding it. See http://activemq.apache.org/producer-flow-control.html for more info.",
                                memoryUsage.getLimit(), getActiveMQDestination().getQualifiedName(), destinationStatistics.getMessages().getCount());
                } else {
                    LOG.debug("Usage Manager Memory Limit ({}) reached on {}, size {}. Producers will be throttled to the rate at which messages are removed from this destination to prevent flooding it. See http://activemq.apache.org/producer-flow-control.html for more info.",
                            memoryUsage.getLimit(), getActiveMQDestination().getQualifiedName(), destinationStatistics.getMessages().getCount());
                }
                if (!context.isNetworkConnection() && systemUsage.isSendFailIfNoSpace()) {
                    ResourceAllocationException resourceAllocationException = sendMemAllocationException;
                    if (resourceAllocationException == null) {
                        synchronized (this) {
                            resourceAllocationException = sendMemAllocationException;
                            if (resourceAllocationException == null) {
                                sendMemAllocationException = resourceAllocationException = new ResourceAllocationException("Usage Manager Memory Limit reached on "
                                        + getActiveMQDestination().getQualifiedName() + "."
                                        + " See http://activemq.apache.org/producer-flow-control.html for more info");
                            }
                        }
                    }
                    throw resourceAllocationException;
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
                            @Override
                            public void run() {

                                try {
                                    // While waiting for space to free up... the
                                    // transaction may be done
                                    if (message.isInTransaction()) {
                                        if (context.getTransaction() == null || context.getTransaction().getState() > IN_USE_STATE) {
                                            throw new JMSException("Send transaction completed while waiting for space");
                                        }
                                    }

                                    // the message may have expired.
                                    if (message.isExpired()) {
                                        LOG.error("message expired waiting for space");
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
                                    if (!sendProducerAck && !context.isInRecoveryMode() && !brokerService.isStopping()) {
                                        ExceptionResponse response = new ExceptionResponse(e);
                                        response.setCorrelationId(message.getCommandId());
                                        context.getConnection().dispatchAsync(response);
                                    } else {
                                        LOG.debug("unexpected exception on deferred send of: {}", message, e);
                                    }
                                } finally {
                                    getDestinationStatistics().getBlockedSends().decrement();
                                    producerExchangeCopy.blockingOnFlowControl(false);
                                }
                            }
                        });

                        getDestinationStatistics().getBlockedSends().increment();
                        producerExchange.blockingOnFlowControl(true);
                        if (!context.isNetworkConnection() && systemUsage.getSendFailIfNoSpaceAfterTimeout() != 0) {
                            flowControlTimeoutMessages.add(new TimeoutMessage(message, context, systemUsage
                                    .getSendFailIfNoSpaceAfterTimeout()));
                        }

                        registerCallbackForNotFullNotification();
                        context.setDontSendReponse(true);
                        return;
                    }

                } else {

                    if (memoryUsage.isFull()) {
                        waitForSpace(context, producerExchange, memoryUsage, "Usage Manager Memory Limit reached. Producer ("
                                + message.getProducerId() + ") stopped to prevent flooding "
                                + getActiveMQDestination().getQualifiedName() + "."
                                + " See http://activemq.apache.org/producer-flow-control.html for more info");
                    }

                    // The usage manager could have delayed us by the time
                    // we unblock the message could have expired..
                    if (message.isExpired()) {
                        LOG.debug("Expired message: {}", message);
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

    private final LinkedList<MessageContext> indexOrderedCursorUpdates = new LinkedList<>();

    @Override
    public void onAdd(MessageContext messageContext) {
        synchronized (indexOrderedCursorUpdates) {
            indexOrderedCursorUpdates.addLast(messageContext);
        }
    }

    public void rollbackPendingCursorAdditions(MessageId messageId) {
        synchronized (indexOrderedCursorUpdates) {
            for (int i = indexOrderedCursorUpdates.size() - 1; i >= 0; i--) {
                MessageContext mc = indexOrderedCursorUpdates.get(i);
                if (mc.message.getMessageId().equals(messageId)) {
                    indexOrderedCursorUpdates.remove(mc);
                    if (mc.onCompletion != null) {
                        mc.onCompletion.run();
                    }
                    break;
                }
            }
        }
    }

    private void doPendingCursorAdditions() throws Exception {
        LinkedList<MessageContext> orderedUpdates = new LinkedList<>();
        sendLock.lockInterruptibly();
        try {
            synchronized (indexOrderedCursorUpdates) {
                MessageContext candidate = indexOrderedCursorUpdates.peek();
                while (candidate != null && candidate.message.getMessageId().getFutureOrSequenceLong() != null) {
                    candidate = indexOrderedCursorUpdates.removeFirst();
                    // check for duplicate adds suppressed by the store
                    if (candidate.message.getMessageId().getFutureOrSequenceLong() instanceof Long && ((Long)candidate.message.getMessageId().getFutureOrSequenceLong()).compareTo(-1l) == 0) {
                        LOG.warn("{} messageStore indicated duplicate add attempt for {}, suppressing duplicate dispatch", this, candidate.message.getMessageId());
                    } else {
                        orderedUpdates.add(candidate);
                    }
                    candidate = indexOrderedCursorUpdates.peek();
                }
            }
            messagesLock.writeLock().lock();
            try {
                for (MessageContext messageContext : orderedUpdates) {
                    if (!messages.addMessageLast(messageContext.message)) {
                        // cursor suppressed a duplicate
                        messageContext.duplicate = true;
                    }
                    if (messageContext.onCompletion != null) {
                        messageContext.onCompletion.run();
                    }
                }
            } finally {
                messagesLock.writeLock().unlock();
            }
        } finally {
            sendLock.unlock();
        }
        for (MessageContext messageContext : orderedUpdates) {
            if (!messageContext.duplicate) {
                messageSent(messageContext.context, messageContext.message);
            }
        }
        orderedUpdates.clear();
    }

    final class CursorAddSync extends Synchronization {

        private final MessageContext messageContext;

        CursorAddSync(MessageContext messageContext) {
            this.messageContext = messageContext;
            this.messageContext.message.incrementReferenceCount();
        }

        @Override
        public void afterCommit() throws Exception {
            if (store != null && messageContext.message.isPersistent()) {
                doPendingCursorAdditions();
            } else {
                cursorAdd(messageContext.message);
                messageSent(messageContext.context, messageContext.message);
            }
            messageContext.message.decrementReferenceCount();
        }

        @Override
        public void afterRollback() throws Exception {
            if (store != null && messageContext.message.isPersistent()) {
                rollbackPendingCursorAdditions(messageContext.message.getMessageId());
            }
            messageContext.message.decrementReferenceCount();
        }
    }

    void doMessageSend(final ProducerBrokerExchange producerExchange, final Message message) throws IOException,
            Exception {
        final ConnectionContext context = producerExchange.getConnectionContext();
        ListenableFuture<Object> result = null;

        producerExchange.incrementSend();
        pendingSends.incrementAndGet();
        do {
            checkUsage(context, producerExchange, message);
            message.getMessageId().setBrokerSequenceId(getDestinationSequenceId());
            if (store != null && message.isPersistent()) {
                message.getMessageId().setFutureOrSequenceLong(null);
                try {
                    //AMQ-6133 - don't store async if using persistJMSRedelivered
                    //This flag causes a sync update later on dispatch which can cause a race
                    //condition if the original add is processed after the update, which can cause
                    //a duplicate message to be stored
                    if (messages.isCacheEnabled() && !isPersistJMSRedelivered()) {
                        result = store.asyncAddQueueMessage(context, message, isOptimizeStorage());
                        result.addListener(new PendingMarshalUsageTracker(message));
                    } else {
                        store.addMessage(context, message);
                    }
                } catch (Exception e) {
                    // we may have a store in inconsistent state, so reset the cursor
                    // before restarting normal broker operations
                    resetNeeded = true;
                    pendingSends.decrementAndGet();
                    rollbackPendingCursorAdditions(message.getMessageId());
                    throw e;
                }
            }

            //Clear the unmarshalled state if the message is marshalled
            //Persistent messages will always be marshalled but non-persistent may not be
            //Specially non-persistent messages over the VM transport won't be
            if (isReduceMemoryFootprint() && message.isMarshalled()) {
                message.clearUnMarshalledState();
            }
            if(tryOrderedCursorAdd(message, context)) {
                break;
            }
        } while (started.get());

        if (result != null && message.isResponseRequired() && !result.isCancelled()) {
            try {
                result.get();
            } catch (CancellationException e) {
                // ignore - the task has been cancelled if the message
                // has already been deleted
            }
        }
    }

    private boolean tryOrderedCursorAdd(Message message, ConnectionContext context) throws Exception {
        boolean result = true;

        if (context.isInTransaction()) {
            context.getTransaction().addSynchronization(new CursorAddSync(new MessageContext(context, message, null)));
        } else if (store != null && message.isPersistent()) {
            doPendingCursorAdditions();
        } else {
            // no ordering issue with non persistent messages
            result = tryCursorAdd(message);
            messageSent(context, message);
        }

        return result;
    }

    private void checkUsage(ConnectionContext context,ProducerBrokerExchange producerBrokerExchange, Message message) throws ResourceAllocationException, IOException, InterruptedException {
        if (message.isPersistent()) {
            if (store != null && systemUsage.getStoreUsage().isFull(getStoreUsageHighWaterMark())) {
                final String logMessage = "Persistent store is Full, " + getStoreUsageHighWaterMark() + "% of "
                    + systemUsage.getStoreUsage().getLimit() + ". Stopping producer ("
                    + message.getProducerId() + ") to prevent flooding "
                    + getActiveMQDestination().getQualifiedName() + "."
                    + " See http://activemq.apache.org/producer-flow-control.html for more info";

                waitForSpace(context, producerBrokerExchange, systemUsage.getStoreUsage(), getStoreUsageHighWaterMark(), logMessage);
            }
        } else if (messages.getSystemUsage() != null && systemUsage.getTempUsage().isFull()) {
            final String logMessage = "Temp Store is Full ("
                    + systemUsage.getTempUsage().getPercentUsage() + "% of " + systemUsage.getTempUsage().getLimit()
                    +"). Stopping producer (" + message.getProducerId()
                + ") to prevent flooding " + getActiveMQDestination().getQualifiedName() + "."
                + " See http://activemq.apache.org/producer-flow-control.html for more info";

            waitForSpace(context, producerBrokerExchange, messages.getSystemUsage().getTempUsage(), logMessage);
        }
    }

    private void expireMessages() {
        LOG.debug("{} expiring messages ..", getActiveMQDestination().getQualifiedName());

        // just track the insertion count
        List<Message> browsedMessages = new InsertionCountList<Message>();
        doBrowse(browsedMessages, this.getMaxExpirePageSize());
        asyncWakeup();
        LOG.debug("{} expiring messages done.", getActiveMQDestination().getQualifiedName());
    }

    @Override
    public void gc() {
    }

    @Override
    public void acknowledge(ConnectionContext context, Subscription sub, MessageAck ack, MessageReference node)
            throws IOException {
        messageConsumed(context, node);
        if (store != null && node.isPersistent()) {
            store.removeAsyncMessage(context, convertToNonRangedAck(ack, node));
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

    public long getPendingMessageSize() {
        messagesLock.readLock().lock();
        try{
            return messages.messageSize();
        } finally {
            messagesLock.readLock().unlock();
        }
    }

    public long getPendingMessageCount() {
         return this.destinationStatistics.getMessages().getCount();
    }

    @Override
    public String toString() {
        return destination.getQualifiedName() + ", subscriptions=" + consumers.size()
                + ", memory=" + memoryUsage.getPercentUsage() + "%, size=" + destinationStatistics.getMessages().getCount() + ", pending="
                + indexOrderedCursorUpdates.size();
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            if (memoryUsage != null) {
                memoryUsage.start();
            }
            if (systemUsage.getStoreUsage() != null) {
                systemUsage.getStoreUsage().start();
            }
            if (systemUsage.getTempUsage() != null) {
                systemUsage.getTempUsage().start();
            }
            systemUsage.getMemoryUsage().addUsageListener(this);
            messages.start();
            if (getExpireMessagesPeriod() > 0) {
                scheduler.executePeriodically(expireMessagesTask, getExpireMessagesPeriod());
            }
            doPageIn(false);
        }
    }

    @Override
    public void stop() throws Exception {
        if (started.compareAndSet(true, false)) {
            if (taskRunner != null) {
                taskRunner.shutdown();
            }
            if (this.executor != null) {
                ThreadPoolUtils.shutdownNow(executor);
                executor = null;
            }

            scheduler.cancel(expireMessagesTask);

            if (flowControlTimeoutTask.isAlive()) {
                flowControlTimeoutTask.interrupt();
            }

            if (messages != null) {
                messages.stop();
            }

            for (MessageReference messageReference : pagedInMessages.values()) {
                messageReference.decrementReferenceCount();
            }
            pagedInMessages.clear();

            systemUsage.getMemoryUsage().removeUsageListener(this);
            if (memoryUsage != null) {
                memoryUsage.stop();
            }
            if (systemUsage.getStoreUsage() != null) {
                systemUsage.getStoreUsage().stop();
            }
            if (this.systemUsage.getTempUsage() != null) {
                this.systemUsage.getTempUsage().stop();
            }
            if (store != null) {
                store.stop();
            }
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
            messageGroupOwners.setDestination(this);
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

    public boolean isResetNeeded() {
        return resetNeeded;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    private QueueMessageReference createMessageReference(Message message) {
        QueueMessageReference result = new IndirectMessageReference(message);
        return result;
    }

    @Override
    public Message[] browse() {
        List<Message> browseList = new ArrayList<Message>();
        doBrowse(browseList, getMaxBrowsePageSize());
        return browseList.toArray(new Message[browseList.size()]);
    }

    public void doBrowse(List<Message> browseList, int max) {
        final ConnectionContext connectionContext = createConnectionContext();
        try {
            int maxPageInAttempts = 1;
            if (max > 0) {
                messagesLock.readLock().lock();
                try {
                    maxPageInAttempts += (messages.size() / max);
                } finally {
                    messagesLock.readLock().unlock();
                }
                while (shouldPageInMoreForBrowse(max) && maxPageInAttempts-- > 0) {
                    pageInMessages(!memoryUsage.isFull(110), max);
                }
            }
            doBrowseList(browseList, max, dispatchPendingList, pagedInPendingDispatchLock, connectionContext, "redeliveredWaitingDispatch+pagedInPendingDispatch");
            doBrowseList(browseList, max, pagedInMessages, pagedInMessagesLock, connectionContext, "pagedInMessages");

            // we need a store iterator to walk messages on disk, independent of the cursor which is tracking
            // the next message batch
        } catch (BrokerStoppedException ignored) {
        } catch (Exception e) {
            LOG.error("Problem retrieving message for browse", e);
        }
    }

    protected void doBrowseList(List<Message> browseList, int max, PendingList list, ReentrantReadWriteLock lock, ConnectionContext connectionContext, String name) throws Exception {
        List<MessageReference> toExpire = new ArrayList<MessageReference>();
        lock.readLock().lock();
        try {
            addAll(list.values(), browseList, max, toExpire);
        } finally {
            lock.readLock().unlock();
        }
        for (MessageReference ref : toExpire) {
            if (broker.isExpired(ref)) {
                LOG.debug("expiring from {}: {}", name, ref);
                messageExpired(connectionContext, ref);
            } else {
                lock.writeLock().lock();
                try {
                    list.remove(ref);
                } finally {
                    lock.writeLock().unlock();
                }
                ref.decrementReferenceCount();
            }
        }
    }

    private boolean shouldPageInMoreForBrowse(int max) {
        int alreadyPagedIn = 0;
        pagedInMessagesLock.readLock().lock();
        try {
            alreadyPagedIn = pagedInMessages.size();
        } finally {
            pagedInMessagesLock.readLock().unlock();
        }
        int messagesInQueue = alreadyPagedIn;
        messagesLock.readLock().lock();
        try {
            messagesInQueue += messages.size();
        } finally {
            messagesLock.readLock().unlock();
        }

        LOG.trace("max {}, alreadyPagedIn {}, messagesCount {}, memoryUsage {}%",
                max, alreadyPagedIn, messagesInQueue, memoryUsage.getPercentUsage());
        return (alreadyPagedIn == 0 || (alreadyPagedIn < max)
                && (alreadyPagedIn < messagesInQueue)
                && messages.hasSpace());
    }

    private void addAll(Collection<? extends MessageReference> refs, List<Message> l, int max,
            List<MessageReference> toExpire) throws Exception {
        for (Iterator<? extends MessageReference> i = refs.iterator(); i.hasNext() && l.size() < max;) {
            QueueMessageReference ref = (QueueMessageReference) i.next();
            if (ref.isExpired() && (ref.getLockOwner() == null)) {
                toExpire.add(ref);
            } else if (!ref.isAcked() && l.contains(ref.getMessage()) == false) {
                l.add(ref.getMessage());
            }
        }
    }

    public QueueMessageReference getMessage(String id) {
        MessageId msgId = new MessageId(id);
        pagedInMessagesLock.readLock().lock();
        try {
            QueueMessageReference ref = (QueueMessageReference)this.pagedInMessages.get(msgId);
            if (ref != null) {
                return ref;
            }
        } finally {
            pagedInMessagesLock.readLock().unlock();
        }
        messagesLock.writeLock().lock();
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
            messagesLock.writeLock().unlock();
        }
        return null;
    }

    public void purge() throws Exception {
        ConnectionContext c = createConnectionContext();
        List<MessageReference> list = null;
        try {
            sendLock.lock();
            long originalMessageCount = this.destinationStatistics.getMessages().getCount();
            do {
                doPageIn(true, false, getMaxPageSize());  // signal no expiry processing needed.
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
                        messages.rollback(r.getMessageId());
                    } catch (IOException e) {
                    }
                }
                // don't spin/hang if stats are out and there is nothing left in the
                // store
            } while (!list.isEmpty() && this.destinationStatistics.getMessages().getCount() > 0);

            if (this.destinationStatistics.getMessages().getCount() > 0) {
                LOG.warn("{} after purge of {} messages, message count stats report: {}", getActiveMQDestination().getQualifiedName(), originalMessageCount, this.destinationStatistics.getMessages().getCount());
            }
        } finally {
            sendLock.unlock();
        }
    }

    @Override
    public void clearPendingMessages(int pendingAdditionsCount) {
        messagesLock.writeLock().lock();
        try {
            final ActiveMQMessage dummyPersistent = new ActiveMQMessage();
            dummyPersistent.setPersistent(true);
            for (int i=0; i<pendingAdditionsCount; i++) {
                try {
                    // track the increase in the cursor size w/o reverting to the store
                    messages.addMessageFirst(dummyPersistent);
                } catch (Exception ignored) {
                    LOG.debug("Unexpected exception on tracking pending message additions", ignored);
                }
            }
            if (resetNeeded) {
                messages.gc();
                messages.reset();
                resetNeeded = false;
            } else {
                messages.rebase();
            }
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
        Set<MessageReference> set = new LinkedHashSet<MessageReference>();
        ConnectionContext context = createConnectionContext();
        do {
            doPageIn(true);
            pagedInMessagesLock.readLock().lock();
            try {
                if (!set.addAll(pagedInMessages.values())) {
                    // nothing new to check - mem constraint on page in
                    return movedCounter;
                };
            } finally {
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

        if (destination.equals(dest)) {
            return 0;
        }

        int movedCounter = 0;
        int count = 0;
        Set<MessageReference> set = new LinkedHashSet<MessageReference>();
        do {
            doPageIn(true, false, (messages.isCacheEnabled() || !broker.getBrokerService().isPersistent()) ? messages.size() : getMaxBrowsePageSize());
            pagedInMessagesLock.readLock().lock();
            try {
                if (!set.addAll(pagedInMessages.values())) {
                    // nothing new to check - mem constraint on page in
                    return movedCounter;
                }
            } finally {
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
        Set<Destination> destsToPause = regionBroker.getDestinations(dest);
        try {
            for (Destination d: destsToPause) {
                if (d instanceof Queue) {
                    ((Queue)d).pauseDispatch();
                }
            }
            BrokerSupport.resend(context, m.getMessage(), dest);
            removeMessage(context, m);
            messagesLock.writeLock().lock();
            try {
                messages.rollback(m.getMessageId());
                if (isDLQ()) {
                    ActiveMQDestination originalDestination = m.getMessage().getOriginalDestination();
                    if (originalDestination != null) {
                        for (Destination destination : regionBroker.getDestinations(originalDestination)) {
                            DeadLetterStrategy strategy = destination.getDeadLetterStrategy();
                            strategy.rollback(m.getMessage());
                        }
                    }
                }
            } finally {
                messagesLock.writeLock().unlock();
            }
        } finally {
            for (Destination d: destsToPause) {
                if (d instanceof Queue) {
                    ((Queue)d).resumeDispatch();
                }
            }
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

        if (destination.equals(dest)) {
            return 0;
        }

        int movedCounter = 0;
        Set<MessageReference> set = new LinkedHashSet<MessageReference>();
        do {
            doPageIn(true);
            pagedInMessagesLock.readLock().lock();
            try {
                if (!set.addAll(pagedInMessages.values())) {
                    // nothing new to check - mem constraint on page in
                    return movedCounter;
                }
            } finally {
                pagedInMessagesLock.readLock().unlock();
            }
            List<MessageReference> list = new ArrayList<MessageReference>(set);
            for (MessageReference ref : list) {
                if (filter.evaluate(context, ref)) {
                    // We should only move messages that can be locked.
                    moveMessageTo(context, (QueueMessageReference)ref, dest);
                    set.remove(ref);
                    if (++movedCounter >= maximumMessages && maximumMessages > 0) {
                        return movedCounter;
                    }
                }
            }
        } while (set.size() < this.destinationStatistics.getMessages().getCount() && set.size() < maximumMessages);
        return movedCounter;
    }

    public int retryMessages(ConnectionContext context, int maximumMessages) throws Exception {
        if (!isDLQ()) {
            throw new Exception("Retry of message is only possible on Dead Letter Queues!");
        }
        int restoredCounter = 0;
        // ensure we deal with a snapshot to avoid potential duplicates in the event of messages
        // getting immediate dlq'ed
        long numberOfRetryAttemptsToCheckAllMessagesOnce = this.destinationStatistics.getMessages().getCount();
        Set<MessageReference> set = new LinkedHashSet<MessageReference>();
        do {
            doPageIn(true);
            pagedInMessagesLock.readLock().lock();
            try {
                if (!set.addAll(pagedInMessages.values())) {
                    // nothing new to check - mem constraint on page in
                    return restoredCounter;
                }
            } finally {
                pagedInMessagesLock.readLock().unlock();
            }
            List<MessageReference> list = new ArrayList<MessageReference>(set);
            for (MessageReference ref : list) {
                numberOfRetryAttemptsToCheckAllMessagesOnce--;
                if (ref.getMessage().getOriginalDestination() != null) {

                    moveMessageTo(context, (QueueMessageReference)ref, ref.getMessage().getOriginalDestination());
                    set.remove(ref);
                    if (++restoredCounter >= maximumMessages && maximumMessages > 0) {
                        return restoredCounter;
                    }
                }
            }
        } while (numberOfRetryAttemptsToCheckAllMessagesOnce > 0 && set.size() < this.destinationStatistics.getMessages().getCount());
        return restoredCounter;
    }

    /**
     * @return true if we would like to iterate again
     * @see org.apache.activemq.thread.Task#iterate()
     */
    @Override
    public boolean iterate() {
        MDC.put("activemq.destination", getName());
        boolean pageInMoreMessages = false;
        synchronized (iteratingMutex) {

            // If optimize dispatch is on or this is a slave this method could be called recursively
            // we set this state value to short-circuit wakeup in those cases to avoid that as it
            // could lead to errors.
            iterationRunning = true;

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
                            LOG.debug("{} consumers subscribed. Starting dispatch.", consumers.size());
                        } else {
                            LOG.debug("{} ms elapsed and {} consumers subscribed. Starting dispatch.", timeout, consumers.size());
                        }
                    }
                    if (timeBeforeDispatchStarts > 0 && consumersBeforeDispatchStarts <= 0) {
                        iteratingMutex.wait(timeBeforeDispatchStarts);
                        LOG.debug("{} ms elapsed. Starting dispatch.", timeBeforeDispatchStarts);
                    }
                } catch (Exception e) {
                    LOG.error(e.toString());
                }
            }

            messagesLock.readLock().lock();
            try{
                pageInMoreMessages |= !messages.isEmpty();
            } finally {
                messagesLock.readLock().unlock();
            }

            pagedInPendingDispatchLock.readLock().lock();
            try {
                pageInMoreMessages |= !dispatchPendingList.isEmpty();
            } finally {
                pagedInPendingDispatchLock.readLock().unlock();
            }

            boolean hasBrowsers = !browserSubscriptions.isEmpty();

            if (pageInMoreMessages || hasBrowsers || !dispatchPendingList.hasRedeliveries()) {
                try {
                    pageInMessages(hasBrowsers && getMaxBrowsePageSize() > 0, getMaxPageSize());
                } catch (Throwable e) {
                    LOG.error("Failed to page in more queue messages ", e);
                }
            }

            if (hasBrowsers) {
                PendingList messagesInMemory = isPrioritizedMessages() ?
                        new PrioritizedPendingList() : new OrderedPendingList();
                pagedInMessagesLock.readLock().lock();
                try {
                    messagesInMemory.addAll(pagedInMessages);
                } finally {
                    pagedInMessagesLock.readLock().unlock();
                }

                Iterator<QueueBrowserSubscription> browsers = browserSubscriptions.iterator();
                while (browsers.hasNext()) {
                    QueueBrowserSubscription browser = browsers.next();
                    try {
                        MessageEvaluationContext msgContext = new NonCachedMessageEvaluationContext();
                        msgContext.setDestination(destination);

                        LOG.debug("dispatch to browser: {}, already dispatched/paged count: {}", browser, messagesInMemory.size());
                        boolean added = false;
                        for (MessageReference node : messagesInMemory) {
                            if (!((QueueMessageReference)node).isAcked() && !browser.isDuplicate(node.getMessageId()) && !browser.atMax()) {
                                msgContext.setMessageReference(node);
                                if (browser.matches(node, msgContext)) {
                                    browser.add(node);
                                    added = true;
                                }
                            }
                        }
                        // are we done browsing? no new messages paged
                        if (!added || browser.atMax()) {
                            browser.decrementQueueRef();
                            browsers.remove();
                        } else {
                            wakeup();
                        }
                    } catch (Exception e) {
                        LOG.warn("exception on dispatch to browser: {}", browser, e);
                    }
                }
            }

            if (pendingWakeups.get() > 0) {
                pendingWakeups.decrementAndGet();
            }
            MDC.remove("activemq.destination");
            iterationRunning = false;

            return pendingWakeups.get() > 0;
        }
    }

    public void pauseDispatch() {
        dispatchSelector.pause();
    }

    public void resumeDispatch() {
        dispatchSelector.resume();
        wakeup();
    }

    public boolean isDispatchPaused() {
        return dispatchSelector.isPaused();
    }

    protected MessageReferenceFilter createMessageIdFilter(final String messageId) {
        return new MessageReferenceFilter() {
            @Override
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

        if (selector == null || selector.isEmpty()) {
            return new MessageReferenceFilter() {

                @Override
                public boolean evaluate(ConnectionContext context, MessageReference messageReference) throws JMSException {
                    return true;
                }
            };
        }

        final BooleanExpression selectorExpression = SelectorParser.parse(selector);

        return new MessageReferenceFilter() {
            @Override
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
            dispatchPendingList.remove(r);
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
        LOG.trace("ack of {} with {}", reference.getMessageId(), ack);
        // This sends the ack the the journal..
        if (!ack.isInTransaction()) {
            acknowledge(context, sub, ack, reference);
            dropMessage(reference);
        } else {
            try {
                acknowledge(context, sub, ack, reference);
            } finally {
                context.getTransaction().addSynchronization(new Synchronization() {

                    @Override
                    public void afterCommit() throws Exception {
                        dropMessage(reference);
                        wakeup();
                    }

                    @Override
                    public void afterRollback() throws Exception {
                        reference.setAcked(false);
                        wakeup();
                    }
                });
            }
        }
        if (ack.isPoisonAck() || (sub != null && sub.getConsumerInfo().isNetworkSubscription())) {
            // message gone to DLQ, is ok to allow redelivery
            messagesLock.writeLock().lock();
            try {
                messages.rollback(reference.getMessageId());
            } finally {
                messagesLock.writeLock().unlock();
            }
            if (sub != null && sub.getConsumerInfo().isNetworkSubscription()) {
                getDestinationStatistics().getForwards().increment();
            }
        }
        // after successful store update
        reference.setAcked(true);
    }

    private void dropMessage(QueueMessageReference reference) {
        //use dropIfLive so we only process the statistics at most one time
        if (reference.dropIfLive()) {
            getDestinationStatistics().getDequeues().increment();
            getDestinationStatistics().getMessages().decrement();
            pagedInMessagesLock.writeLock().lock();
            try {
                pagedInMessages.remove(reference);
            } finally {
                pagedInMessagesLock.writeLock().unlock();
            }
        }
    }

    public void messageExpired(ConnectionContext context, MessageReference reference) {
        messageExpired(context, null, reference);
    }

    @Override
    public void messageExpired(ConnectionContext context, Subscription subs, MessageReference reference) {
        LOG.debug("message expired: {}", reference);
        broker.messageExpired(context, reference, subs);
        destinationStatistics.getExpired().increment();
        try {
            removeMessage(context, subs, (QueueMessageReference) reference);
            messagesLock.writeLock().lock();
            try {
                messages.rollback(reference.getMessageId());
            } finally {
                messagesLock.writeLock().unlock();
            }
        } catch (IOException e) {
            LOG.error("Failed to remove expired Message from the store ", e);
        }
    }

    private final boolean cursorAdd(final Message msg) throws Exception {
        messagesLock.writeLock().lock();
        try {
            return messages.addMessageLast(msg);
        } finally {
            messagesLock.writeLock().unlock();
        }
    }

    private final boolean tryCursorAdd(final Message msg) throws Exception {
        messagesLock.writeLock().lock();
        try {
            return messages.tryAddMessageLast(msg, 50);
        } finally {
            messagesLock.writeLock().unlock();
        }
    }

    final void messageSent(final ConnectionContext context, final Message msg) throws Exception {
        pendingSends.decrementAndGet();
        destinationStatistics.getEnqueues().increment();
        destinationStatistics.getMessages().increment();
        destinationStatistics.getMessageSize().addSize(msg.getSize());
        messageDelivered(context, msg);
        consumersLock.readLock().lock();
        try {
            if (consumers.isEmpty()) {
                onMessageWithNoConsumers(context, msg);
            }
        }finally {
            consumersLock.readLock().unlock();
        }
        LOG.debug("{} Message {} sent to {}",broker.getBrokerName(), msg.getMessageId(), this.destination);
        wakeup();
    }

    @Override
    public void wakeup() {
        if (optimizedDispatch && !iterationRunning) {
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
            LOG.warn("Async task runner failed to wakeup ", e);
        }
    }

    private void doPageIn(boolean force) throws Exception {
        doPageIn(force, true, getMaxPageSize());
    }

    private void doPageIn(boolean force, boolean processExpired, int maxPageSize) throws Exception {
        PendingList newlyPaged = doPageInForDispatch(force, processExpired, maxPageSize);
        pagedInPendingDispatchLock.writeLock().lock();
        try {
            if (dispatchPendingList.isEmpty()) {
                dispatchPendingList.addAll(newlyPaged);

            } else {
                for (MessageReference qmr : newlyPaged) {
                    if (!dispatchPendingList.contains(qmr)) {
                        dispatchPendingList.addMessageLast(qmr);
                    }
                }
            }
        } finally {
            pagedInPendingDispatchLock.writeLock().unlock();
        }
    }

    private PendingList doPageInForDispatch(boolean force, boolean processExpired, int maxPageSize) throws Exception {
        List<QueueMessageReference> result = null;
        PendingList resultList = null;

        int toPageIn = maxPageSize;
        messagesLock.readLock().lock();
        try {
            toPageIn = Math.min(toPageIn, messages.size());
        } finally {
            messagesLock.readLock().unlock();
        }
        int pagedInPendingSize = 0;
        pagedInPendingDispatchLock.readLock().lock();
        try {
            pagedInPendingSize = dispatchPendingList.size();
        } finally {
            pagedInPendingDispatchLock.readLock().unlock();
        }
        if (isLazyDispatch() && !force) {
            // Only page in the minimum number of messages which can be
            // dispatched immediately.
            toPageIn = Math.min(toPageIn, getConsumerMessageCountBeforeFull());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} toPageIn: {}, force:{}, Inflight: {}, pagedInMessages.size {}, pagedInPendingDispatch.size {}, enqueueCount: {}, dequeueCount: {}, memUsage:{}, maxPageSize:{}",
                    this,
                    toPageIn,
                    force,
                    destinationStatistics.getInflight().getCount(),
                    pagedInMessages.size(),
                    pagedInPendingSize,
                    destinationStatistics.getEnqueues().getCount(),
                    destinationStatistics.getDequeues().getCount(),
                    getMemoryUsage().getUsage(),
                    maxPageSize);
        }

        if (toPageIn > 0 && (force || (haveRealConsumer() && pagedInPendingSize < maxPageSize))) {
            int count = 0;
            result = new ArrayList<QueueMessageReference>(toPageIn);
            messagesLock.writeLock().lock();
            try {
                try {
                    messages.setMaxBatchSize(toPageIn);
                    messages.reset();
                    while (count < toPageIn && messages.hasNext()) {
                        MessageReference node = messages.next();
                        messages.remove();

                        QueueMessageReference ref = createMessageReference(node.getMessage());
                        if (processExpired && ref.isExpired()) {
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

            if (count > 0) {
                // Only add new messages, not already pagedIn to avoid multiple
                // dispatch attempts
                pagedInMessagesLock.writeLock().lock();
                try {
                    if (isPrioritizedMessages()) {
                        resultList = new PrioritizedPendingList();
                    } else {
                        resultList = new OrderedPendingList();
                    }
                    for (QueueMessageReference ref : result) {
                        if (!pagedInMessages.contains(ref)) {
                            pagedInMessages.addMessageLast(ref);
                            resultList.addMessageLast(ref);
                        } else {
                            ref.decrementReferenceCount();
                            // store should have trapped duplicate in it's index, or cursor audit trapped insert
                            // or producerBrokerExchange suppressed send.
                            // note: jdbc store will not trap unacked messages as a duplicate b/c it gives each message a unique sequence id
                            LOG.warn("{}, duplicate message {} - {} from cursor, is cursor audit disabled or too constrained? Redirecting to dlq", this, ref.getMessageId(), ref.getMessage().getMessageId().getFutureOrSequenceLong());
                            if (store != null) {
                                ConnectionContext connectionContext = createConnectionContext();
                                dropMessage(ref);
                                if (gotToTheStore(ref.getMessage())) {
                                    LOG.debug("Duplicate message {} from cursor, removing from store", ref.getMessage());
                                    store.removeMessage(connectionContext, new MessageAck(ref.getMessage(), MessageAck.POISON_ACK_TYPE, 1));
                                }
                                broker.getRoot().sendToDeadLetterQueue(connectionContext, ref.getMessage(), null, new Throwable("duplicate paged in from cursor for " + destination));
                            }
                        }
                    }
                } finally {
                    pagedInMessagesLock.writeLock().unlock();
                }
            } else if (!messages.hasSpace()) {
                if (isFlowControlLogRequired()) {
                    LOG.warn("{} cursor blocked, no space available to page in messages; usage: {}", this, this.systemUsage.getMemoryUsage());
                } else {
                    LOG.debug("{} cursor blocked, no space available to page in messages; usage: {}", this, this.systemUsage.getMemoryUsage());
                }
            }
        }

        // Avoid return null list, if condition is not validated
        return resultList != null ? resultList : new OrderedPendingList();
    }

    private final boolean haveRealConsumer() {
        return consumers.size() - browserSubscriptions.size() > 0;
    }

    private void doDispatch(PendingList list) throws Exception {
        boolean doWakeUp = false;

        pagedInPendingDispatchLock.writeLock().lock();
        try {
            if (isPrioritizedMessages() && !dispatchPendingList.isEmpty() && list != null && !list.isEmpty()) {
                // merge all to select priority order
                for (MessageReference qmr : list) {
                    if (!dispatchPendingList.contains(qmr)) {
                        dispatchPendingList.addMessageLast(qmr);
                    }
                }
                list = null;
            }

            doActualDispatch(dispatchPendingList);
            // and now see if we can dispatch the new stuff.. and append to the pending
            // list anything that does not actually get dispatched.
            if (list != null && !list.isEmpty()) {
                if (dispatchPendingList.isEmpty()) {
                    dispatchPendingList.addAll(doActualDispatch(list));
                } else {
                    for (MessageReference qmr : list) {
                        if (!dispatchPendingList.contains(qmr)) {
                            dispatchPendingList.addMessageLast(qmr);
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
    private PendingList doActualDispatch(PendingList list) throws Exception {
        List<Subscription> consumers;
        consumersLock.readLock().lock();

        try {
            if (this.consumers.isEmpty()) {
                // slave dispatch happens in processDispatchNotification
                return list;
            }
            consumers = new ArrayList<Subscription>(this.consumers);
        } finally {
            consumersLock.readLock().unlock();
        }

        Set<Subscription> fullConsumers = new HashSet<Subscription>(this.consumers.size());

        for (Iterator<MessageReference> iterator = list.iterator(); iterator.hasNext();) {

            MessageReference node = iterator.next();
            Subscription target = null;
            for (Subscription s : consumers) {
                if (s instanceof QueueBrowserSubscription) {
                    continue;
                }
                if (!fullConsumers.contains(s)) {
                    if (!s.isFull()) {
                        if (dispatchSelector.canSelect(s, node) && assignMessageGroup(s, (QueueMessageReference)node) && !((QueueMessageReference) node).isAcked() ) {
                            // Dispatch it.
                            s.add(node);
                            LOG.trace("assigned {} to consumer {}", node.getMessageId(), s.getConsumerInfo().getConsumerId());
                            iterator.remove();
                            target = s;
                            break;
                        }
                    } else {
                        // no further dispatch of list to a full consumer to
                        // avoid out of order message receipt
                        fullConsumers.add(s);

                        //For full consumers we need to mark that they are slow and
                        // then call the broker.slowConsumer() hook if implemented
                        if (s instanceof PrefetchSubscription) {
                            final PrefetchSubscription sub = (PrefetchSubscription) s;
                            if (!sub.isSlowConsumer()) {
                                sub.setSlowConsumer(true);
                                broker.slowConsumer(sub.getContext(), this, sub);
                            }
                        }
                        LOG.trace("Subscription full {}", s);
                    }
                }
            }

            if (target == null && node.isDropped()) {
                iterator.remove();
            }

            // return if there are no consumers or all consumers are full
            if (target == null && consumers.size() == fullConsumers.size()) {
                return list;
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
                } finally {
                    consumersLock.writeLock().unlock();
                }
            }
        }

        return list;
    }

    protected boolean assignMessageGroup(Subscription subscription, QueueMessageReference node) throws Exception {
        boolean result = true;
        // Keep message groups together.
        String groupId = node.getGroupID();
        int sequence = node.getGroupSequence();
        if (groupId != null) {

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
                            subscription.getConsumerInfo().decrementAssignedGroupCount(destination);
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
        message.setJMSXGroupFirstForConsumer(true);
        subs.getConsumerInfo().incrementAssignedGroupCount(destination);
    }

    protected void pageInMessages(boolean force, int maxPageSize) throws Exception {
        doDispatch(doPageInForDispatch(force, true, maxPageSize));
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
        consumersLock.readLock().lock();
        try {
            for (Subscription s : consumers) {
                if (s.isBrowser()) {
                    continue;
                }
                int countBeforeFull = s.countBeforeFull();
                total += countBeforeFull;
            }
        } finally {
            consumersLock.readLock().unlock();
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
            for (MessageReference ref : dispatchPendingList) {
                if (messageId.equals(ref.getMessageId())) {
                    message = (QueueMessageReference)ref;
                    dispatchPendingList.remove(ref);
                    break;
                }
            }
        } finally {
            pagedInPendingDispatchLock.writeLock().unlock();
        }

        if (message == null) {
            pagedInMessagesLock.readLock().lock();
            try {
                message = (QueueMessageReference)pagedInMessages.get(messageId);
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
                    + dispatchPendingList.size() + ") for subscription: "
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
        } finally {
            consumersLock.readLock().unlock();
        }
        return sub;
    }

    @Override
    public void onUsageChanged(@SuppressWarnings("rawtypes") Usage usage, int oldPercentUsage, int newPercentUsage) {
        if (oldPercentUsage > newPercentUsage) {
            asyncWakeup();
        }
    }

    @Override
    protected Logger getLog() {
        return LOG;
    }

    protected boolean isOptimizeStorage(){
        boolean result = false;
        if (isDoOptimzeMessageStorage()){
            consumersLock.readLock().lock();
            try{
                if (consumers.isEmpty()==false){
                    result = true;
                    for (Subscription s : consumers) {
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
            } finally {
                consumersLock.readLock().unlock();
            }
        }
        return result;
    }
}
