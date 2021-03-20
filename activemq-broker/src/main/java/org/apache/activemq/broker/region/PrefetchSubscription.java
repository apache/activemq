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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.VMPendingMessageCursor;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.Response;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.transport.TransmitCallback;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A subscription that honors the pre-fetch option of the ConsumerInfo.
 */
public abstract class PrefetchSubscription extends AbstractSubscription {

    private static final Logger LOG = LoggerFactory.getLogger(PrefetchSubscription.class);
    protected final Scheduler scheduler;

    protected PendingMessageCursor pending;
    protected final List<MessageReference> dispatched = new ArrayList<MessageReference>();
    private int maxProducersToAudit=32;
    private int maxAuditDepth=2048;
    protected final SystemUsage usageManager;
    protected final Object pendingLock = new Object();
    protected final Object dispatchLock = new Object();
    private final CountDownLatch okForAckAsDispatchDone = new CountDownLatch(1);

    public PrefetchSubscription(Broker broker, SystemUsage usageManager, ConnectionContext context, ConsumerInfo info, PendingMessageCursor cursor) throws JMSException {
        super(broker,context, info);
        this.usageManager=usageManager;
        pending = cursor;
        try {
            pending.start();
        } catch (Exception e) {
            throw new JMSException(e.getMessage());
        }
        this.scheduler = broker.getScheduler();
    }

    public PrefetchSubscription(Broker broker,SystemUsage usageManager, ConnectionContext context, ConsumerInfo info) throws JMSException {
        this(broker,usageManager,context, info, new VMPendingMessageCursor(false));
    }

    /**
     * Allows a message to be pulled on demand by a client
     */
    @Override
    public Response pullMessage(ConnectionContext context, final MessagePull pull) throws Exception {
        // The slave should not deliver pull messages.
        // TODO: when the slave becomes a master, He should send a NULL message to all the
        // consumers to 'wake them up' in case they were waiting for a message.
        if (getPrefetchSize() == 0) {
            prefetchExtension.set(pull.getQuantity());
            final long dispatchCounterBeforePull = getSubscriptionStatistics().getDispatched().getCount();

            // Have the destination push us some messages.
            for (Destination dest : destinations) {
                dest.iterate();
            }
            dispatchPending();

            synchronized(this) {
                // If there was nothing dispatched.. we may need to setup a timeout.
                if (dispatchCounterBeforePull == getSubscriptionStatistics().getDispatched().getCount() || pull.isAlwaysSignalDone()) {
                    // immediate timeout used by receiveNoWait()
                    if (pull.getTimeout() == -1) {
                        // Null message indicates the pull is done or did not have pending.
                        prefetchExtension.set(1);
                        add(QueueMessageReference.NULL_MESSAGE);
                        dispatchPending();
                    }
                    if (pull.getTimeout() > 0) {
                        scheduler.executeAfterDelay(new Runnable() {
                            @Override
                            public void run() {
                                pullTimeout(dispatchCounterBeforePull, pull.isAlwaysSignalDone());
                            }
                        }, pull.getTimeout());
                    }
                }
            }
        }
        return null;
    }

    /**
     * Occurs when a pull times out. If nothing has been dispatched since the
     * timeout was setup, then send the NULL message.
     */
    final void pullTimeout(long dispatchCounterBeforePull, boolean alwaysSignalDone) {
        synchronized (pendingLock) {
            if (dispatchCounterBeforePull == getSubscriptionStatistics().getDispatched().getCount() || alwaysSignalDone) {
                try {
                    prefetchExtension.set(1);
                    add(QueueMessageReference.NULL_MESSAGE);
                    dispatchPending();
                } catch (Exception e) {
                    context.getConnection().serviceException(e);
                } finally {
                    prefetchExtension.set(0);
                }
            }
        }
    }

    @Override
    public void add(MessageReference node) throws Exception {
        synchronized (pendingLock) {
            // The destination may have just been removed...
            if (!destinations.contains(node.getRegionDestination()) && node != QueueMessageReference.NULL_MESSAGE) {
                // perhaps we should inform the caller that we are no longer valid to dispatch to?
                return;
            }

            // Don't increment for the pullTimeout control message.
            if (!node.equals(QueueMessageReference.NULL_MESSAGE)) {
                getSubscriptionStatistics().getEnqueues().increment();
            }
            pending.addMessageLast(node);
        }
        dispatchPending();
    }

    @Override
    public void processMessageDispatchNotification(MessageDispatchNotification mdn) throws Exception {
        synchronized(pendingLock) {
            try {
                pending.reset();
                while (pending.hasNext()) {
                    MessageReference node = pending.next();
                    node.decrementReferenceCount();
                    if (node.getMessageId().equals(mdn.getMessageId())) {
                        // Synchronize between dispatched list and removal of messages from pending list
                        // related to remove subscription action
                        synchronized(dispatchLock) {
                            pending.remove();
                            createMessageDispatch(node, node.getMessage());
                            dispatched.add(node);
                            onDispatch(node, node.getMessage());
                        }
                        return;
                    }
                }
            } finally {
                pending.release();
            }
        }
        throw new JMSException(
                "Slave broker out of sync with master: Dispatched message ("
                        + mdn.getMessageId() + ") was not in the pending list for "
                        + mdn.getConsumerId() + " on " + mdn.getDestination().getPhysicalName());
    }

    @Override
    public final void acknowledge(final ConnectionContext context,final MessageAck ack) throws Exception {
        // Handle the standard acknowledgment case.
        boolean callDispatchMatched = false;
        Destination destination = null;

        if (!okForAckAsDispatchDone.await(0l, TimeUnit.MILLISECONDS)) {
            // suppress unexpected ack exception in this expected case
            LOG.warn("Ignoring ack received before dispatch; result of failover with an outstanding ack. Acked messages will be replayed if present on this broker. Ignored ack: {}", ack);
            return;
        }

        LOG.trace("ack: {}", ack);

        synchronized(dispatchLock) {
            if (ack.isStandardAck()) {
                // First check if the ack matches the dispatched. When using failover this might
                // not be the case. We don't ever want to ack the wrong messages.
                assertAckMatchesDispatched(ack);

                // Acknowledge all dispatched messages up till the message id of
                // the acknowledgment.
                boolean inAckRange = false;
                List<MessageReference> removeList = new ArrayList<MessageReference>();
                for (final MessageReference node : dispatched) {
                    MessageId messageId = node.getMessageId();
                    if (ack.getFirstMessageId() == null
                            || ack.getFirstMessageId().equals(messageId)) {
                        inAckRange = true;
                    }
                    if (inAckRange) {
                        // Don't remove the nodes until we are committed.
                        if (!context.isInTransaction()) {
                            getSubscriptionStatistics().getDequeues().increment();
                            removeList.add(node);
                            contractPrefetchExtension(1);
                        } else {
                            registerRemoveSync(context, node);
                        }
                        acknowledge(context, ack, node);
                        if (ack.getLastMessageId().equals(messageId)) {
                            destination = (Destination) node.getRegionDestination();
                            callDispatchMatched = true;
                            break;
                        }
                    }
                }
                for (final MessageReference node : removeList) {
                    dispatched.remove(node);
                    decrementPrefetchCounter(node);
                }
                // this only happens after a reconnect - get an ack which is not
                // valid
                if (!callDispatchMatched) {
                    LOG.warn("Could not correlate acknowledgment with dispatched message: {}", ack);
                }
            } else if (ack.isIndividualAck()) {
                // Message was delivered and acknowledge - but only delete the
                // individual message
                for (final MessageReference node : dispatched) {
                    MessageId messageId = node.getMessageId();
                    if (ack.getLastMessageId().equals(messageId)) {
                        // Don't remove the nodes until we are committed - immediateAck option
                        if (!context.isInTransaction()) {
                            getSubscriptionStatistics().getDequeues().increment();
                            dispatched.remove(node);
                            decrementPrefetchCounter(node);
                            contractPrefetchExtension(1);
                        } else {
                            registerRemoveSync(context, node);
                            expandPrefetchExtension(1);
                        }
                        acknowledge(context, ack, node);
                        destination = (Destination) node.getRegionDestination();
                        callDispatchMatched = true;
                        break;
                    }
                }
            } else if (ack.isDeliveredAck()) {
                // Message was delivered but not acknowledged: update pre-fetch
                // counters.
                int index = 0;
                for (Iterator<MessageReference> iter = dispatched.iterator(); iter.hasNext(); index++) {
                    final MessageReference node = iter.next();
                    Destination nodeDest = (Destination) node.getRegionDestination();
                    if (ack.getLastMessageId().equals(node.getMessageId())) {
                        expandPrefetchExtension(ack.getMessageCount());
                        destination = nodeDest;
                        callDispatchMatched = true;
                        break;
                    }
                }
                if (!callDispatchMatched) {
                    throw new JMSException(
                            "Could not correlate acknowledgment with dispatched message: "
                                    + ack);
                }
            } else if (ack.isExpiredAck()) {
                // Message was expired
                int index = 0;
                boolean inAckRange = false;
                for (Iterator<MessageReference> iter = dispatched.iterator(); iter.hasNext(); index++) {
                    final MessageReference node = iter.next();
                    Destination nodeDest = (Destination) node.getRegionDestination();
                    MessageId messageId = node.getMessageId();
                    if (ack.getFirstMessageId() == null || ack.getFirstMessageId().equals(messageId)) {
                        inAckRange = true;
                    }
                    if (inAckRange) {
                        Destination regionDestination = nodeDest;
                        if (broker.isExpired(node)) {
                            regionDestination.messageExpired(context, this, node);
                        }
                        iter.remove();
                        decrementPrefetchCounter(node);

                        if (ack.getLastMessageId().equals(messageId)) {
                            contractPrefetchExtension(1);
                            destination = (Destination) node.getRegionDestination();
                            callDispatchMatched = true;
                            break;
                        }
                    }
                }
                if (!callDispatchMatched) {
                    throw new JMSException(
                            "Could not correlate expiration acknowledgment with dispatched message: "
                                    + ack);
                }
            } else if (ack.isRedeliveredAck()) {
                // Message was re-delivered but it was not yet considered to be
                // a DLQ message.
                boolean inAckRange = false;
                for (final MessageReference node : dispatched) {
                    MessageId messageId = node.getMessageId();
                    if (ack.getFirstMessageId() == null
                            || ack.getFirstMessageId().equals(messageId)) {
                        inAckRange = true;
                    }
                    if (inAckRange) {
                        if (ack.getLastMessageId().equals(messageId)) {
                            destination = (Destination) node.getRegionDestination();
                            callDispatchMatched = true;
                            break;
                        }
                    }
                }
                if (!callDispatchMatched) {
                    throw new JMSException(
                            "Could not correlate acknowledgment with dispatched message: "
                                    + ack);
                }
            } else if (ack.isPoisonAck()) {
                // TODO: what if the message is already in a DLQ???
                // Handle the poison ACK case: we need to send the message to a
                // DLQ
                if (ack.isInTransaction()) {
                    throw new JMSException("Poison ack cannot be transacted: "
                            + ack);
                }
                int index = 0;
                boolean inAckRange = false;
                List<MessageReference> removeList = new ArrayList<MessageReference>();
                for (final MessageReference node : dispatched) {
                    MessageId messageId = node.getMessageId();
                    if (ack.getFirstMessageId() == null
                            || ack.getFirstMessageId().equals(messageId)) {
                        inAckRange = true;
                    }
                    if (inAckRange) {
                        sendToDLQ(context, node, ack.getPoisonCause());
                        Destination nodeDest = (Destination) node.getRegionDestination();
                        removeList.add(node);
                        getSubscriptionStatistics().getDequeues().increment();
                        index++;
                        acknowledge(context, ack, node);
                        if (ack.getLastMessageId().equals(messageId)) {
                            contractPrefetchExtension(1);
                            destination = nodeDest;
                            callDispatchMatched = true;
                            break;
                        }
                    }
                }
                for (final MessageReference node : removeList) {
                    dispatched.remove(node);
                    decrementPrefetchCounter(node);
                }
                if (!callDispatchMatched) {
                    throw new JMSException(
                            "Could not correlate acknowledgment with dispatched message: "
                                    + ack);
                }
            }
        }
        if (callDispatchMatched && destination != null) {
            destination.wakeup();
            dispatchPending();

            if (pending.isEmpty()) {
                wakeupDestinationsForDispatch();
            }
        } else {
            LOG.debug("Acknowledgment out of sync (Normally occurs when failover connection reconnects): {}", ack);
        }
    }

    private void registerRemoveSync(ConnectionContext context, final MessageReference node) {
        // setup a Synchronization to remove nodes from the
        // dispatched list.
        context.getTransaction().addSynchronization(
                new Synchronization() {

                    @Override
                    public void afterCommit()
                            throws Exception {
                        Destination nodeDest = (Destination) node.getRegionDestination();
                        synchronized (dispatchLock) {
                            getSubscriptionStatistics().getDequeues().increment();
                            if (dispatched.remove(node)) {
                                // if consumer is removed, dispatched will be empty and inflight will
                                // already have been adjusted
                                decrementPrefetchCounter(node);
                            }
                        }
                        contractPrefetchExtension(1);
                        nodeDest.wakeup();
                        dispatchPending();
                    }

                    @Override
                    public void afterRollback() throws Exception {
                        contractPrefetchExtension(1);
                    }
                });
    }

    /**
     * Checks an ack versus the contents of the dispatched list.
     *  called with dispatchLock held
     * @param ack
     * @throws JMSException if it does not match
     */
    protected void assertAckMatchesDispatched(MessageAck ack) throws JMSException {
        MessageId firstAckedMsg = ack.getFirstMessageId();
        MessageId lastAckedMsg = ack.getLastMessageId();
        int checkCount = 0;
        boolean checkFoundStart = false;
        boolean checkFoundEnd = false;
        for (MessageReference node : dispatched) {

            if (firstAckedMsg == null) {
                checkFoundStart = true;
            } else if (!checkFoundStart && firstAckedMsg.equals(node.getMessageId())) {
                checkFoundStart = true;
            }

            if (checkFoundStart) {
                checkCount++;
            }

            if (lastAckedMsg != null && lastAckedMsg.equals(node.getMessageId())) {
                checkFoundEnd = true;
                break;
            }
        }
        if (!checkFoundStart && firstAckedMsg != null)
            throw new JMSException("Unmatched acknowledge: " + ack
                    + "; Could not find Message-ID " + firstAckedMsg
                    + " in dispatched-list (start of ack)");
        if (!checkFoundEnd && lastAckedMsg != null)
            throw new JMSException("Unmatched acknowledge: " + ack
                    + "; Could not find Message-ID " + lastAckedMsg
                    + " in dispatched-list (end of ack)");
        if (ack.getMessageCount() != checkCount && !ack.isInTransaction()) {
            throw new JMSException("Unmatched acknowledge: " + ack
                    + "; Expected message count (" + ack.getMessageCount()
                    + ") differs from count in dispatched-list (" + checkCount
                    + ")");
        }
    }

    /**
     *
     * @param context
     * @param node
     * @param poisonCause
     * @throws IOException
     * @throws Exception
     */
    protected void sendToDLQ(final ConnectionContext context, final MessageReference node, Throwable poisonCause) throws IOException, Exception {
        broker.getRoot().sendToDeadLetterQueue(context, node, this, poisonCause);
    }

    @Override
    public int getInFlightSize() {
        return dispatched.size();
    }

    /**
     * Used to determine if the broker can dispatch to the consumer.
     *
     * @return true if the subscription is full
     */
    @Override
    public boolean isFull() {
        return getPrefetchSize() == 0 ? prefetchExtension.get() == 0 : dispatched.size() - prefetchExtension.get() >= info.getPrefetchSize();
    }

    /**
     * @return true when 60% or more room is left for dispatching messages
     */
    @Override
    public boolean isLowWaterMark() {
        return (dispatched.size() - prefetchExtension.get()) <= (info.getPrefetchSize() * .4);
    }

    /**
     * @return true when 10% or less room is left for dispatching messages
     */
    @Override
    public boolean isHighWaterMark() {
        return (dispatched.size() - prefetchExtension.get()) >= (info.getPrefetchSize() * .9);
    }

    @Override
    public int countBeforeFull() {
        return getPrefetchSize() == 0 ? prefetchExtension.get() : info.getPrefetchSize() + prefetchExtension.get() - dispatched.size();
    }

    @Override
    public int getPendingQueueSize() {
        return pending.size();
    }

    @Override
    public long getPendingMessageSize() {
        return pending.messageSize();
    }

    @Override
    public int getDispatchedQueueSize() {
        return dispatched.size();
    }

    @Override
    public long getDequeueCounter() {
        return getSubscriptionStatistics().getDequeues().getCount();
    }

    @Override
    public long getDispatchedCounter() {
        return getSubscriptionStatistics().getDispatched().getCount();
    }

    @Override
    public long getEnqueueCounter() {
        return getSubscriptionStatistics().getEnqueues().getCount();
    }

    @Override
    public boolean isRecoveryRequired() {
        return pending.isRecoveryRequired();
    }

    public PendingMessageCursor getPending() {
        return this.pending;
    }

    public void setPending(PendingMessageCursor pending) {
        this.pending = pending;
        if (this.pending!=null) {
            this.pending.setSystemUsage(usageManager);
            this.pending.setMemoryUsageHighWaterMark(getCursorMemoryHighWaterMark());
        }
    }

    @Override
    public void add(ConnectionContext context, Destination destination) throws Exception {
        synchronized(pendingLock) {
            super.add(context, destination);
            pending.add(context, destination);
        }
    }

    @Override
    public List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception {
        return remove(context, destination, dispatched);
    }

    public List<MessageReference> remove(ConnectionContext context, Destination destination, List<MessageReference> dispatched) throws Exception {
        LinkedList<MessageReference> redispatch = new LinkedList<MessageReference>();
        synchronized(pendingLock) {
            super.remove(context, destination);
            // Here is a potential problem concerning Inflight stat:
            // Messages not already committed or rolled back may not be removed from dispatched list at the moment
            // Except if each commit or rollback callback action comes before remove of subscriber.
            redispatch.addAll(pending.remove(context, destination));

            if (dispatched == null) {
                return redispatch;
            }

            // Synchronized to DispatchLock if necessary
            if (dispatched == this.dispatched) {
                synchronized(dispatchLock) {
                    addReferencesAndUpdateRedispatch(redispatch, destination, dispatched);
                }
            } else {
                addReferencesAndUpdateRedispatch(redispatch, destination, dispatched);
            }
        }

        return redispatch;
    }

    private void addReferencesAndUpdateRedispatch(LinkedList<MessageReference> redispatch, Destination destination, List<MessageReference> dispatched) {
        ArrayList<MessageReference> references = new ArrayList<MessageReference>();
        for (MessageReference r : dispatched) {
            if (r.getRegionDestination() == destination) {
                references.add(r);
                getSubscriptionStatistics().getInflightMessageSize().addSize(-r.getSize());
            }
        }
        redispatch.addAll(0, references);
        destination.getDestinationStatistics().getInflight().subtract(references.size());
        dispatched.removeAll(references);
    }

    // made public so it can be used in MQTTProtocolConverter
    public void dispatchPending() throws IOException {
        List<Destination> slowConsumerTargets = null;

        synchronized(pendingLock) {
            try {
                int numberToDispatch = countBeforeFull();
                if (numberToDispatch > 0) {
                    setSlowConsumer(false);
                    setPendingBatchSize(pending, numberToDispatch);
                    int count = 0;
                    pending.reset();
                    while (count < numberToDispatch && !isFull() && pending.hasNext()) {
                        MessageReference node = pending.next();
                        if (node == null) {
                            break;
                        }
                        if (trackedInPendingTransaction(node)) {
                            node.decrementReferenceCount();
                            continue;
                        }

                        // Synchronize between dispatched list and remove of message from pending list
                        // related to remove subscription action
                        synchronized(dispatchLock) {
                            pending.remove();
                            if (!isDropped(node) && canDispatch(node)) {

                                // Message may have been sitting in the pending
                                // list a while waiting for the consumer to ak the message.
                                if (node != QueueMessageReference.NULL_MESSAGE && node.isExpired()) {
                                    //increment number to dispatch
                                    numberToDispatch++;
                                    if (broker.isExpired(node)) {
                                        ((Destination)node.getRegionDestination()).messageExpired(context, this, node);
                                    }

                                    if (!isBrowser()) {
                                        node.decrementReferenceCount();
                                        continue;
                                    }
                                }
                                dispatch(node);
                                count++;
                            }
                        }
                        // decrement after dispatch has taken ownership to avoid usage jitter
                        node.decrementReferenceCount();
                    }
                } else if (!pending.isEmpty() && !isSlowConsumer()) {
                    setSlowConsumer(true);
                    slowConsumerTargets = destinations;
                }
            } finally {
                pending.release();
            }
        }

        if (slowConsumerTargets != null) {
            for (Destination dest : slowConsumerTargets) {
                dest.slowConsumer(context, this);
            }
        }
    }

    protected boolean trackedInPendingTransaction(MessageReference node) {
        return false;
    }

    protected void setPendingBatchSize(PendingMessageCursor pending, int numberToDispatch) {
        pending.setMaxBatchSize(numberToDispatch);
    }

    // called with dispatchLock held
    protected boolean dispatch(final MessageReference node) throws IOException {
        final Message message = node.getMessage();
        if (message == null) {
            return false;
        }

        okForAckAsDispatchDone.countDown();

        MessageDispatch md = createMessageDispatch(node, message);
        if (node != QueueMessageReference.NULL_MESSAGE) {
            dispatched.add(node);
            getSubscriptionStatistics().getDispatched().increment();
        }
        if (getPrefetchSize() == 0) {
            while (true) {
                int currentExtension = prefetchExtension.get();
                int newExtension = Math.max(0, currentExtension - 1);
                if (prefetchExtension.compareAndSet(currentExtension, newExtension)) {
                    break;
                }
            }
        }
        if (info.isDispatchAsync()) {
            md.setTransmitCallback(new TransmitCallback() {

                @Override
                public void onSuccess() {
                    // Since the message gets queued up in async dispatch, we don't want to
                    // decrease the reference count until it gets put on the wire.
                    onDispatch(node, message);
                }

                @Override
                public void onFailure() {
                    Destination nodeDest = (Destination) node.getRegionDestination();
                    if (nodeDest != null) {
                        if (node != QueueMessageReference.NULL_MESSAGE) {
                            nodeDest.getDestinationStatistics().getDispatched().increment();
                            incrementPrefetchCounter(node);
                            LOG.trace("{} failed to dispatch: {} - {}, dispatched: {}, inflight: {}",
                                    info.getConsumerId(), message.getMessageId(), message.getDestination(),
                                    getSubscriptionStatistics().getDispatched().getCount(), dispatched.size());
                        }
                    }
                    if (node instanceof QueueMessageReference) {
                        ((QueueMessageReference) node).unlock();
                    }
                }
            });
            context.getConnection().dispatchAsync(md);
        } else {
            context.getConnection().dispatchSync(md);
            onDispatch(node, message);
        }
        return true;
    }

    protected void onDispatch(final MessageReference node, final Message message) {
        Destination nodeDest = (Destination) node.getRegionDestination();
        if (nodeDest != null) {
            if (node != QueueMessageReference.NULL_MESSAGE) {
                nodeDest.getDestinationStatistics().getDispatched().increment();
                incrementPrefetchCounter(node);
                LOG.trace("{} dispatched: {} - {}, dispatched: {}, inflight: {}",
                        info.getConsumerId(), message.getMessageId(), message.getDestination(),
                        getSubscriptionStatistics().getDispatched().getCount(), dispatched.size());
            }
        }

        if (info.isDispatchAsync()) {
            try {
                dispatchPending();
            } catch (IOException e) {
                context.getConnection().serviceExceptionAsync(e);
            }
        }
    }

    /**
     * inform the MessageConsumer on the client to change it's prefetch
     *
     * @param newPrefetch
     */
    @Override
    public void updateConsumerPrefetch(int newPrefetch) {
        if (context != null && context.getConnection() != null && context.getConnection().isManageable()) {
            ConsumerControl cc = new ConsumerControl();
            cc.setConsumerId(info.getConsumerId());
            cc.setPrefetch(newPrefetch);
            context.getConnection().dispatchAsync(cc);
        }
    }

    /**
     * @param node
     * @param message
     * @return MessageDispatch
     */
    protected MessageDispatch createMessageDispatch(MessageReference node, Message message) {
        MessageDispatch md = new MessageDispatch();
        md.setConsumerId(info.getConsumerId());

        if (node == QueueMessageReference.NULL_MESSAGE) {
            md.setMessage(null);
            md.setDestination(null);
        } else {
            Destination regionDestination = (Destination) node.getRegionDestination();
            md.setDestination(regionDestination.getActiveMQDestination());
            md.setMessage(message);
            md.setRedeliveryCounter(node.getRedeliveryCounter());
        }

        return md;
    }

    /**
     * Use when a matched message is about to be dispatched to the client.
     *
     * @param node
     * @return false if the message should not be dispatched to the client
     *         (another sub may have already dispatched it for example).
     * @throws IOException
     */
    protected abstract boolean canDispatch(MessageReference node) throws IOException;

    protected abstract boolean isDropped(MessageReference node);

    /**
     * Used during acknowledgment to remove the message.
     *
     * @throws IOException
     */
    protected abstract void acknowledge(ConnectionContext context, final MessageAck ack, final MessageReference node) throws IOException;


    public int getMaxProducersToAudit() {
        return maxProducersToAudit;
    }

    public void setMaxProducersToAudit(int maxProducersToAudit) {
        this.maxProducersToAudit = maxProducersToAudit;
        if (this.pending != null) {
            this.pending.setMaxProducersToAudit(maxProducersToAudit);
        }
    }

    public int getMaxAuditDepth() {
        return maxAuditDepth;
    }

    public void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
        if (this.pending != null) {
            this.pending.setMaxAuditDepth(maxAuditDepth);
        }
    }

    @Override
    public void setPrefetchSize(int prefetchSize) {
        this.info.setPrefetchSize(prefetchSize);
        try {
            this.dispatchPending();
        } catch (Exception e) {
            LOG.trace("Caught exception during dispatch after prefetch change.", e);
        }
    }

    private void incrementPrefetchCounter(final MessageReference node) {
        ((Destination)node.getRegionDestination()).getDestinationStatistics().getInflight().increment();
        getSubscriptionStatistics().getInflightMessageSize().addSize(node.getSize());
    }

    private void decrementPrefetchCounter(final MessageReference node) {
        ((Destination)node.getRegionDestination()).getDestinationStatistics().getInflight().decrement();
        getSubscriptionStatistics().getInflightMessageSize().addSize(-node.getSize());
    }
}
