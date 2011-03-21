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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.InvalidSelectorException;
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
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A subscription that honors the pre-fetch option of the ConsumerInfo.
 * 
 * 
 */
public abstract class PrefetchSubscription extends AbstractSubscription {

    private static final Logger LOG = LoggerFactory.getLogger(PrefetchSubscription.class);
    protected final Scheduler scheduler;
    
    protected PendingMessageCursor pending;
    protected final List<MessageReference> dispatched = new CopyOnWriteArrayList<MessageReference>();
    protected int prefetchExtension;
    protected boolean usePrefetchExtension = true;
    protected long enqueueCounter;
    protected long dispatchCounter;
    protected long dequeueCounter;
    private int maxProducersToAudit=32;
    private int maxAuditDepth=2048;
    protected final SystemUsage usageManager;
    protected final Object pendingLock = new Object();
    private final Object dispatchLock = new Object();
    private final CountDownLatch okForAckAsDispatchDone = new CountDownLatch(1);
    
    public PrefetchSubscription(Broker broker, SystemUsage usageManager, ConnectionContext context, ConsumerInfo info, PendingMessageCursor cursor) throws InvalidSelectorException {
        super(broker,context, info);
        this.usageManager=usageManager;
        pending = cursor;
        this.scheduler = broker.getScheduler();
    }

    public PrefetchSubscription(Broker broker,SystemUsage usageManager, ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        this(broker,usageManager,context, info, new VMPendingMessageCursor(false));
    }

    /**
     * Allows a message to be pulled on demand by a client
     */
    public Response pullMessage(ConnectionContext context, MessagePull pull) throws Exception {
        // The slave should not deliver pull messages. TODO: when the slave
        // becomes a master,
        // He should send a NULL message to all the consumers to 'wake them up'
        // in case
        // they were waiting for a message.
        if (getPrefetchSize() == 0 && !isSlave()) {
            final long dispatchCounterBeforePull;
        	synchronized(this) {
        		prefetchExtension++;
        		dispatchCounterBeforePull = dispatchCounter;
        	}
            
        	// Have the destination push us some messages.
        	for (Destination dest : destinations) {
				dest.iterate();
			}
        	dispatchPending();
            
            synchronized(this) {
	            // If there was nothing dispatched.. we may need to setup a timeout.
	            if (dispatchCounterBeforePull == dispatchCounter) {
	                // immediate timeout used by receiveNoWait()
	                if (pull.getTimeout() == -1) {
	                    // Send a NULL message.
	                    add(QueueMessageReference.NULL_MESSAGE);
	                    dispatchPending();
	                }
	                if (pull.getTimeout() > 0) {
	                    scheduler.executeAfterDelay(new Runnable() {
	
	                        public void run() {
	                            pullTimeout(dispatchCounterBeforePull);
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
    final void pullTimeout(long dispatchCounterBeforePull) {
    	synchronized (pendingLock) {
    		if (dispatchCounterBeforePull == dispatchCounter) {
                try {
                    add(QueueMessageReference.NULL_MESSAGE);
                    dispatchPending();
                } catch (Exception e) {
                    context.getConnection().serviceException(e);
                }
            }
        }
    }

    public void add(MessageReference node) throws Exception {
        synchronized (pendingLock) {
            // The destination may have just been removed...  
            if( !destinations.contains(node.getRegionDestination()) && node!=QueueMessageReference.NULL_MESSAGE) {
                // perhaps we should inform the caller that we are no longer valid to dispatch to?
                return;
            }
            enqueueCounter++;
            pending.addMessageLast(node);    
        }
        dispatchPending();
    }

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

    public final void acknowledge(final ConnectionContext context,final MessageAck ack) throws Exception {
        // Handle the standard acknowledgment case.
        boolean callDispatchMatched = false;
        Destination destination = null;
        
        if (!isSlave()) {
            if (!okForAckAsDispatchDone.await(0l, TimeUnit.MILLISECONDS)) {
                // suppress unexpected ack exception in this expected case
                LOG.warn("Ignoring ack received before dispatch; result of failover with an outstanding ack. Acked messages will be replayed if present on this broker. Ignored ack: " + ack);
                return;
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ack:" + ack);
        }
        synchronized(dispatchLock) {
            if (ack.isStandardAck()) {
            	// First check if the ack matches the dispatched. When using failover this might
            	// not be the case. We don't ever want to ack the wrong messages.
            	assertAckMatchesDispatched(ack);
            	
                // Acknowledge all dispatched messages up till the message id of
                // the acknowledgment.
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
                        // Don't remove the nodes until we are committed.  
                        if (!context.isInTransaction()) {
                            dequeueCounter++;
                            node.getRegionDestination().getDestinationStatistics().getInflight().decrement();
                            removeList.add(node);
                        } else {
                            // setup a Synchronization to remove nodes from the
                            // dispatched list.
                            context.getTransaction().addSynchronization(
                                    new Synchronization() {

                                        @Override
                                        public void afterCommit()
                                                throws Exception {
                                            synchronized(dispatchLock) {
                                                dequeueCounter++;
                                                dispatched.remove(node);
                                                node.getRegionDestination().getDestinationStatistics().getInflight().decrement();
                                            }
                                        }

                                        @Override
                                        public void afterRollback() throws Exception {
                                            synchronized(dispatchLock) {
                                                if (isSlave()) {
                                                    node.getRegionDestination().getDestinationStatistics().getInflight().decrement();
                                                } else {
                                                    // poisionAck will decrement - otherwise still inflight on client
                                                }
                                            }
                                        }
                                    });
                        }
                        index++;
                        acknowledge(context, ack, node);
                        if (ack.getLastMessageId().equals(messageId)) {                  
                            // contract prefetch if dispatch required a pull
                            if (getPrefetchSize() == 0) {
                                prefetchExtension = Math.max(0, prefetchExtension - index);
                            } else if (usePrefetchExtension && context.isInTransaction()) {
                                // extend prefetch window only if not a pulling consumer
                                prefetchExtension = Math.max(prefetchExtension, index);
                            }
                            destination = node.getRegionDestination();
                            callDispatchMatched = true;
                            break;
                        }
                    }
                }
                for (final MessageReference node : removeList) {
                    dispatched.remove(node);
                }
                // this only happens after a reconnect - get an ack which is not
                // valid
                if (!callDispatchMatched) {
                    LOG.warn("Could not correlate acknowledgment with dispatched message: "
                                  + ack);
                }
            } else if (ack.isIndividualAck()) {
                // Message was delivered and acknowledge - but only delete the
                // individual message
                for (final MessageReference node : dispatched) {
                    MessageId messageId = node.getMessageId();
                    if (ack.getLastMessageId().equals(messageId)) {
                        // this should never be within a transaction
                        dequeueCounter++;
                        node.getRegionDestination().getDestinationStatistics().getInflight().decrement();
                        destination = node.getRegionDestination();
                        acknowledge(context, ack, node);
                        dispatched.remove(node);
                        prefetchExtension = Math.max(0, prefetchExtension - 1);
                        callDispatchMatched = true;
                        break;
                    }
                }
            }else if (ack.isDeliveredAck()) {
                // Message was delivered but not acknowledged: update pre-fetch
                // counters.
                int index = 0;
                for (Iterator<MessageReference> iter = dispatched.iterator(); iter.hasNext(); index++) {
                    final MessageReference node = iter.next();
                    if (node.isExpired()) {
                        if (broker.isExpired(node)) {
                            node.getRegionDestination().messageExpired(context, this, node);
                        }
                        dispatched.remove(node);
                        node.getRegionDestination().getDestinationStatistics().getInflight().decrement();
                    }
                    if (ack.getLastMessageId().equals(node.getMessageId())) {
                        if (usePrefetchExtension) {
                            prefetchExtension = Math.max(prefetchExtension, index + 1);
                        }
                        destination = node.getRegionDestination();
                        callDispatchMatched = true;
                        break;
                    }
                }
                if (!callDispatchMatched) {
                    throw new JMSException(
                            "Could not correlate acknowledgment with dispatched message: "
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
                            destination = node.getRegionDestination();
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
                        sendToDLQ(context, node);
                        node.getRegionDestination().getDestinationStatistics()
                                .getInflight().decrement();
                        removeList.add(node);
                        dequeueCounter++;
                        index++;
                        acknowledge(context, ack, node);
                        if (ack.getLastMessageId().equals(messageId)) {
                            prefetchExtension = Math.max(0, prefetchExtension
                                    - (index + 1));
                            destination = node.getRegionDestination();
                            callDispatchMatched = true;
                            break;
                        }
                    }
                }
                for (final MessageReference node : removeList) {
                    dispatched.remove(node);
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
        } else {
            if (isSlave()) {
                throw new JMSException(
                        "Slave broker out of sync with master: Acknowledgment ("
                                + ack + ") was not in the dispatch list: "
                                + dispatched);
            } else {
                LOG.debug("Acknowledgment out of sync (Normally occurs when failover connection reconnects): "
                        + ack);
            }
        }
    }

    /**
     * Checks an ack versus the contents of the dispatched list.
     * 
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
     * @param context
     * @param node
     * @throws IOException
     * @throws Exception
     */
    protected void sendToDLQ(final ConnectionContext context, final MessageReference node) throws IOException, Exception {
        broker.getRoot().sendToDeadLetterQueue(context, node, this);
    }
    
    public int getInFlightSize() {
        return dispatched.size();
    }
    
    /**
     * Used to determine if the broker can dispatch to the consumer.
     * 
     * @return
     */
    public boolean isFull() {
        return dispatched.size() - prefetchExtension >= info.getPrefetchSize();
    }

    /**
     * @return true when 60% or more room is left for dispatching messages
     */
    public boolean isLowWaterMark() {
        return (dispatched.size() - prefetchExtension) <= (info.getPrefetchSize() * .4);
    }

    /**
     * @return true when 10% or less room is left for dispatching messages
     */
    public boolean isHighWaterMark() {
        return (dispatched.size() - prefetchExtension) >= (info.getPrefetchSize() * .9);
    }

    @Override
    public int countBeforeFull() {
        return info.getPrefetchSize() + prefetchExtension - dispatched.size();
    }

    public int getPendingQueueSize() {
        return pending.size();
    }

    public int getDispatchedQueueSize() {
        return dispatched.size();
    }

    public long getDequeueCounter() {
        return dequeueCounter;
    }

    public long getDispatchedCounter() {
        return dispatchCounter;
    }

    public long getEnqueueCounter() {
        return enqueueCounter;
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
        List<MessageReference> rc = new ArrayList<MessageReference>();
        synchronized(pendingLock) {
            super.remove(context, destination);
            // Here is a potential problem concerning Inflight stat:
            // Messages not already committed or rolled back may not be removed from dispatched list at the moment
            // Except if each commit or rollback callback action comes before remove of subscriber.
            rc.addAll(pending.remove(context, destination));

            // Synchronized to DispatchLock
            synchronized(dispatchLock) {
                ArrayList<MessageReference> references = new ArrayList<MessageReference>();
	            for (MessageReference r : dispatched) {
	                if( r.getRegionDestination() == destination) {
                        references.add(r);
	                }
	            }
                rc.addAll(references);
                destination.getDestinationStatistics().getDispatched().subtract(references.size());
                destination.getDestinationStatistics().getInflight().subtract(references.size());
                dispatched.removeAll(references);
            }            
        }
        return rc;
    }

    protected void dispatchPending() throws IOException {
        if (!isSlave()) {
           synchronized(pendingLock) {
                try {
                    int numberToDispatch = countBeforeFull();
                    if (numberToDispatch > 0) {
                        setSlowConsumer(false);
                        setPendingBatchSize(pending, numberToDispatch);
                        int count = 0;
                        pending.reset();
                        while (pending.hasNext() && !isFull()
                                && count < numberToDispatch) {
                            MessageReference node = pending.next();
                            if (node == null) {
                                break;
                            }
                            
                            // Synchronize between dispatched list and remove of message from pending list
                            // related to remove subscription action
                            synchronized(dispatchLock) {
                                pending.remove();
                                node.decrementReferenceCount();
                                if( !isDropped(node) && canDispatch(node)) {

                                    // Message may have been sitting in the pending
                                    // list a while waiting for the consumer to ak the message.
                                    if (node!=QueueMessageReference.NULL_MESSAGE && node.isExpired()) {
                                        //increment number to dispatch
                                        numberToDispatch++;
                                        if (broker.isExpired(node)) {
                                            node.getRegionDestination().messageExpired(context, this, node);
                                        }
                                        continue;
                                    }
                                    dispatch(node);
                                    count++;
                                }
                            }
                        }
                    } else if (!isSlowConsumer()) {
                        setSlowConsumer(true);
                        for (Destination dest :destinations) {
                            dest.slowConsumer(context, this);
                        }
                    }
                } finally {
                    pending.release();
                }
            }
        }
    }

    protected void setPendingBatchSize(PendingMessageCursor pending, int numberToDispatch) {
        pending.setMaxBatchSize(numberToDispatch);
    }

    protected boolean dispatch(final MessageReference node) throws IOException {
        final Message message = node.getMessage();
        if (message == null) {
            return false;
        }
        
        okForAckAsDispatchDone.countDown();
        
        // No reentrant lock - Patch needed to IndirectMessageReference on method lock
        if (!isSlave()) {

            MessageDispatch md = createMessageDispatch(node, message);
            // NULL messages don't count... they don't get Acked.
            if (node != QueueMessageReference.NULL_MESSAGE) {
                dispatchCounter++;
                dispatched.add(node);
            } else {
                prefetchExtension = Math.max(0, prefetchExtension - 1);
            }
            if (info.isDispatchAsync()) {
                md.setTransmitCallback(new Runnable() {

                    public void run() {
                        // Since the message gets queued up in async dispatch,
                        // we don't want to
                        // decrease the reference count until it gets put on the
                        // wire.
                        onDispatch(node, message);
                    }
                });
                context.getConnection().dispatchAsync(md);
            } else {
                context.getConnection().dispatchSync(md);
                onDispatch(node, message);
            }
            return true;
        } else {
            return false;
        }
    }

    protected void onDispatch(final MessageReference node, final Message message) {
        if (node.getRegionDestination() != null) {
            if (node != QueueMessageReference.NULL_MESSAGE) {
                node.getRegionDestination().getDestinationStatistics().getDispatched().increment();
                node.getRegionDestination().getDestinationStatistics().getInflight().increment();   
                if (LOG.isTraceEnabled()) {
                    LOG.trace(info.getConsumerId() + " dispatched: " + message.getMessageId() + " - "
                            + message.getDestination()  + ", dispatched: " + dispatchCounter + ", inflight: " + dispatched.size());
                }
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
        if (node == QueueMessageReference.NULL_MESSAGE) {
            MessageDispatch md = new MessageDispatch();
            md.setMessage(null);
            md.setConsumerId(info.getConsumerId());
            md.setDestination(null);
            return md;
        } else {
            MessageDispatch md = new MessageDispatch();
            md.setConsumerId(info.getConsumerId());
            md.setDestination(node.getRegionDestination().getActiveMQDestination());
            md.setMessage(message);
            md.setRedeliveryCounter(node.getRedeliveryCounter());
            return md;
        }
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
    }

    public int getMaxAuditDepth() {
        return maxAuditDepth;
    }

    public void setMaxAuditDepth(int maxAuditDepth) {
        this.maxAuditDepth = maxAuditDepth;
    }
    
    public boolean isUsePrefetchExtension() {
        return usePrefetchExtension;
    }

    public void setUsePrefetchExtension(boolean usePrefetchExtension) {
        this.usePrefetchExtension = usePrefetchExtension;
    }
}
