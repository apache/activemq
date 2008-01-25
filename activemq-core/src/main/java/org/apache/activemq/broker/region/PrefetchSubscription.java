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

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQMessageAudit;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A subscription that honors the pre-fetch option of the ConsumerInfo.
 * 
 * @version $Revision: 1.15 $
 */
public abstract class PrefetchSubscription extends AbstractSubscription {

    private static final Log LOG = LogFactory.getLog(PrefetchSubscription.class);
    protected PendingMessageCursor pending;
    protected final List<MessageReference> dispatched = new CopyOnWriteArrayList<MessageReference>();
    protected int prefetchExtension;
    protected long enqueueCounter;
    protected long dispatchCounter;
    protected long dequeueCounter;
    protected boolean optimizedDispatch=false;
    private int maxProducersToAudit=32;
    private int maxAuditDepth=2048;
    protected final SystemUsage usageManager;
    protected ActiveMQMessageAudit audit = new ActiveMQMessageAudit();
    private final Object pendingLock = new Object();
    private final Object dispatchLock = new Object();

    public PrefetchSubscription(Broker broker, SystemUsage usageManager, ConnectionContext context, ConsumerInfo info, PendingMessageCursor cursor) throws InvalidSelectorException {
        super(broker, context, info);
        this.usageManager=usageManager;
        pending = cursor;
    }

    public PrefetchSubscription(Broker broker, SystemUsage usageManager, ConnectionContext context, ConsumerInfo info) throws InvalidSelectorException {
        this(broker,usageManager,context, info, new VMPendingMessageCursor());
    }

    /**
     * Allows a message to be pulled on demand by a client
     */
    public synchronized Response pullMessage(ConnectionContext context, MessagePull pull) throws Exception {
        // The slave should not deliver pull messages. TODO: when the slave
        // becomes a master,
        // He should send a NULL message to all the consumers to 'wake them up'
        // in case
        // they were waiting for a message.
        if (getPrefetchSize() == 0 && !isSlave()) {
            prefetchExtension++;
            final long dispatchCounterBeforePull = dispatchCounter;
            dispatchPending();
            // If there was nothing dispatched.. we may need to setup a timeout.
            if (dispatchCounterBeforePull == dispatchCounter) {
                // imediate timeout used by receiveNoWait()
                if (pull.getTimeout() == -1) {
                    // Send a NULL message.
                    add(QueueMessageReference.NULL_MESSAGE);
                    dispatchPending();
                }
                if (pull.getTimeout() > 0) {
                    Scheduler.executeAfterDelay(new Runnable() {

                        public void run() {
                            pullTimeout(dispatchCounterBeforePull);
                        }
                    }, pull.getTimeout());
                }
            }
        }
        return null;
    }

    /**
     * Occurs when a pull times out. If nothing has been dispatched since the
     * timeout was setup, then send the NULL message.
     */
    final synchronized void pullTimeout(long dispatchCounterBeforePull) {
        if (dispatchCounterBeforePull == dispatchCounter) {
            try {
                add(QueueMessageReference.NULL_MESSAGE);
                dispatchPending();
            } catch (Exception e) {
                context.getConnection().serviceException(e);
            }
        }
    }

    public void add(MessageReference node) throws Exception {
		boolean pendingEmpty = false;
		boolean dispatchPending = false;
		synchronized (pendingLock) {
			pendingEmpty = pending.isEmpty();
			enqueueCounter++;
			if (optimizedDispatch && !isFull() && pendingEmpty && !isSlave()) {
				pending.dispatched(node);
				dispatch(node);
			} else {
				optimizePrefetch();
				synchronized (pendingLock) {
					if (pending.isEmpty() && LOG.isDebugEnabled()) {
						LOG.debug("Prefetch limit.");
					}
					pending.addMessageLast(node);
					dispatchPending = true;
				}
			}
		}
		if (dispatchPending) {
			dispatchPending();
		}
	}

    public void processMessageDispatchNotification(MessageDispatchNotification mdn) throws Exception {
        synchronized(pendingLock) {
            try {
                pending.reset();
                while (pending.hasNext()) {
                    MessageReference node = pending.next();
                    if (node.getMessageId().equals(mdn.getMessageId())) {
                        pending.remove();
                        createMessageDispatch(node, node.getMessage());
                        synchronized(dispatchLock) {
                            dispatched.add(node);
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
                        + mdn.getMessageId() + ") was not in the pending list");
    }

    public  void acknowledge(final ConnectionContext context,final MessageAck ack) throws Exception {
        // Handle the standard acknowledgment case.
        boolean callDispatchMatched = false;
        synchronized(dispatchLock) {
            if (ack.isStandardAck()) {
                // Acknowledge all dispatched messages up till the message id of
                // the
                // acknowledgment.
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
                            node.getRegionDestination()
                                    .getDestinationStatistics().getDequeues()
                                    .increment();
                            removeList.add(node);
                        } else {
                            // setup a Synchronization to remove nodes from the
                            // dispatched list.
                            context.getTransaction().addSynchronization(
                                    new Synchronization() {

                                        public void afterCommit()
                                                throws Exception {
                                            synchronized(dispatchLock) {
                                            
                                                dequeueCounter++;
                                                dispatched.remove(node);
                                                node
                                                        .getRegionDestination()
                                                        .getDestinationStatistics()
                                                        .getDequeues()
                                                        .increment();
                                                prefetchExtension--;
                                            }
                                        }

                                        public void afterRollback()
                                                throws Exception {
                                            super.afterRollback();
                                        }
                                    });
                        }
                        index++;
                        acknowledge(context, ack, node);
                        if (ack.getLastMessageId().equals(messageId)) {
                            if (context.isInTransaction()) {
                                // extend prefetch window only if not a pulling
                                // consumer
                                if (getPrefetchSize() != 0) {
                                    prefetchExtension = Math.max(
                                            prefetchExtension, index + 1);
                                }
                            } else {
                                prefetchExtension = Math.max(0,
                                        prefetchExtension - (index + 1));
                            }
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
                    if (LOG.isDebugEnabled()) {
                        LOG
                                .debug("Could not correlate acknowledgment with dispatched message: "
                                        + ack);
                    }
                }
            } else if (ack.isDeliveredAck()) {
                // Message was delivered but not acknowledged: update pre-fetch
                // counters.
                // Acknowledge all dispatched messages up till the message id of
                // the
                // acknowledgment.
                int index = 0;
                for (Iterator<MessageReference> iter = dispatched.iterator(); iter
                        .hasNext(); index++) {
                    final MessageReference node = iter.next();
                    if (ack.getLastMessageId().equals(node.getMessageId())) {
                        prefetchExtension = Math.max(prefetchExtension,
                                index + 1);
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
                // a
                // DLQ message.
                // Acknowledge all dispatched messages up till the message id of
                // the
                // acknowledgment.
                boolean inAckRange = false;
                for (final MessageReference node : dispatched) {
                    MessageId messageId = node.getMessageId();
                    if (ack.getFirstMessageId() == null
                            || ack.getFirstMessageId().equals(messageId)) {
                        inAckRange = true;
                    }
                    if (inAckRange) {
                        node.incrementRedeliveryCounter();
                        if (ack.getLastMessageId().equals(messageId)) {
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
                // Acknowledge all dispatched messages up till the message id of
                // the
                // acknowledgment.
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
                                .getDequeues().increment();
                        removeList.add(node);
                        dequeueCounter++;
                        index++;
                        acknowledge(context, ack, node);
                        if (ack.getLastMessageId().equals(messageId)) {
                            prefetchExtension = Math.max(0, prefetchExtension
                                    - (index + 1));
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
        if (callDispatchMatched) {
            dispatchPending();
        } else {
            if (isSlave()) {
                throw new JMSException(
                        "Slave broker out of sync with master: Acknowledgment ("
                                + ack + ") was not in the dispatch list: "
                                + dispatched);
            } else {
                LOG
                        .debug("Acknowledgment out of sync (Normally occurs when failover connection reconnects): "
                                + ack);
            }
        }
    }

    /**
     * @param context
     * @param node
     * @throws IOException
     * @throws Exception
     */
    protected void sendToDLQ(final ConnectionContext context, final MessageReference node) throws IOException, Exception {
        broker.sendToDeadLetterQueue(context, node);
    }

    /**
     * Used to determine if the broker can dispatch to the consumer.
     * 
     * @return
     */
    protected boolean isFull() {
        return isSlave() || dispatched.size() - prefetchExtension >= info.getPrefetchSize();
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
        }
    }

    /**
     * optimize message consumer prefetch if the consumer supports it
     */
    public void optimizePrefetch() {
        /*
         * if(info!=null&&info.isOptimizedAcknowledge()&&context!=null&&context.getConnection()!=null
         * &&context.getConnection().isManageable()){
         * if(info.getCurrentPrefetchSize()!=info.getPrefetchSize() &&
         * isLowWaterMark()){
         * info.setCurrentPrefetchSize(info.getPrefetchSize());
         * updateConsumerPrefetch(info.getPrefetchSize()); }else
         * if(info.getCurrentPrefetchSize()==info.getPrefetchSize() &&
         * isHighWaterMark()){ // want to purge any outstanding acks held by the
         * consumer info.setCurrentPrefetchSize(1); updateConsumerPrefetch(1); } }
         */
    }

    public void add(ConnectionContext context, Destination destination) throws Exception {
        synchronized(pendingLock) {
            super.add(context, destination);
            pending.add(context, destination);
        }
    }

    public void remove(ConnectionContext context, Destination destination) throws Exception {
        synchronized(pendingLock) {
            super.remove(context, destination);
            pending.remove(context, destination);
        }
    }

    protected void dispatchPending() throws IOException {
        if (!isSlave()) {
           synchronized(pendingLock) {
                try {
                    int numberToDispatch = countBeforeFull();
                    if (numberToDispatch > 0) {
                        pending.setMaxBatchSize(numberToDispatch);
                        int count = 0;
                        pending.reset();
                        while (pending.hasNext() && !isFull()
                                && count < numberToDispatch) {
                            MessageReference node = pending.next();
                            if (node == null) {
                                break;
                            }
                            if (canDispatch(node)) {
                                pending.remove();
                                // Message may have been sitting in the pending
                                // list
                                // a while
                                // waiting for the consumer to ak the message.
                                if (node != QueueMessageReference.NULL_MESSAGE
                                        && broker.isExpired(node)) {
                                    broker.messageExpired(getContext(), node);
                                    dequeueCounter++;
                                    continue;
                                }
                                dispatch(node);
                                count++;
                            }
                        }
                    }
                } finally {
                    pending.release();
                }
            }
        }
    }

    protected boolean dispatch(final MessageReference node) throws IOException {
        final Message message = node.getMessage();
        if (message == null) {
            return false;
        }         
        // Make sure we can dispatch a message.
        if (canDispatch(node) && !isSlave()) {
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

    /**
     * Used during acknowledgment to remove the message.
     * 
     * @throws IOException
     */
    protected void acknowledge(ConnectionContext context, final MessageAck ack, final MessageReference node) throws IOException {
    }

    public boolean isOptimizedDispatch() {
        return optimizedDispatch;
    }

    public void setOptimizedDispatch(boolean optimizedDispatch) {
        this.optimizedDispatch = optimizedDispatch;
    }

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

}
