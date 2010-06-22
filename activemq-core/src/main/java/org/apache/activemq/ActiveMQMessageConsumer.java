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
package org.apache.activemq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TransactionRolledBackException;
import org.apache.activemq.blob.BlobDownloader;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.management.JMSConsumerStatsImpl;
import org.apache.activemq.management.StatsCapable;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.Callback;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A client uses a <CODE>MessageConsumer</CODE> object to receive messages
 * from a destination. A <CODE> MessageConsumer</CODE> object is created by
 * passing a <CODE>Destination</CODE> object to a message-consumer creation
 * method supplied by a session.
 * <P>
 * <CODE>MessageConsumer</CODE> is the parent interface for all message
 * consumers.
 * <P>
 * A message consumer can be created with a message selector. A message selector
 * allows the client to restrict the messages delivered to the message consumer
 * to those that match the selector.
 * <P>
 * A client may either synchronously receive a message consumer's messages or
 * have the consumer asynchronously deliver them as they arrive.
 * <P>
 * For synchronous receipt, a client can request the next message from a message
 * consumer using one of its <CODE> receive</CODE> methods. There are several
 * variations of <CODE>receive</CODE> that allow a client to poll or wait for
 * the next message.
 * <P>
 * For asynchronous delivery, a client can register a
 * <CODE>MessageListener</CODE> object with a message consumer. As messages
 * arrive at the message consumer, it delivers them by calling the
 * <CODE>MessageListener</CODE>'s<CODE>
 * onMessage</CODE> method.
 * <P>
 * It is a client programming error for a <CODE>MessageListener</CODE> to
 * throw an exception.
 * 
 * @version $Revision: 1.22 $
 * @see javax.jms.MessageConsumer
 * @see javax.jms.QueueReceiver
 * @see javax.jms.TopicSubscriber
 * @see javax.jms.Session
 */
public class ActiveMQMessageConsumer implements MessageAvailableConsumer, StatsCapable, ActiveMQDispatcher {

    @SuppressWarnings("serial")
    class PreviouslyDeliveredMap<K, V> extends HashMap<K, V> {
        final TransactionId transactionId;
        public PreviouslyDeliveredMap(TransactionId transactionId) {
            this.transactionId = transactionId;
        }
    }

    private static final Log LOG = LogFactory.getLog(ActiveMQMessageConsumer.class);
    protected final Scheduler scheduler;
    protected final ActiveMQSession session;
    protected final ConsumerInfo info;

    // These are the messages waiting to be delivered to the client
    protected final MessageDispatchChannel unconsumedMessages;

    // The are the messages that were delivered to the consumer but that have
    // not been acknowledged. It's kept in reverse order since we
    // Always walk list in reverse order.
    private final LinkedList<MessageDispatch> deliveredMessages = new LinkedList<MessageDispatch>();
    // track duplicate deliveries in a transaction such that the tx integrity can be validated
    private PreviouslyDeliveredMap<MessageId, Boolean> previouslyDeliveredMessages;
    private int deliveredCounter;
    private int additionalWindowSize;
    private long redeliveryDelay;
    private int ackCounter;
    private int dispatchedCount;
    private final AtomicReference<MessageListener> messageListener = new AtomicReference<MessageListener>();
    private final JMSConsumerStatsImpl stats;

    private final String selector;
    private boolean synchronizationRegistered;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private MessageAvailableListener availableListener;

    private RedeliveryPolicy redeliveryPolicy;
    private boolean optimizeAcknowledge;
    private final AtomicBoolean deliveryingAcknowledgements = new AtomicBoolean();
    private ExecutorService executorService;
    private MessageTransformer transformer;
    private boolean clearDispatchList;
    boolean inProgressClearRequiredFlag;

    private MessageAck pendingAck;
    private long lastDeliveredSequenceId;

    private IOException failureError;
    
    private long optimizeAckTimestamp = System.currentTimeMillis();
    private final long optimizeAckTimeout = 300;
    private long failoverRedeliveryWaitPeriod = 0;

    /**
     * Create a MessageConsumer
     * 
     * @param session
     * @param dest
     * @param name
     * @param selector
     * @param prefetch
     * @param maximumPendingMessageCount
     * @param noLocal
     * @param browser
     * @param dispatchAsync
     * @param messageListener
     * @throws JMSException
     */
    public ActiveMQMessageConsumer(ActiveMQSession session, ConsumerId consumerId, ActiveMQDestination dest,
            String name, String selector, int prefetch,
            int maximumPendingMessageCount, boolean noLocal, boolean browser,
            boolean dispatchAsync, MessageListener messageListener) throws JMSException {
        if (dest == null) {
            throw new InvalidDestinationException("Don't understand null destinations");
        } else if (dest.getPhysicalName() == null) {
            throw new InvalidDestinationException("The destination object was not given a physical name.");
        } else if (dest.isTemporary()) {
            String physicalName = dest.getPhysicalName();

            if (physicalName == null) {
                throw new IllegalArgumentException("Physical name of Destination should be valid: " + dest);
            }

            String connectionID = session.connection.getConnectionInfo().getConnectionId().getValue();

            if (physicalName.indexOf(connectionID) < 0) {
                throw new InvalidDestinationException(
                                                      "Cannot use a Temporary destination from another Connection");
            }

            if (session.connection.isDeleted(dest)) {
                throw new InvalidDestinationException(
                                                      "Cannot use a Temporary destination that has been deleted");
            }
            if (prefetch < 0) {
                throw new JMSException("Cannot have a prefetch size less than zero");
            }
        }
        if (session.connection.isMessagePrioritySupported()) {
            this.unconsumedMessages = new SimplePriorityMessageDispatchChannel();
        }else {
            this.unconsumedMessages = new FifoMessageDispatchChannel();
        }

        this.session = session;
        this.scheduler = session.getScheduler();
        this.redeliveryPolicy = session.connection.getRedeliveryPolicy();
        setTransformer(session.getTransformer());

        this.info = new ConsumerInfo(consumerId);
        this.info.setExclusive(this.session.connection.isExclusiveConsumer());
        this.info.setSubscriptionName(name);
        this.info.setPrefetchSize(prefetch);
        this.info.setCurrentPrefetchSize(prefetch);
        this.info.setMaximumPendingMessageLimit(maximumPendingMessageCount);
        this.info.setNoLocal(noLocal);
        this.info.setDispatchAsync(dispatchAsync);
        this.info.setRetroactive(this.session.connection.isUseRetroactiveConsumer());
        this.info.setSelector(null);

        // Allows the options on the destination to configure the consumerInfo
        if (dest.getOptions() != null) {
            Map<String, String> options = new HashMap<String, String>(dest.getOptions());
            IntrospectionSupport.setProperties(this.info, options, "consumer.");
        }

        this.info.setDestination(dest);
        this.info.setBrowser(browser);
        if (selector != null && selector.trim().length() != 0) {
            // Validate the selector
            SelectorParser.parse(selector);
            this.info.setSelector(selector);
            this.selector = selector;
        } else if (info.getSelector() != null) {
            // Validate the selector
            SelectorParser.parse(this.info.getSelector());
            this.selector = this.info.getSelector();
        } else {
            this.selector = null;
        }

        this.stats = new JMSConsumerStatsImpl(session.getSessionStats(), dest);
        this.optimizeAcknowledge = session.connection.isOptimizeAcknowledge() && session.isAutoAcknowledge()
                                   && !info.isBrowser();
        this.info.setOptimizedAcknowledge(this.optimizeAcknowledge);
        this.failoverRedeliveryWaitPeriod = session.connection.getConsumerFailoverRedeliveryWaitPeriod();
        if (messageListener != null) {
            setMessageListener(messageListener);
        }
        try {
            this.session.addConsumer(this);
            this.session.syncSendPacket(info);
        } catch (JMSException e) {
            this.session.removeConsumer(this);
            throw e;
        }

        if (session.connection.isStarted()) {
            start();
        }
    }

    private boolean isAutoAcknowledgeEach() {
        return session.isAutoAcknowledge() || ( session.isDupsOkAcknowledge() && getDestination().isQueue() );
    }

    private boolean isAutoAcknowledgeBatch() {
        return session.isDupsOkAcknowledge() && !getDestination().isQueue() ;
    }

    public StatsImpl getStats() {
        return stats;
    }

    public JMSConsumerStatsImpl getConsumerStats() {
        return stats;
    }

    public RedeliveryPolicy getRedeliveryPolicy() {
        return redeliveryPolicy;
    }

    /**
     * Sets the redelivery policy used when messages are redelivered
     */
    public void setRedeliveryPolicy(RedeliveryPolicy redeliveryPolicy) {
        this.redeliveryPolicy = redeliveryPolicy;
    }

    public MessageTransformer getTransformer() {
        return transformer;
    }

    /**
     * Sets the transformer used to transform messages before they are sent on
     * to the JMS bus
     */
    public void setTransformer(MessageTransformer transformer) {
        this.transformer = transformer;
    }

    /**
     * @return Returns the value.
     */
    public ConsumerId getConsumerId() {
        return info.getConsumerId();
    }

    /**
     * @return the consumer name - used for durable consumers
     */
    public String getConsumerName() {
        return this.info.getSubscriptionName();
    }

    /**
     * @return true if this consumer does not accept locally produced messages
     */
    protected boolean isNoLocal() {
        return info.isNoLocal();
    }

    /**
     * Retrieve is a browser
     * 
     * @return true if a browser
     */
    protected boolean isBrowser() {
        return info.isBrowser();
    }

    /**
     * @return ActiveMQDestination
     */
    protected ActiveMQDestination getDestination() {
        return info.getDestination();
    }

    /**
     * @return Returns the prefetchNumber.
     */
    public int getPrefetchNumber() {
        return info.getPrefetchSize();
    }

    /**
     * @return true if this is a durable topic subscriber
     */
    public boolean isDurableSubscriber() {
        return info.getSubscriptionName() != null && info.getDestination().isTopic();
    }

    /**
     * Gets this message consumer's message selector expression.
     * 
     * @return this message consumer's message selector, or null if no message
     *         selector exists for the message consumer (that is, if the message
     *         selector was not set or was set to null or the empty string)
     * @throws JMSException if the JMS provider fails to receive the next
     *                 message due to some internal error.
     */
    public String getMessageSelector() throws JMSException {
        checkClosed();
        return selector;
    }

    /**
     * Gets the message consumer's <CODE>MessageListener</CODE>.
     * 
     * @return the listener for the message consumer, or null if no listener is
     *         set
     * @throws JMSException if the JMS provider fails to get the message
     *                 listener due to some internal error.
     * @see javax.jms.MessageConsumer#setMessageListener(javax.jms.MessageListener)
     */
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return this.messageListener.get();
    }

    /**
     * Sets the message consumer's <CODE>MessageListener</CODE>.
     * <P>
     * Setting the message listener to null is the equivalent of unsetting the
     * message listener for the message consumer.
     * <P>
     * The effect of calling <CODE>MessageConsumer.setMessageListener</CODE>
     * while messages are being consumed by an existing listener or the consumer
     * is being used to consume messages synchronously is undefined.
     * 
     * @param listener the listener to which the messages are to be delivered
     * @throws JMSException if the JMS provider fails to receive the next
     *                 message due to some internal error.
     * @see javax.jms.MessageConsumer#getMessageListener
     */
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        if (info.getPrefetchSize() == 0) {
            throw new JMSException(
                                   "Illegal prefetch size of zero. This setting is not supported for asynchronous consumers please set a value of at least 1");
        }
        if (listener != null) {
            boolean wasRunning = session.isRunning();
            if (wasRunning) {
                session.stop();
            }

            this.messageListener.set(listener);
            session.redispatch(this, unconsumedMessages);

            if (wasRunning) {
                session.start();
            }
        } else {
            this.messageListener.set(null);
        }
    }

    public MessageAvailableListener getAvailableListener() {
        return availableListener;
    }

    /**
     * Sets the listener used to notify synchronous consumers that there is a
     * message available so that the {@link MessageConsumer#receiveNoWait()} can
     * be called.
     */
    public void setAvailableListener(MessageAvailableListener availableListener) {
        this.availableListener = availableListener;
    }

    /**
     * Used to get an enqueued message from the unconsumedMessages list. The
     * amount of time this method blocks is based on the timeout value. - if
     * timeout==-1 then it blocks until a message is received. - if timeout==0
     * then it it tries to not block at all, it returns a message if it is
     * available - if timeout>0 then it blocks up to timeout amount of time.
     * Expired messages will consumed by this method.
     * 
     * @throws JMSException
     * @return null if we timeout or if the consumer is closed.
     */
    private MessageDispatch dequeue(long timeout) throws JMSException {
        try {
            long deadline = 0;
            if (timeout > 0) {
                deadline = System.currentTimeMillis() + timeout;
            }
            while (true) {
                MessageDispatch md = unconsumedMessages.dequeue(timeout);
                if (md == null) {
                    if (timeout > 0 && !unconsumedMessages.isClosed()) {
                        timeout = Math.max(deadline - System.currentTimeMillis(), 0);
                    } else {
                    	if (failureError != null) {
                    		throw JMSExceptionSupport.create(failureError);
                    	} else {
                    		return null;
                    	}
                    }
                } else if (md.getMessage() == null) {
                    return null;
                } else if (md.getMessage().isExpired()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(getConsumerId() + " received expired message: " + md);
                    }
                    beforeMessageIsConsumed(md);
                    afterMessageIsConsumed(md, true);
                    if (timeout > 0) {
                        timeout = Math.max(deadline - System.currentTimeMillis(), 0);
                    }
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(getConsumerId() + " received message: " + md);
                    }
                    return md;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw JMSExceptionSupport.create(e);
        }
    }

    /**
     * Receives the next message produced for this message consumer.
     * <P>
     * This call blocks indefinitely until a message is produced or until this
     * message consumer is closed.
     * <P>
     * If this <CODE>receive</CODE> is done within a transaction, the consumer
     * retains the message until the transaction commits.
     * 
     * @return the next message produced for this message consumer, or null if
     *         this message consumer is concurrently closed
     */
    public Message receive() throws JMSException {
        checkClosed();
        checkMessageListener();

        sendPullCommand(0);
        MessageDispatch md = dequeue(-1);
        if (md == null) {
            return null;
        }

        beforeMessageIsConsumed(md);
        afterMessageIsConsumed(md, false);

        return createActiveMQMessage(md);
    }

    /**
     * @param md
     * @return
     */
    private ActiveMQMessage createActiveMQMessage(final MessageDispatch md) throws JMSException {
        ActiveMQMessage m = (ActiveMQMessage)md.getMessage().copy();
        if (m.getDataStructureType()==CommandTypes.ACTIVEMQ_BLOB_MESSAGE) {
        	((ActiveMQBlobMessage)m).setBlobDownloader(new BlobDownloader(session.getBlobTransferPolicy()));
        }
        if (transformer != null) {
            Message transformedMessage = transformer.consumerTransform(session, this, m);
            if (transformedMessage != null) {
                m = ActiveMQMessageTransformation.transformMessage(transformedMessage, session.connection);
            }
        }
        if (session.isClientAcknowledge()) {
            m.setAcknowledgeCallback(new Callback() {
                public void execute() throws Exception {
                    session.checkClosed();
                    session.acknowledge();
                }
            });
        }else if (session.isIndividualAcknowledge()) {
            m.setAcknowledgeCallback(new Callback() {
                public void execute() throws Exception {
                    session.checkClosed();
                    acknowledge(md);
                }
            });
        }
        return m;
    }

    /**
     * Receives the next message that arrives within the specified timeout
     * interval.
     * <P>
     * This call blocks until a message arrives, the timeout expires, or this
     * message consumer is closed. A <CODE>timeout</CODE> of zero never
     * expires, and the call blocks indefinitely.
     * 
     * @param timeout the timeout value (in milliseconds), a time out of zero
     *                never expires.
     * @return the next message produced for this message consumer, or null if
     *         the timeout expires or this message consumer is concurrently
     *         closed
     */
    public Message receive(long timeout) throws JMSException {
        checkClosed();
        checkMessageListener();
        if (timeout == 0) {
            return this.receive();

        }

        sendPullCommand(timeout);
        while (timeout > 0) {

            MessageDispatch md;
            if (info.getPrefetchSize() == 0) {
                md = dequeue(-1); // We let the broker let us know when we timeout.
            } else {
                md = dequeue(timeout);
            }

            if (md == null) {
                return null;
            }

            beforeMessageIsConsumed(md);
            afterMessageIsConsumed(md, false);
            return createActiveMQMessage(md);
        }
        return null;
    }

    /**
     * Receives the next message if one is immediately available.
     * 
     * @return the next message produced for this message consumer, or null if
     *         one is not available
     * @throws JMSException if the JMS provider fails to receive the next
     *                 message due to some internal error.
     */
    public Message receiveNoWait() throws JMSException {
        checkClosed();
        checkMessageListener();
        sendPullCommand(-1);

        MessageDispatch md;
        if (info.getPrefetchSize() == 0) {
            md = dequeue(-1); // We let the broker let us know when we
            // timeout.
        } else {
            md = dequeue(0);
        }

        if (md == null) {
            return null;
        }

        beforeMessageIsConsumed(md);
        afterMessageIsConsumed(md, false);
        return createActiveMQMessage(md);
    }

    /**
     * Closes the message consumer.
     * <P>
     * Since a provider may allocate some resources on behalf of a <CODE>
     * MessageConsumer</CODE>
     * outside the Java virtual machine, clients should close them when they are
     * not needed. Relying on garbage collection to eventually reclaim these
     * resources may not be timely enough.
     * <P>
     * This call blocks until a <CODE>receive</CODE> or message listener in
     * progress has completed. A blocked message consumer <CODE>receive </CODE>
     * call returns null when this message consumer is closed.
     * 
     * @throws JMSException if the JMS provider fails to close the consumer due
     *                 to some internal error.
     */
    public void close() throws JMSException {
        if (!unconsumedMessages.isClosed()) {
            if (session.getTransactionContext().isInTransaction()) {
                session.getTransactionContext().addSynchronization(new Synchronization() {
                    @Override
                    public void afterCommit() throws Exception {
                        doClose();
                    }

                    @Override
                    public void afterRollback() throws Exception {
                        doClose();
                    }
                });
            } else {
                doClose();
            } 
        }
    }

    void doClose() throws JMSException {
        dispose();
        RemoveInfo removeCommand = info.createRemoveCommand();
        LOG.info("remove: " + this.getConsumerId() + ", lasteDeliveredSequenceId:" + lastDeliveredSequenceId);
        removeCommand.setLastDeliveredSequenceId(lastDeliveredSequenceId);
        this.session.asyncSendPacket(removeCommand);
    }
    
    void inProgressClearRequired() {
        inProgressClearRequiredFlag = true;
        // deal with delivered messages async to avoid lock contention with in progress acks
        clearDispatchList = true;
    }
    
    void clearMessagesInProgress() {
        if (inProgressClearRequiredFlag) {
            synchronized (unconsumedMessages.getMutex()) {
                if (inProgressClearRequiredFlag) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(getConsumerId() + " clearing dispatched list (" + unconsumedMessages.size() + ") on transport interrupt");
                    }
                    // ensure unconsumed are rolledback up front as they may get redelivered to another consumer
                    List<MessageDispatch> list = unconsumedMessages.removeAll();
                    if (!this.info.isBrowser()) {
                        for (MessageDispatch old : list) {
                            session.connection.rollbackDuplicate(this, old.getMessage());
                        }
                    }
                    // allow dispatch on this connection to resume
                    session.connection.transportInterruptionProcessingComplete();
                    inProgressClearRequiredFlag = false;
                }
            }
        }
    }

    void deliverAcks() {
        MessageAck ack = null;
        if (deliveryingAcknowledgements.compareAndSet(false, true)) {
            if (isAutoAcknowledgeEach()) {
                synchronized(deliveredMessages) {
                    ack = makeAckForAllDeliveredMessages(MessageAck.STANDARD_ACK_TYPE);
                    if (ack != null) {
                        deliveredMessages.clear();
                        ackCounter = 0;
            		} else {
            		    ack = pendingAck;
            		    pendingAck = null;
            		}
            	}
            } else if (pendingAck != null && pendingAck.isStandardAck()) {
                ack = pendingAck;
                pendingAck = null;
            }
            if (ack != null) {
                final MessageAck ackToSend = ack;
                
                if (executorService == null) {
                    executorService = Executors.newSingleThreadExecutor();
                }
                executorService.submit(new Runnable() {
                    public void run() {
                        try {
                            session.sendAck(ackToSend,true);
                        } catch (JMSException e) {
                            LOG.error(getConsumerId() + " failed to delivered acknowledgements", e);
                        } finally {
                            deliveryingAcknowledgements.set(false);
                        }
                    }
                });
            } else {
                deliveryingAcknowledgements.set(false);
            }
        }
    }

    public void dispose() throws JMSException {
        if (!unconsumedMessages.isClosed()) {
            
            // Do we have any acks we need to send out before closing?
            // Ack any delivered messages now.
            if (!session.getTransacted()) { 
                deliverAcks();
                if (isAutoAcknowledgeBatch()) {
                    acknowledge();
                }
            }
            if (executorService != null) {
                executorService.shutdown();
                try {
                    executorService.awaitTermination(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            if (session.isClientAcknowledge()) {
                if (!this.info.isBrowser()) {
                    // rollback duplicates that aren't acknowledged
                    List<MessageDispatch> tmp = null;
                    synchronized (this.deliveredMessages) {
                        tmp = new ArrayList<MessageDispatch>(this.deliveredMessages);
                    }
                    for (MessageDispatch old : tmp) {
                        this.session.connection.rollbackDuplicate(this, old.getMessage());
                    }
                    tmp.clear();
                }
            }
            if (!session.isTransacted()) {
                synchronized(deliveredMessages) {
                    deliveredMessages.clear();
                }
            }
            unconsumedMessages.close();
            this.session.removeConsumer(this);
            List<MessageDispatch> list = unconsumedMessages.removeAll();
            if (!this.info.isBrowser()) {
                for (MessageDispatch old : list) {
                    // ensure we don't filter this as a duplicate
                    session.connection.rollbackDuplicate(this, old.getMessage());
                }
            }
        }
    }

    /**
     * @throws IllegalStateException
     */
    protected void checkClosed() throws IllegalStateException {
        if (unconsumedMessages.isClosed()) {
            throw new IllegalStateException("The Consumer is closed");
        }
    }

    /**
     * If we have a zero prefetch specified then send a pull command to the
     * broker to pull a message we are about to receive
     */
    protected void sendPullCommand(long timeout) throws JMSException {
        clearDispatchList();
        if (info.getPrefetchSize() == 0 && unconsumedMessages.isEmpty()) {
            MessagePull messagePull = new MessagePull();
            messagePull.configure(info);
            messagePull.setTimeout(timeout);
            session.asyncSendPacket(messagePull);
        }
    }

    protected void checkMessageListener() throws JMSException {
        session.checkMessageListener();
    }

    protected void setOptimizeAcknowledge(boolean value) {
        if (optimizeAcknowledge && !value) {
            deliverAcks();
        }
        optimizeAcknowledge = value;
    }

    protected void setPrefetchSize(int prefetch) {
        deliverAcks();
        this.info.setCurrentPrefetchSize(prefetch);
    }

    private void beforeMessageIsConsumed(MessageDispatch md) throws JMSException {
        md.setDeliverySequenceId(session.getNextDeliveryId());
        lastDeliveredSequenceId = md.getMessage().getMessageId().getBrokerSequenceId();
        if (!isAutoAcknowledgeBatch()) {
            synchronized(deliveredMessages) {
                deliveredMessages.addFirst(md);
            }
            if (session.getTransacted()) {
                ackLater(md, MessageAck.DELIVERED_ACK_TYPE);
            }
        }
    }
    
    private void afterMessageIsConsumed(MessageDispatch md, boolean messageExpired) throws JMSException {
        if (unconsumedMessages.isClosed()) {
            return;
        }
        if (messageExpired) {
            synchronized (deliveredMessages) {
                deliveredMessages.remove(md);
            }
            stats.getExpiredMessageCount().increment();
            ackLater(md, MessageAck.DELIVERED_ACK_TYPE);
        } else {
            stats.onMessage();
            if (session.getTransacted()) {
                // Do nothing.
            } else if (isAutoAcknowledgeEach()) {
                if (deliveryingAcknowledgements.compareAndSet(false, true)) {
                    synchronized (deliveredMessages) {
                        if (!deliveredMessages.isEmpty()) {
                            if (optimizeAcknowledge) {
                                ackCounter++;
                                if (ackCounter >= (info.getPrefetchSize() * .65) || System.currentTimeMillis() >= (optimizeAckTimestamp + optimizeAckTimeout)) {
                                	MessageAck ack = makeAckForAllDeliveredMessages(MessageAck.STANDARD_ACK_TYPE);
                                	if (ack != null) {
                            		    deliveredMessages.clear();
                            		    ackCounter = 0;
                            		    session.sendAck(ack);
                            		    optimizeAckTimestamp = System.currentTimeMillis();
                                	}
                                }
                            } else {
                                MessageAck ack = makeAckForAllDeliveredMessages(MessageAck.STANDARD_ACK_TYPE);
                                if (ack!=null) {
                                    deliveredMessages.clear();
                                    session.sendAck(ack);
                                }
                            }
                        }
                    }
                    deliveryingAcknowledgements.set(false);
                }
            } else if (isAutoAcknowledgeBatch()) {
                ackLater(md, MessageAck.STANDARD_ACK_TYPE);
            } else if (session.isClientAcknowledge()||session.isIndividualAcknowledge()) {
                boolean messageUnackedByConsumer = false;
                synchronized (deliveredMessages) {
                    messageUnackedByConsumer = deliveredMessages.contains(md);
                }
                if (messageUnackedByConsumer) {
                    ackLater(md, MessageAck.DELIVERED_ACK_TYPE);
                }
            } 
            else {
                throw new IllegalStateException("Invalid session state.");
            }
        }
    }

    /**
     * Creates a MessageAck for all messages contained in deliveredMessages.
     * Caller should hold the lock for deliveredMessages.
     * 
     * @param type Ack-Type (i.e. MessageAck.STANDARD_ACK_TYPE) 
     * @return <code>null</code> if nothing to ack.
     */
	private MessageAck makeAckForAllDeliveredMessages(byte type) {
		synchronized (deliveredMessages) {
			if (deliveredMessages.isEmpty())
				return null;
			    
			MessageDispatch md = deliveredMessages.getFirst();
		    MessageAck ack = new MessageAck(md, type, deliveredMessages.size());
		    ack.setFirstMessageId(deliveredMessages.getLast().getMessage().getMessageId());
		    return ack;
		}
	}

    private void ackLater(MessageDispatch md, byte ackType) throws JMSException {

        // Don't acknowledge now, but we may need to let the broker know the
        // consumer got the message to expand the pre-fetch window
        if (session.getTransacted()) {
            session.doStartTransaction();
            if (!synchronizationRegistered) {
                synchronizationRegistered = true;
                session.getTransactionContext().addSynchronization(new Synchronization() {
                    @Override
                    public void beforeEnd() throws Exception {
                        acknowledge();
                        synchronizationRegistered = false;
                    }

                    @Override
                    public void afterCommit() throws Exception {
                        commit();
                        synchronizationRegistered = false;
                    }

                    @Override
                    public void afterRollback() throws Exception {
                        rollback();
                        synchronizationRegistered = false;
                    }
                });
            }
        }

        deliveredCounter++;
        
        MessageAck oldPendingAck = pendingAck;
        pendingAck = new MessageAck(md, ackType, deliveredCounter);
        pendingAck.setTransactionId(session.getTransactionContext().getTransactionId());
        if( oldPendingAck==null ) {
            pendingAck.setFirstMessageId(pendingAck.getLastMessageId());
        } else if ( oldPendingAck.getAckType() == pendingAck.getAckType() ) {
            pendingAck.setFirstMessageId(oldPendingAck.getFirstMessageId());
        } else {
            // old pending ack being superseded by ack of another type, if is is not a delivered
            // ack and hence important, send it now so it is not lost.
            if ( !oldPendingAck.isDeliveredAck()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Sending old pending ack " + oldPendingAck + ", new pending: " + pendingAck);
                }
                session.sendAck(oldPendingAck);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("dropping old pending ack " + oldPendingAck + ", new pending: " + pendingAck);
                }
            }
        }
        
        if ((0.5 * info.getPrefetchSize()) <= (deliveredCounter - additionalWindowSize)) {
            session.sendAck(pendingAck);
            pendingAck=null;
            deliveredCounter = 0;
            additionalWindowSize = 0;
        }
    }

    /**
     * Acknowledge all the messages that have been delivered to the client up to
     * this point.
     * 
     * @throws JMSException
     */
    public void acknowledge() throws JMSException {
        clearDispatchList();
        waitForRedeliveries();
        synchronized(deliveredMessages) {
            // Acknowledge all messages so far.
            MessageAck ack = makeAckForAllDeliveredMessages(MessageAck.STANDARD_ACK_TYPE);
            if (ack == null)
            	return; // no msgs
            
            if (session.getTransacted()) {
                rollbackOnFailedRecoveryRedelivery();
                session.doStartTransaction();
                ack.setTransactionId(session.getTransactionContext().getTransactionId());
            }
            session.sendAck(ack);
            pendingAck = null;
            
            // Adjust the counters
            deliveredCounter = Math.max(0, deliveredCounter - deliveredMessages.size());
            additionalWindowSize = Math.max(0, additionalWindowSize - deliveredMessages.size());
            
            if (!session.getTransacted()) {  
                deliveredMessages.clear();
            } 
        }
    }
    
    private void waitForRedeliveries() {
        if (failoverRedeliveryWaitPeriod > 0 && previouslyDeliveredMessages != null) {
            long expiry = System.currentTimeMillis() + failoverRedeliveryWaitPeriod;
            int numberNotReplayed;
            do {
                numberNotReplayed = 0;
                synchronized(deliveredMessages) {
                    if (previouslyDeliveredMessages != null) { 
                        for (Entry<MessageId, Boolean> entry: previouslyDeliveredMessages.entrySet()) {
                            if (!entry.getValue()) {
                                numberNotReplayed++;
                            }
                        }
                    }
                }
                if (numberNotReplayed > 0) {
                    LOG.info("waiting for redelivery of " + numberNotReplayed + " in transaction: "
                            + previouslyDeliveredMessages.transactionId +  ", to consumer :" + this.getConsumerId());
                    try {
                        Thread.sleep(Math.max(500, failoverRedeliveryWaitPeriod/4));
                    } catch (InterruptedException outOfhere) {
                        break;
                    }
                }
            } while (numberNotReplayed > 0 && expiry < System.currentTimeMillis());
        }
    }

    /*
     * called with deliveredMessages locked
     */
    private void rollbackOnFailedRecoveryRedelivery() throws JMSException {
        if (previouslyDeliveredMessages != null) {
            // if any previously delivered messages was not re-delivered, transaction is invalid and must rollback
            // as messages have been dispatched else where.
            int numberNotReplayed = 0;
            for (Entry<MessageId, Boolean> entry: previouslyDeliveredMessages.entrySet()) {
                if (!entry.getValue()) {
                    numberNotReplayed++;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("previously delivered message has not been replayed in transaction: "
                                + previouslyDeliveredMessages.transactionId 
                                + " , messageId: " + entry.getKey());
                    }
                }
            }
            if (numberNotReplayed > 0) {
                String message = "rolling back transaction (" 
                    + previouslyDeliveredMessages.transactionId + ") post failover recovery. " + numberNotReplayed
                    + " previously delivered message(s) not replayed to consumer: " + this.getConsumerId();
                LOG.warn(message);
                throw new TransactionRolledBackException(message);   
            }
        }
    }

    void acknowledge(MessageDispatch md) throws JMSException {
        MessageAck ack = new MessageAck(md,MessageAck.INDIVIDUAL_ACK_TYPE,1);
        session.sendAck(ack);
        synchronized(deliveredMessages){
            deliveredMessages.remove(md);
        }
    }

    public void commit() throws JMSException {
        synchronized (deliveredMessages) {
            deliveredMessages.clear();
            clearPreviouslyDelivered();
        }
        redeliveryDelay = 0;
    }

    public void rollback() throws JMSException {
        synchronized (unconsumedMessages.getMutex()) {
            if (optimizeAcknowledge) {
                // remove messages read but not acked at the broker yet through
                // optimizeAcknowledge
                if (!this.info.isBrowser()) {
                    synchronized(deliveredMessages) {
                        for (int i = 0; (i < deliveredMessages.size()) && (i < ackCounter); i++) {
                            // ensure we don't filter this as a duplicate
                            MessageDispatch md = deliveredMessages.removeLast();
                            session.connection.rollbackDuplicate(this, md.getMessage());
                        }
                    }
                }
            }
            synchronized(deliveredMessages) {
                rollbackPreviouslyDeliveredAndNotRedelivered();
                if (deliveredMessages.isEmpty()) {
                    return;
                }
    
                // Only increase the redelivery delay after the first redelivery..
                MessageDispatch lastMd = deliveredMessages.getFirst();
                final int currentRedeliveryCount = lastMd.getMessage().getRedeliveryCounter();
                if (currentRedeliveryCount > 0) {
                    redeliveryDelay = redeliveryPolicy.getRedeliveryDelay(redeliveryDelay);
                }
                MessageId firstMsgId = deliveredMessages.getLast().getMessage().getMessageId();
    
                for (Iterator<MessageDispatch> iter = deliveredMessages.iterator(); iter.hasNext();) {
                    MessageDispatch md = iter.next();
                    md.getMessage().onMessageRolledBack();
                    // ensure we don't filter this as a duplicate
                    session.connection.rollbackDuplicate(this, md.getMessage());
                }
    
                if (redeliveryPolicy.getMaximumRedeliveries() != RedeliveryPolicy.NO_MAXIMUM_REDELIVERIES
                    && lastMd.getMessage().getRedeliveryCounter() > redeliveryPolicy.getMaximumRedeliveries()) {
                    // We need to NACK the messages so that they get sent to the
                    // DLQ.
                    // Acknowledge the last message.
                    
                    MessageAck ack = new MessageAck(lastMd, MessageAck.POSION_ACK_TYPE, deliveredMessages.size());
					ack.setFirstMessageId(firstMsgId);
                    session.sendAck(ack,true);
                    // Adjust the window size.
                    additionalWindowSize = Math.max(0, additionalWindowSize - deliveredMessages.size());
                    redeliveryDelay = 0;
                } else {
                    
                    // only redelivery_ack after first delivery
                    if (currentRedeliveryCount > 0) {
                        MessageAck ack = new MessageAck(lastMd, MessageAck.REDELIVERED_ACK_TYPE, deliveredMessages.size());
                        ack.setFirstMessageId(firstMsgId);
                        session.sendAck(ack,true);
                    }
    
                    // stop the delivery of messages.
                    unconsumedMessages.stop();
    
                    for (Iterator<MessageDispatch> iter = deliveredMessages.iterator(); iter.hasNext();) {
                        MessageDispatch md = iter.next();
                        unconsumedMessages.enqueueFirst(md);
                    }
    
                    if (redeliveryDelay > 0 && !unconsumedMessages.isClosed()) {
                        // Start up the delivery again a little later.
                        scheduler.executeAfterDelay(new Runnable() {
                            public void run() {
                                try {
                                    if (started.get()) {
                                        start();
                                    }
                                } catch (JMSException e) {
                                    session.connection.onAsyncException(e);
                                }
                            }
                        }, redeliveryDelay);
                    } else {
                        start();
                    }
    
                }
                deliveredCounter -= deliveredMessages.size();
                deliveredMessages.clear();
            }
        }
        if (messageListener.get() != null) {
            session.redispatch(this, unconsumedMessages);
        }
    }

    /*
     * called with unconsumedMessages && deliveredMessages locked
     * remove any message not re-delivered as they can't be replayed to this 
     * consumer on rollback
     */
    private void rollbackPreviouslyDeliveredAndNotRedelivered() {
        if (previouslyDeliveredMessages != null) {
            for (Entry<MessageId, Boolean> entry: previouslyDeliveredMessages.entrySet()) {
                if (!entry.getValue()) {              
                    removeFromDeliveredMessages(entry.getKey());
                }
            }
            clearPreviouslyDelivered();
        }
    }

    /*
     * called with deliveredMessages locked
     */
    private void removeFromDeliveredMessages(MessageId key) {
        Iterator<MessageDispatch> iterator = deliveredMessages.iterator();
        while (iterator.hasNext()) {
            MessageDispatch candidate = iterator.next();
            if (key.equals(candidate.getMessage().getMessageId())) {
                session.connection.rollbackDuplicate(this, candidate.getMessage());
                iterator.remove();
                break;
            }
        }
    }
    /*
     * called with deliveredMessages locked
     */
    private void clearPreviouslyDelivered() {
        if (previouslyDeliveredMessages != null) {
            previouslyDeliveredMessages.clear();
            previouslyDeliveredMessages = null;
        }
    }

    public void dispatch(MessageDispatch md) {
        MessageListener listener = this.messageListener.get();
        try {
            clearMessagesInProgress();
            clearDispatchList();
            synchronized (unconsumedMessages.getMutex()) {
                if (!unconsumedMessages.isClosed()) {
                    if (this.info.isBrowser() || !session.connection.isDuplicate(this, md.getMessage())) {
                        if (listener != null && unconsumedMessages.isRunning()) {
                            ActiveMQMessage message = createActiveMQMessage(md);
                            beforeMessageIsConsumed(md);
                            try {
                                boolean expired = message.isExpired();
                                if (!expired) {
                                    listener.onMessage(message);
                                }
                                afterMessageIsConsumed(md, expired);
                            } catch (RuntimeException e) {
                                LOG.error(getConsumerId() + " Exception while processing message: " + md.getMessage().getMessageId(), e);
                                if (isAutoAcknowledgeBatch() || isAutoAcknowledgeEach() || session.isIndividualAcknowledge()) {
                                    // schedual redelivery and possible dlq processing
                                    rollback();
                                } else {
                                    // Transacted or Client ack: Deliver the
                                    // next message.
                                    afterMessageIsConsumed(md, false);
                                }
                            }
                        } else {
                            if (!unconsumedMessages.isRunning()) {
                                // delayed redelivery, ensure it can be re delivered
                                session.connection.rollbackDuplicate(this, md.getMessage());
                            }
                            unconsumedMessages.enqueue(md);
                            if (availableListener != null) {
                                availableListener.onMessageAvailable(this);
                            }
                        }
                    } else {
                        if (!session.isTransacted()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(getConsumerId() + " ignoring (auto acking) duplicate: " + md.getMessage());
                            }
                            MessageAck ack = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, 1);
                            session.sendAck(ack);
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(getConsumerId() + " tracking transacted redlivery of duplicate: " + md.getMessage());
                            }
                            boolean needsPoisonAck = false;
                            synchronized (deliveredMessages) {
                                if (previouslyDeliveredMessages != null) {
                                    previouslyDeliveredMessages.put(md.getMessage().getMessageId(), true);
                                } else {
                                    // delivery while pending redelivery to another consumer on the same connection
                                    // not waiting for redelivery will help here
                                    needsPoisonAck = true;
                                }
                            }
                            if (needsPoisonAck) {
                                LOG.warn("acking duplicate delivery as poison, redelivery must be pending to another"
                                        + " consumer on this connection, failoverRedeliveryWaitPeriod=" 
                                        + failoverRedeliveryWaitPeriod + ". Message: " + md);
                                MessageAck poisonAck = new MessageAck(md, MessageAck.POSION_ACK_TYPE, 1);
                                poisonAck.setFirstMessageId(md.getMessage().getMessageId());
                                session.sendAck(poisonAck);
                            } else {
                                ackLater(md, MessageAck.DELIVERED_ACK_TYPE);
                            }
                        }
                    }
                }
            }
            if (++dispatchedCount % 1000 == 0) {
                dispatchedCount = 0;
                Thread.yield();
            }
        } catch (Exception e) {
            session.connection.onClientInternalException(e);
        }
    }

    // async (on next call) clear or track delivered as they may be flagged as duplicates if they arrive again
    private void clearDispatchList() {
        if (clearDispatchList) {
            synchronized (deliveredMessages) {  
                if (clearDispatchList) {
                    if (!deliveredMessages.isEmpty()) {
                        if (session.isTransacted()) {    
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(getConsumerId() + " tracking existing transacted delivered list (" + deliveredMessages.size() + ") on transport interrupt");
                            }
                            if (previouslyDeliveredMessages == null) {
                                previouslyDeliveredMessages = new PreviouslyDeliveredMap<MessageId, Boolean>(session.getTransactionContext().getTransactionId());
                            }
                            for (MessageDispatch delivered : deliveredMessages) {
                                previouslyDeliveredMessages.put(delivered.getMessage().getMessageId(), false);
                            }
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(getConsumerId() + " clearing delivered list (" + deliveredMessages.size() + ") on transport interrupt");
                            }
                            deliveredMessages.clear();
                            pendingAck = null;
                        }
                    }
                    clearDispatchList = false;
                }
            }
        }
    }

    public int getMessageSize() {
        return unconsumedMessages.size();
    }

    public void start() throws JMSException {
        if (unconsumedMessages.isClosed()) {
            return;
        }
        started.set(true);
        unconsumedMessages.start();
        session.executor.wakeup();
    }

    public void stop() {
        started.set(false);
        unconsumedMessages.stop();
    }

    @Override
    public String toString() {
        return "ActiveMQMessageConsumer { value=" + info.getConsumerId() + ", started=" + started.get()
               + " }";
    }

    /**
     * Delivers a message to the message listener.
     * 
     * @return
     * @throws JMSException
     */
    public boolean iterate() {
        MessageListener listener = this.messageListener.get();
        if (listener != null) {
            MessageDispatch md = unconsumedMessages.dequeueNoWait();
            if (md != null) {
                dispatch(md);
                return true;
            }
        }
        return false;
    }

    public boolean isInUse(ActiveMQTempDestination destination) {
        return info.getDestination().equals(destination);
    }

    public long getLastDeliveredSequenceId() {
        return lastDeliveredSequenceId;
    }

	public IOException getFailureError() {
		return failureError;
	}

	public void setFailureError(IOException failureError) {
		this.failureError = failureError;
	}
}
