/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.apache.activemq;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.RedeliveryPolicy;
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

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

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
 * For asynchronous delivery, a client can register a <CODE>MessageListener</CODE>
 * object with a message consumer. As messages arrive at the message consumer,
 * it delivers them by calling the <CODE>MessageListener</CODE>'s<CODE>
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

    private static final Log log = LogFactory.getLog(ActiveMQMessageConsumer.class);

    protected final ActiveMQSession session;
    protected final ConsumerInfo info;

    // These are the messages waiting to be delivered to the client
    private final MessageDispatchChannel unconsumedMessages = new MessageDispatchChannel();

    // The are the messages that were delivered to the consumer but that have
    // not been acknowledged. It's kept in reverse order since we
    // Always walk list in reverse order. Only used when session is client ack.
    private final LinkedList deliveredMessages = new LinkedList();
    private int deliveredCounter = 0;
    private int additionalWindowSize = 0;
    private int rollbackCounter = 0;
    private long redeliveryDelay = 0;

    private MessageListener messageListener;
    private JMSConsumerStatsImpl stats;

    private final String selector;
    private boolean synchronizationRegistered = false;
    private AtomicBoolean started = new AtomicBoolean(false);

    private MessageAvailableListener availableListener;

    /**
     * Create a MessageConsumer
     * 
     * @param session
     * @param consumerId
     * @param dest
     * @param name
     * @param selector
     * @param prefetch
     * @param noLocal
     * @param browser
     * @param dispatchAsync
     * @throws JMSException
     */
    public ActiveMQMessageConsumer(ActiveMQSession session, ConsumerId consumerId, ActiveMQDestination dest,
            String name, String selector, int prefetch, boolean noLocal, boolean browser, boolean dispatchAsync)
            throws JMSException {
        if (dest == null) {
            throw new InvalidDestinationException("Don't understand null destinations");
        } else if (dest.getPhysicalName() == null) {
                throw new InvalidDestinationException("The destination object was not given a physical name.");
        } else if (dest.isTemporary()) {
            String physicalName = dest.getPhysicalName();

            if (physicalName == null) {
                throw new IllegalArgumentException("Physical name of Destination should be valid: " + dest);
            }

            String connectionID = session.connection.getConnectionInfo().getConnectionId().getConnectionId();

            if (physicalName.indexOf(connectionID) < 0) {
                throw new InvalidDestinationException("Cannot use a Temporary destination from another Connection");
            }

            if (session.connection.isDeleted(dest)) {
                throw new InvalidDestinationException("Cannot use a Temporary destination that has been deleted");
            }
        }

        this.session = session;
        this.selector = selector;

        this.info = new ConsumerInfo(consumerId);
        this.info.setSubcriptionName(name);
        this.info.setPrefetchSize(prefetch);
        this.info.setNoLocal(noLocal);
        this.info.setDispatchAsync(dispatchAsync);
        this.info.setRetroactive(this.session.connection.isUseRetroactiveConsumer());

        // Allows the options on the destination to configure the consumerInfo
        if (dest.getOptions() != null) {
            HashMap options = new HashMap(dest.getOptions());
            IntrospectionSupport.setProperties(this.info, options, "consumer.");
        }

        this.info.setDestination(dest);
        this.info.setBrowser(browser);
        if (selector != null && selector.trim().length() != 0) {
            // Validate that the selector
            new SelectorParser().parse(selector);
            this.info.setSelector(selector);
        } else {
            this.info.setSelector(null);
        }

        this.stats = new JMSConsumerStatsImpl(session.getSessionStats(), dest);
        try {
            this.session.addConsumer(this);
            this.session.syncSendPacket(info);
        } catch (JMSException e) {
            this.session.removeConsumer(this);
            throw e;
        }

        if (session.connection.isStarted())
            start();
    }

    public StatsImpl getStats() {
        return stats;
    }

    public JMSConsumerStatsImpl getConsumerStats() {
        return stats;
    }

    /**
     * @return Returns the consumerId.
     */
    protected ConsumerId getConsumerId() {
        return info.getConsumerId();
    }

    /**
     * @return the consumer name - used for durable consumers
     */
    protected String getConsumerName() {
        return this.info.getSubcriptionName();
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
        return info.getSubcriptionName()!=null && info.getDestination().isTopic();
    }

    /**
     * Gets this message consumer's message selector expression.
     * 
     * @return this message consumer's message selector, or null if no message
     *         selector exists for the message consumer (that is, if the message
     *         selector was not set or was set to null or the empty string)
     * @throws JMSException
     *             if the JMS provider fails to receive the next message due to
     *             some internal error.
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
     * @throws JMSException
     *             if the JMS provider fails to get the message listener due to
     *             some internal error.
     * @see javax.jms.MessageConsumer#setMessageListener(javax.jms.MessageListener)
     */
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return this.messageListener;
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
     * @param listener
     *            the listener to which the messages are to be delivered
     * @throws JMSException
     *             if the JMS provider fails to receive the next message due to
     *             some internal error.
     * @see javax.jms.MessageConsumer#getMessageListener
     */
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        this.messageListener = listener;
        if (listener != null) {
            boolean wasRunning = session.isRunning();
            if (wasRunning)
                session.stop();

            session.redispatch(unconsumedMessages);

            if (wasRunning)
                session.start();

        }
    }

    
    public MessageAvailableListener getAvailableListener() {
        return availableListener;
    }

    /**
     * Sets the listener used to notify synchronous consumers that there is a message
     * available so that the {@link MessageConsumer#receiveNoWait()} can be called.
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
     * 
     * Expired messages will consumed by this method.
     * 
     * @throws JMSException
     * 
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
                        return null;
                    }
                } else if (md.getMessage().isExpired()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Received expired message: " + md);
                    }
                    beforeMessageIsConsumed(md);
                    afterMessageIsConsumed(md, true);
                    if (timeout > 0) {
                        timeout = Math.max(deadline - System.currentTimeMillis(), 0);
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Received message: " + md);
                    }
                    return md;
                }
            }
        } catch (InterruptedException e) {
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
        MessageDispatch md = dequeue(-1);
        if (md == null)
            return null;

        beforeMessageIsConsumed(md);
        afterMessageIsConsumed(md, false);

        return createActiveMQMessage(md);
    }

    /**
     * @param md
     * @return
     */
    private ActiveMQMessage createActiveMQMessage(final MessageDispatch md) {
        ActiveMQMessage m = (ActiveMQMessage) md.getMessage();
        if (session.isClientAcknowledge()) {
            m.setAcknowledgeCallback(new Callback() {
                public void execute() throws Throwable {
                    session.checkClosed();
                    session.acknowledge();
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
     * @param timeout
     *            the timeout value (in milliseconds)
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

        while (timeout > 0) {
            MessageDispatch md = dequeue(timeout);
            if (md == null)
                return null;

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
     * @throws JMSException
     *             if the JMS provider fails to receive the next message due to
     *             some internal error.
     */
    public Message receiveNoWait() throws JMSException {
        checkClosed();
        checkMessageListener();
        MessageDispatch md = dequeue(0);
        if (md == null)
            return null;

        beforeMessageIsConsumed(md);
        afterMessageIsConsumed(md, false);
        return createActiveMQMessage(md);
    }

    /**
     * Closes the message consumer.
     * <P>
     * Since a provider may allocate some resources on behalf of a <CODE>
     * MessageConsumer</CODE> outside the Java virtual machine, clients should
     * close them when they are not needed. Relying on garbage collection to
     * eventually reclaim these resources may not be timely enough.
     * <P>
     * This call blocks until a <CODE>receive</CODE> or message listener in
     * progress has completed. A blocked message consumer <CODE>receive </CODE>
     * call returns null when this message consumer is closed.
     * 
     * @throws JMSException
     *             if the JMS provider fails to close the consumer due to some
     *             internal error.
     */
    public void close() throws JMSException {
        if (!unconsumedMessages.isClosed()) {
            dispose();
            this.session.syncSendPacket(info.createRemoveCommand());
        }
    }

    public void dispose() throws JMSException {
        if (!unconsumedMessages.isClosed()) {
            // Do we have any acks we need to send out before closing?
            // Ack any delivered messages now. (session may still
            // commit/rollback the acks).
            if ((session.isTransacted() || session.isDupsOkAcknowledge())) {
                acknowledge();
            }
            deliveredMessages.clear();
            unconsumedMessages.close();
            this.session.removeConsumer(this);
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

    protected void checkMessageListener() throws IllegalStateException {
        if (messageListener != null) {
            throw new IllegalStateException("Cannot synchronously receive a message when a MessageListener is set");
        }
    }

    private void beforeMessageIsConsumed(MessageDispatch md) {
        md.setDeliverySequenceId(session.getNextDeliveryId());
        if (!session.isDupsOkAcknowledge())
            deliveredMessages.addFirst(md);
    }

    private void afterMessageIsConsumed(MessageDispatch md, boolean messageExpired) throws JMSException {
        if (unconsumedMessages.isClosed())
            return;

        if (messageExpired) {
            ackLater(md, MessageAck.DELIVERED_ACK_TYPE);
        } else {
            stats.onMessage();
            if (session.isTransacted()) {
                ackLater(md, MessageAck.DELIVERED_ACK_TYPE);
            } else if (session.isAutoAcknowledge()) {
                if (!deliveredMessages.isEmpty()) {
                    MessageAck ack = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, deliveredMessages.size());
                    session.asyncSendPacket(ack);
                    deliveredMessages.clear();
                }
            } else if (session.isDupsOkAcknowledge()) {
                ackLater(md, MessageAck.STANDARD_ACK_TYPE);
            } else if (session.isClientAcknowledge()) {
                ackLater(md, MessageAck.DELIVERED_ACK_TYPE);
            } else {
                throw new IllegalStateException("Invalid session state.");
            }
        }
    }

    private void ackLater(MessageDispatch md, byte ackType) throws JMSException {

        // Don't acknowledge now, but we may need to let the broker know the
        // consumer got the message
        // to expand the pre-fetch window
        if (session.isTransacted()) {
            session.doStartTransaction();
            if (!synchronizationRegistered) {
                synchronizationRegistered = true;
                session.getTransactionContext().addSynchronization(new Synchronization() {
                    public void beforeEnd() throws Throwable {
                        acknowledge();
                        synchronizationRegistered = false;
                    }

                    public void afterCommit() throws Throwable {
                        commit();
                        synchronizationRegistered = false;
                    }

                    public void afterRollback() throws Throwable {
                        rollback();
                        synchronizationRegistered = false;
                    }
                });
            }
        }

        // The delivered message list is only needed for the recover method
        // which is only used with client ack.
        deliveredCounter++;
        if ((0.5 * info.getPrefetchSize()) <= (deliveredCounter - additionalWindowSize)) {
            MessageAck ack = new MessageAck(md, ackType, deliveredCounter);
            ack.setTransactionId(session.getTransactionContext().getTransactionId());
            session.asyncSendPacket(ack);
            additionalWindowSize = deliveredCounter;

            // When using DUPS ok, we do a real ack.
            if (ackType == MessageAck.STANDARD_ACK_TYPE) {
                deliveredCounter = additionalWindowSize = 0;
            }
        }
    }

    /**
     * Acknowledge all the messages that have been delivered to the client upto
     * this point.
     * 
     * @param deliverySequenceId
     * @throws JMSException
     */
    public void acknowledge() throws JMSException {
        if (deliveredMessages.isEmpty())
            return;

        // Acknowledge the last message.
        MessageDispatch lastMd = (MessageDispatch) deliveredMessages.get(0);
        MessageAck ack = new MessageAck(lastMd, MessageAck.STANDARD_ACK_TYPE, deliveredMessages.size());
        if (session.isTransacted()) {
            session.doStartTransaction();
            ack.setTransactionId(session.getTransactionContext().getTransactionId());
        }
        session.asyncSendPacket(ack);

        // Adjust the counters
        deliveredCounter -= deliveredMessages.size();
        additionalWindowSize = Math.max(0, additionalWindowSize - deliveredMessages.size());

        if (!session.isTransacted()) {
            deliveredMessages.clear();
        }
    }

    public void commit() throws JMSException {
        deliveredMessages.clear();
        rollbackCounter = 0;
        redeliveryDelay = 0;
    }

    public void rollback() throws JMSException {
        synchronized (unconsumedMessages.getMutex()) {
            if (deliveredMessages.isEmpty())
                return;

            rollbackCounter++;
            RedeliveryPolicy redeliveryPolicy = session.connection.getRedeliveryPolicy();
            if (rollbackCounter > redeliveryPolicy.getMaximumRedeliveries()) {
                
                // We need to NACK the messages so that they get sent to the
                // DLQ.

                // Acknowledge the last message.
                MessageDispatch lastMd = (MessageDispatch) deliveredMessages.get(0);
                MessageAck ack = new MessageAck(lastMd, MessageAck.POSION_ACK_TYPE, deliveredMessages.size());
                session.asyncSendPacket(ack);

                // Adjust the counters
                deliveredCounter -= deliveredMessages.size();
                additionalWindowSize = Math.max(0, additionalWindowSize - deliveredMessages.size());

            } else {

                // stop the delivery of messages.
                unconsumedMessages.stop();

                // Start up the delivery again a little later.
                if (redeliveryDelay == 0) {
                    redeliveryDelay = redeliveryPolicy.getInitialRedeliveryDelay();
                } else {
                    if (redeliveryPolicy.isUseExponentialBackOff())
                        redeliveryDelay *= redeliveryPolicy.getBackOffMultiplier();
                }

                Scheduler.executeAfterDelay(new Runnable() {
                    public void run() {
                        try {
                            if (started.get())
                                start();
                        } catch (JMSException e) {
                            session.connection.onAsyncException(e);
                        }
                    }
                }, redeliveryDelay);
                
                for (Iterator iter = deliveredMessages.iterator(); iter.hasNext();) {
                    MessageDispatch md = (MessageDispatch) iter.next();
                    md.getMessage().incrementRedeliveryCounter();
                    unconsumedMessages.enqueueFirst(md);
                }
            }

            deliveredMessages.clear();
        }

        if (messageListener != null) {
            session.redispatch(unconsumedMessages);
        }
    }

    public void dispatch(MessageDispatch md) {
        MessageListener listener = this.messageListener;
        try {
            if (!unconsumedMessages.isClosed()) {
                if (listener != null && started.get()) {
                    ActiveMQMessage message = createActiveMQMessage(md);
                    beforeMessageIsConsumed(md);
                    listener.onMessage(message);
                    afterMessageIsConsumed(md, false);
                } else {
                    if (availableListener != null) {
                        availableListener.onMessageAvailable(this);
                    }
                    unconsumedMessages.enqueue(md);
                }
            }
        } catch (Exception e) {
            log.warn("could not process message: " + md, e);
        }
    }

    public int getMessageSize() {
        return unconsumedMessages.size();
    }

    public void start() throws JMSException {
        started.set(true);
        unconsumedMessages.start();
        MessageListener listener = this.messageListener;
        if( listener!=null ) {
            MessageDispatch md;
            while( (md = unconsumedMessages.dequeueNoWait())!=null ) {
                ActiveMQMessage message = createActiveMQMessage(md);
                beforeMessageIsConsumed(md);
                listener.onMessage(message);
                afterMessageIsConsumed(md, false);
            }
        }
    }

    public void stop() {
        started.set(false);
        unconsumedMessages.stop();
    }
    
    public String toString() {
        return "ActiveMQMessageConsumer { consumerId=" +info.getConsumerId()+", started=" +started.get()+" }";
    }

}
