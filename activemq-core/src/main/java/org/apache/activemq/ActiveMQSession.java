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

import org.apache.activemq.blob.BlobTransferPolicy;
import org.apache.activemq.blob.BlobUploader;
import org.apache.activemq.command.*;
import org.apache.activemq.management.JMSSessionStatsImpl;
import org.apache.activemq.management.StatsCapable;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.util.Callback;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import javax.jms.IllegalStateException;
import javax.jms.Message;
import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <P>
 * A <CODE>Session</CODE> object is a single-threaded context for producing
 * and consuming messages. Although it may allocate provider resources outside
 * the Java virtual machine (JVM), it is considered a lightweight JMS object.
 * <P>
 * A session serves several purposes:
 * <UL>
 * <LI>It is a factory for its message producers and consumers.
 * <LI>It supplies provider-optimized message factories.
 * <LI>It is a factory for <CODE>TemporaryTopics</CODE> and
 * <CODE>TemporaryQueues</CODE>.
 * <LI>It provides a way to create <CODE>Queue</CODE> or <CODE>Topic</CODE>
 * objects for those clients that need to dynamically manipulate
 * provider-specific destination names.
 * <LI>It supports a single series of transactions that combine work spanning
 * its producers and consumers into atomic units.
 * <LI>It defines a serial order for the messages it consumes and the messages
 * it produces.
 * <LI>It retains messages it consumes until they have been acknowledged.
 * <LI>It serializes execution of message listeners registered with its message
 * consumers.
 * <LI>It is a factory for <CODE>QueueBrowsers</CODE>.
 * </UL>
 * <P>
 * A session can create and service multiple message producers and consumers.
 * <P>
 * One typical use is to have a thread block on a synchronous
 * <CODE>MessageConsumer</CODE> until a message arrives. The thread may then
 * use one or more of the <CODE>Session</CODE>'s<CODE>MessageProducer</CODE>s.
 * <P>
 * If a client desires to have one thread produce messages while others consume
 * them, the client should use a separate session for its producing thread.
 * <P>
 * Once a connection has been started, any session with one or more registered
 * message listeners is dedicated to the thread of control that delivers
 * messages to it. It is erroneous for client code to use this session or any of
 * its constituent objects from another thread of control. The only exception to
 * this rule is the use of the session or connection <CODE>close</CODE>
 * method.
 * <P>
 * It should be easy for most clients to partition their work naturally into
 * sessions. This model allows clients to start simply and incrementally add
 * message processing complexity as their need for concurrency grows.
 * <P>
 * The <CODE>close</CODE> method is the only session method that can be called
 * while some other session method is being executed in another thread.
 * <P>
 * A session may be specified as transacted. Each transacted session supports a
 * single series of transactions. Each transaction groups a set of message sends
 * and a set of message receives into an atomic unit of work. In effect,
 * transactions organize a session's input message stream and output message
 * stream into series of atomic units. When a transaction commits, its atomic
 * unit of input is acknowledged and its associated atomic unit of output is
 * sent. If a transaction rollback is done, the transaction's sent messages are
 * destroyed and the session's input is automatically recovered.
 * <P>
 * The content of a transaction's input and output units is simply those
 * messages that have been produced and consumed within the session's current
 * transaction.
 * <P>
 * A transaction is completed using either its session's <CODE>commit</CODE>
 * method or its session's <CODE>rollback </CODE> method. The completion of a
 * session's current transaction automatically begins the next. The result is
 * that a transacted session always has a current transaction within which its
 * work is done.
 * <P>
 * The Java Transaction Service (JTS) or some other transaction monitor may be
 * used to combine a session's transaction with transactions on other resources
 * (databases, other JMS sessions, etc.). Since Java distributed transactions
 * are controlled via the Java Transaction API (JTA), use of the session's
 * <CODE>commit</CODE> and <CODE>rollback</CODE> methods in this context is
 * prohibited.
 * <P>
 * The JMS API does not require support for JTA; however, it does define how a
 * provider supplies this support.
 * <P>
 * Although it is also possible for a JMS client to handle distributed
 * transactions directly, it is unlikely that many JMS clients will do this.
 * Support for JTA in the JMS API is targeted at systems vendors who will be
 * integrating the JMS API into their application server products.
 * 
 * @version $Revision: 1.34 $
 * @see javax.jms.Session
 * @see javax.jms.QueueSession
 * @see javax.jms.TopicSession
 * @see javax.jms.XASession
 */
public class ActiveMQSession implements Session, QueueSession, TopicSession, StatsCapable, ActiveMQDispatcher {
	
	/**
	 * Only acknowledge an individual message - using message.acknowledge()
	 * as opposed to CLIENT_ACKNOWLEDGE which 
	 * acknowledges all messages consumed by a session at when acknowledge()
	 * is called
	 */
	public static final int INDIVIDUAL_ACKNOWLEDGE=4;

    public static interface DeliveryListener {
        void beforeDelivery(ActiveMQSession session, Message msg);

        void afterDelivery(ActiveMQSession session, Message msg);
    }

    private static final Log LOG = LogFactory.getLog(ActiveMQSession.class);
    protected static final Scheduler scheduler = Scheduler.getInstance();

    protected int acknowledgementMode;
    protected final ActiveMQConnection connection;
    protected final SessionInfo info;
    protected final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    protected final LongSequenceGenerator producerIdGenerator = new LongSequenceGenerator();
    protected final LongSequenceGenerator deliveryIdGenerator = new LongSequenceGenerator();
    protected final ActiveMQSessionExecutor executor = new ActiveMQSessionExecutor(this);
    protected final AtomicBoolean started = new AtomicBoolean(false);

    protected final CopyOnWriteArrayList<ActiveMQMessageConsumer> consumers = new CopyOnWriteArrayList<ActiveMQMessageConsumer>();
    protected final CopyOnWriteArrayList<ActiveMQMessageProducer> producers = new CopyOnWriteArrayList<ActiveMQMessageProducer>();

    protected boolean closed;
    protected boolean asyncDispatch;
    protected boolean sessionAsyncDispatch;
    protected final boolean debug;
    protected Object sendMutex = new Object();

    private MessageListener messageListener;
    private JMSSessionStatsImpl stats;
    private TransactionContext transactionContext;
    private DeliveryListener deliveryListener;
    private MessageTransformer transformer;
    private BlobTransferPolicy blobTransferPolicy;

    /**
     * Construct the Session
     * 
     * @param connection
     * @param sessionId
     * @param acknowledgeMode n.b if transacted - the acknowledgeMode ==
     *                Session.SESSION_TRANSACTED
     * @param asyncDispatch
     * @param sessionAsyncDispatch
     * @throws JMSException on internal error
     */
    protected ActiveMQSession(ActiveMQConnection connection, SessionId sessionId, int acknowledgeMode, boolean asyncDispatch, boolean sessionAsyncDispatch) throws JMSException {
        this.debug = LOG.isDebugEnabled();
        this.connection = connection;
        this.acknowledgementMode = acknowledgeMode;
        this.asyncDispatch = asyncDispatch;
        this.sessionAsyncDispatch = sessionAsyncDispatch;
        this.info = new SessionInfo(connection.getConnectionInfo(), sessionId.getValue());
        setTransactionContext(new TransactionContext(connection));
        connection.addSession(this);
        stats = new JMSSessionStatsImpl(producers, consumers);
        this.connection.asyncSendPacket(info);
        setTransformer(connection.getTransformer());
        setBlobTransferPolicy(connection.getBlobTransferPolicy());

        if (connection.isStarted()) {
            start();
        }

    }

    protected ActiveMQSession(ActiveMQConnection connection, SessionId sessionId, int acknowledgeMode, boolean asyncDispatch) throws JMSException {
        this(connection, sessionId, acknowledgeMode, asyncDispatch, true);
    }

    /**
     * Sets the transaction context of the session.
     * 
     * @param transactionContext - provides the means to control a JMS
     *                transaction.
     */
    public void setTransactionContext(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    /**
     * Returns the transaction context of the session.
     * 
     * @return transactionContext - session's transaction context.
     */
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.management.StatsCapable#getStats()
     */
    public StatsImpl getStats() {
        return stats;
    }

    /**
     * Returns the session's statistics.
     * 
     * @return stats - session's statistics.
     */
    public JMSSessionStatsImpl getSessionStats() {
        return stats;
    }

    /**
     * Creates a <CODE>BytesMessage</CODE> object. A <CODE>BytesMessage</CODE>
     * object is used to send a message containing a stream of uninterpreted
     * bytes.
     * 
     * @return the an ActiveMQBytesMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public BytesMessage createBytesMessage() throws JMSException {
        ActiveMQBytesMessage message = new ActiveMQBytesMessage();
        configureMessage(message);
        return message;
    }

    /**
     * Creates a <CODE>MapMessage</CODE> object. A <CODE>MapMessage</CODE>
     * object is used to send a self-defining set of name-value pairs, where
     * names are <CODE>String</CODE> objects and values are primitive values
     * in the Java programming language.
     * 
     * @return an ActiveMQMapMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public MapMessage createMapMessage() throws JMSException {
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        configureMessage(message);
        return message;
    }

    /**
     * Creates a <CODE>Message</CODE> object. The <CODE>Message</CODE>
     * interface is the root interface of all JMS messages. A
     * <CODE>Message</CODE> object holds all the standard message header
     * information. It can be sent when a message containing only header
     * information is sufficient.
     * 
     * @return an ActiveMQMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public Message createMessage() throws JMSException {
        ActiveMQMessage message = new ActiveMQMessage();
        configureMessage(message);
        return message;
    }

    /**
     * Creates an <CODE>ObjectMessage</CODE> object. An
     * <CODE>ObjectMessage</CODE> object is used to send a message that
     * contains a serializable Java object.
     * 
     * @return an ActiveMQObjectMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public ObjectMessage createObjectMessage() throws JMSException {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        configureMessage(message);
        return message;
    }

    /**
     * Creates an initialized <CODE>ObjectMessage</CODE> object. An
     * <CODE>ObjectMessage</CODE> object is used to send a message that
     * contains a serializable Java object.
     * 
     * @param object the object to use to initialize this message
     * @return an ActiveMQObjectMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        configureMessage(message);
        message.setObject(object);
        return message;
    }

    /**
     * Creates a <CODE>StreamMessage</CODE> object. A
     * <CODE>StreamMessage</CODE> object is used to send a self-defining
     * stream of primitive values in the Java programming language.
     * 
     * @return an ActiveMQStreamMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public StreamMessage createStreamMessage() throws JMSException {
        ActiveMQStreamMessage message = new ActiveMQStreamMessage();
        configureMessage(message);
        return message;
    }

    /**
     * Creates a <CODE>TextMessage</CODE> object. A <CODE>TextMessage</CODE>
     * object is used to send a message containing a <CODE>String</CODE>
     * object.
     * 
     * @return an ActiveMQTextMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public TextMessage createTextMessage() throws JMSException {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        configureMessage(message);
        return message;
    }

    /**
     * Creates an initialized <CODE>TextMessage</CODE> object. A
     * <CODE>TextMessage</CODE> object is used to send a message containing a
     * <CODE>String</CODE>.
     * 
     * @param text the string used to initialize this message
     * @return an ActiveMQTextMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public TextMessage createTextMessage(String text) throws JMSException {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(text);
        configureMessage(message);
        return message;
    }

    /**
     * Creates an initialized <CODE>BlobMessage</CODE> object. A
     * <CODE>BlobMessage</CODE> object is used to send a message containing a
     * <CODE>URL</CODE> which points to some network addressible BLOB.
     * 
     * @param url the network addressable URL used to pass directly to the
     *                consumer
     * @return a BlobMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public BlobMessage createBlobMessage(URL url) throws JMSException {
        return createBlobMessage(url, false);
    }

    /**
     * Creates an initialized <CODE>BlobMessage</CODE> object. A
     * <CODE>BlobMessage</CODE> object is used to send a message containing a
     * <CODE>URL</CODE> which points to some network addressible BLOB.
     * 
     * @param url the network addressable URL used to pass directly to the
     *                consumer
     * @param deletedByBroker indicates whether or not the resource is deleted
     *                by the broker when the message is acknowledged
     * @return a BlobMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public BlobMessage createBlobMessage(URL url, boolean deletedByBroker) throws JMSException {
        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        configureMessage(message);
        message.setURL(url);
        message.setDeletedByBroker(deletedByBroker);
        return message;
    }

    /**
     * Creates an initialized <CODE>BlobMessage</CODE> object. A
     * <CODE>BlobMessage</CODE> object is used to send a message containing
     * the <CODE>File</CODE> content. Before the message is sent the file
     * conent will be uploaded to the broker or some other remote repository
     * depending on the {@link #getBlobTransferPolicy()}.
     * 
     * @param file the file to be uploaded to some remote repo (or the broker)
     *                depending on the strategy
     * @return a BlobMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public BlobMessage createBlobMessage(File file) throws JMSException {
        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        configureMessage(message);
        message.setBlobUploader(new BlobUploader(getBlobTransferPolicy(), file));
        message.setDeletedByBroker(true);
        message.setName(file.getName());
        return message;
    }

    /**
     * Creates an initialized <CODE>BlobMessage</CODE> object. A
     * <CODE>BlobMessage</CODE> object is used to send a message containing
     * the <CODE>File</CODE> content. Before the message is sent the file
     * conent will be uploaded to the broker or some other remote repository
     * depending on the {@link #getBlobTransferPolicy()}.
     * 
     * @param in the stream to be uploaded to some remote repo (or the broker)
     *                depending on the strategy
     * @return a BlobMessage
     * @throws JMSException if the JMS provider fails to create this message due
     *                 to some internal error.
     */
    public BlobMessage createBlobMessage(InputStream in) throws JMSException {
        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        configureMessage(message);
        message.setBlobUploader(new BlobUploader(getBlobTransferPolicy(), in));
        message.setDeletedByBroker(true);
        return message;
    }

    /**
     * Indicates whether the session is in transacted mode.
     * 
     * @return true if the session is in transacted mode
     * @throws JMSException if there is some internal error.
     */
    public boolean getTransacted() throws JMSException {
        checkClosed();
        return (acknowledgementMode == Session.SESSION_TRANSACTED) || (transactionContext.isInXATransaction());
    }

    /**
     * Returns the acknowledgement mode of the session. The acknowledgement mode
     * is set at the time that the session is created. If the session is
     * transacted, the acknowledgement mode is ignored.
     * 
     * @return If the session is not transacted, returns the current
     *         acknowledgement mode for the session. If the session is
     *         transacted, returns SESSION_TRANSACTED.
     * @throws JMSException
     * @see javax.jms.Connection#createSession(boolean,int)
     * @since 1.1 exception JMSException if there is some internal error.
     */
    public int getAcknowledgeMode() throws JMSException {
        checkClosed();
        return this.acknowledgementMode;
    }

    /**
     * Commits all messages done in this transaction and releases any locks
     * currently held.
     * 
     * @throws JMSException if the JMS provider fails to commit the transaction
     *                 due to some internal error.
     * @throws TransactionRolledBackException if the transaction is rolled back
     *                 due to some internal error during commit.
     * @throws javax.jms.IllegalStateException if the method is not called by a
     *                 transacted session.
     */
    public void commit() throws JMSException {
        checkClosed();
        if (!getTransacted()) {
            throw new javax.jms.IllegalStateException("Not a transacted session");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(getSessionId() + " Transaction Commit :" + transactionContext.getTransactionId());
        }
        transactionContext.commit();
    }

    /**
     * Rolls back any messages done in this transaction and releases any locks
     * currently held.
     * 
     * @throws JMSException if the JMS provider fails to roll back the
     *                 transaction due to some internal error.
     * @throws javax.jms.IllegalStateException if the method is not called by a
     *                 transacted session.
     */
    public void rollback() throws JMSException {
        checkClosed();
        if (!getTransacted()) {
            throw new javax.jms.IllegalStateException("Not a transacted session");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(getSessionId() + " Transaction Rollback");
        }
        transactionContext.rollback();
    }

    /**
     * Closes the session.
     * <P>
     * Since a provider may allocate some resources on behalf of a session
     * outside the JVM, clients should close the resources when they are not
     * needed. Relying on garbage collection to eventually reclaim these
     * resources may not be timely enough.
     * <P>
     * There is no need to close the producers and consumers of a closed
     * session.
     * <P>
     * This call will block until a <CODE>receive</CODE> call or message
     * listener in progress has completed. A blocked message consumer
     * <CODE>receive</CODE> call returns <CODE>null</CODE> when this session
     * is closed.
     * <P>
     * Closing a transacted session must roll back the transaction in progress.
     * <P>
     * This method is the only <CODE>Session</CODE> method that can be called
     * concurrently.
     * <P>
     * Invoking any other <CODE>Session</CODE> method on a closed session must
     * throw a <CODE> JMSException.IllegalStateException</CODE>. Closing a
     * closed session must <I>not </I> throw an exception.
     * 
     * @throws JMSException if the JMS provider fails to close the session due
     *                 to some internal error.
     */
    public void close() throws JMSException {
        if (!closed) {
            dispose();
            connection.asyncSendPacket(info.createRemoveCommand());
        }
    }

    void clearMessagesInProgress() {
        executor.clearMessagesInProgress();
        for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
            ActiveMQMessageConsumer consumer = iter.next();
            consumer.clearMessagesInProgress();
        }
    }

    void deliverAcks() {
        for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
            ActiveMQMessageConsumer consumer = iter.next();
            consumer.deliverAcks();
        }
    }

    public synchronized void dispose() throws JMSException {
        if (!closed) {

            try {
                executor.stop();

                for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
                    ActiveMQMessageConsumer consumer = iter.next();
                    consumer.dispose();
                }
                consumers.clear();

                for (Iterator<ActiveMQMessageProducer> iter = producers.iterator(); iter.hasNext();) {
                    ActiveMQMessageProducer producer = iter.next();
                    producer.dispose();
                }
                producers.clear();

                try {
                    if (getTransactionContext().isInLocalTransaction()) {
                        rollback();
                    }
                } catch (JMSException e) {
                }

            } finally {
                connection.removeSession(this);
                this.transactionContext = null;
                closed = true;
            }
        }
    }

    /**
     * Checks that the session is not closed then configures the message
     */
    protected void configureMessage(ActiveMQMessage message) throws IllegalStateException {
        checkClosed();
        message.setConnection(connection);
    }

    /**
     * Check if the session is closed. It is used for ensuring that the session
     * is open before performing various operations.
     * 
     * @throws IllegalStateException if the Session is closed
     */
    protected void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("The Session is closed");
        }
    }

    /**
     * Stops message delivery in this session, and restarts message delivery
     * with the oldest unacknowledged message.
     * <P>
     * All consumers deliver messages in a serial order. Acknowledging a
     * received message automatically acknowledges all messages that have been
     * delivered to the client.
     * <P>
     * Restarting a session causes it to take the following actions:
     * <UL>
     * <LI>Stop message delivery
     * <LI>Mark all messages that might have been delivered but not
     * acknowledged as "redelivered"
     * <LI>Restart the delivery sequence including all unacknowledged messages
     * that had been previously delivered. Redelivered messages do not have to
     * be delivered in exactly their original delivery order.
     * </UL>
     * 
     * @throws JMSException if the JMS provider fails to stop and restart
     *                 message delivery due to some internal error.
     * @throws IllegalStateException if the method is called by a transacted
     *                 session.
     */
    public void recover() throws JMSException {

        checkClosed();
        if (getTransacted()) {
            throw new IllegalStateException("This session is transacted");
        }

        for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
            ActiveMQMessageConsumer c = iter.next();
            c.rollback();
        }

    }

    /**
     * Returns the session's distinguished message listener (optional).
     * 
     * @return the message listener associated with this session
     * @throws JMSException if the JMS provider fails to get the message
     *                 listener due to an internal error.
     * @see javax.jms.Session#setMessageListener(javax.jms.MessageListener)
     * @see javax.jms.ServerSessionPool
     * @see javax.jms.ServerSession
     */
    public MessageListener getMessageListener() throws JMSException {
        checkClosed();
        return this.messageListener;
    }

    /**
     * Sets the session's distinguished message listener (optional).
     * <P>
     * When the distinguished message listener is set, no other form of message
     * receipt in the session can be used; however, all forms of sending
     * messages are still supported.
     * <P>
     * This is an expert facility not used by regular JMS clients.
     * 
     * @param listener the message listener to associate with this session
     * @throws JMSException if the JMS provider fails to set the message
     *                 listener due to an internal error.
     * @see javax.jms.Session#getMessageListener()
     * @see javax.jms.ServerSessionPool
     * @see javax.jms.ServerSession
     */
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        this.messageListener = listener;

        if (listener != null) {
            executor.setDispatchedBySessionPool(true);
        }
    }

    /**
     * Optional operation, intended to be used only by Application Servers, not
     * by ordinary JMS clients.
     * 
     * @see javax.jms.ServerSession
     */
    public void run() {
        MessageDispatch messageDispatch;
        while ((messageDispatch = executor.dequeueNoWait()) != null) {
            final MessageDispatch md = messageDispatch;
            ActiveMQMessage message = (ActiveMQMessage)md.getMessage();
            if (message.isExpired() || connection.isDuplicate(ActiveMQSession.this, message)) {
                // TODO: Ack it without delivery to client
                continue;
            }

            if (isClientAcknowledge()||isIndividualAcknowledge()) {
                message.setAcknowledgeCallback(new Callback() {
                    public void execute() throws Exception {
                    }
                });
            }

            if (deliveryListener != null) {
                deliveryListener.beforeDelivery(this, message);
            }

            md.setDeliverySequenceId(getNextDeliveryId());

            try {
                messageListener.onMessage(message);
            } catch (RuntimeException e) {
                LOG.error("error dispatching message: ", e);
                // A problem while invoking the MessageListener does not
                // in general indicate a problem with the connection to the broker, i.e.
                // it will usually be sufficient to let the afterDelivery() method either
                // commit or roll back in order to deal with the exception.
                // However, we notify any registered client internal exception listener
                // of the problem.
                connection.onClientInternalException(e);
            }

            try {
                MessageAck ack = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, 1);
                ack.setFirstMessageId(md.getMessage().getMessageId());
                doStartTransaction();
                ack.setTransactionId(getTransactionContext().getTransactionId());
                if (ack.getTransactionId() != null) {
                    getTransactionContext().addSynchronization(new Synchronization() {

                        public void afterRollback() throws Exception {
                            md.getMessage().onMessageRolledBack();
                            // ensure we don't filter this as a duplicate
                            connection.rollbackDuplicate(ActiveMQSession.this, md.getMessage());
                            RedeliveryPolicy redeliveryPolicy = connection.getRedeliveryPolicy();
                            int redeliveryCounter = md.getMessage().getRedeliveryCounter();
                            if (redeliveryPolicy.getMaximumRedeliveries() != RedeliveryPolicy.NO_MAXIMUM_REDELIVERIES
                                && redeliveryCounter > redeliveryPolicy.getMaximumRedeliveries()) {
                                // We need to NACK the messages so that they get
                                // sent to the
                                // DLQ.
                                // Acknowledge the last message.
                                MessageAck ack = new MessageAck(md, MessageAck.POSION_ACK_TYPE, 1);
                                ack.setFirstMessageId(md.getMessage().getMessageId());
                                asyncSendPacket(ack);
                            } else {
                                
                                MessageAck ack = new MessageAck(md, MessageAck.REDELIVERED_ACK_TYPE, 1);
                                ack.setFirstMessageId(md.getMessage().getMessageId());
                                asyncSendPacket(ack);

                                // Figure out how long we should wait to resend
                                // this message.
                                long redeliveryDelay = 0;
                                for (int i = 0; i < redeliveryCounter; i++) {
                                    redeliveryDelay = redeliveryPolicy.getRedeliveryDelay(redeliveryDelay);
                                }
                                scheduler.executeAfterDelay(new Runnable() {

                                    public void run() {
                                        ((ActiveMQDispatcher)md.getConsumer()).dispatch(md);
                                    }
                                }, redeliveryDelay);
                            }
                        }
                    });
                }
                asyncSendPacket(ack);
            } catch (Throwable e) {
                connection.onClientInternalException(e);
            }

            if (deliveryListener != null) {
                deliveryListener.afterDelivery(this, message);
            }
        }
    }

    /**
     * Creates a <CODE>MessageProducer</CODE> to send messages to the
     * specified destination.
     * <P>
     * A client uses a <CODE>MessageProducer</CODE> object to send messages to
     * a destination. Since <CODE>Queue </CODE> and <CODE>Topic</CODE> both
     * inherit from <CODE>Destination</CODE>, they can be used in the
     * destination parameter to create a <CODE>MessageProducer</CODE> object.
     * 
     * @param destination the <CODE>Destination</CODE> to send to, or null if
     *                this is a producer which does not have a specified
     *                destination.
     * @return the MessageProducer
     * @throws JMSException if the session fails to create a MessageProducer due
     *                 to some internal error.
     * @throws InvalidDestinationException if an invalid destination is
     *                 specified.
     * @since 1.1
     */
    public MessageProducer createProducer(Destination destination) throws JMSException {
        checkClosed();
        if (destination instanceof CustomDestination) {
            CustomDestination customDestination = (CustomDestination)destination;
            return customDestination.createProducer(this);
        }
        int timeSendOut = connection.getSendTimeout();
        return new ActiveMQMessageProducer(this, getNextProducerId(), ActiveMQMessageTransformation.transformDestination(destination),timeSendOut);
    }

    /**
     * Creates a <CODE>MessageConsumer</CODE> for the specified destination.
     * Since <CODE>Queue</CODE> and <CODE> Topic</CODE> both inherit from
     * <CODE>Destination</CODE>, they can be used in the destination
     * parameter to create a <CODE>MessageConsumer</CODE>.
     * 
     * @param destination the <CODE>Destination</CODE> to access.
     * @return the MessageConsumer
     * @throws JMSException if the session fails to create a consumer due to
     *                 some internal error.
     * @throws InvalidDestinationException if an invalid destination is
     *                 specified.
     * @since 1.1
     */
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return createConsumer(destination, (String) null);
    }

    /**
     * Creates a <CODE>MessageConsumer</CODE> for the specified destination,
     * using a message selector. Since <CODE> Queue</CODE> and
     * <CODE>Topic</CODE> both inherit from <CODE>Destination</CODE>, they
     * can be used in the destination parameter to create a
     * <CODE>MessageConsumer</CODE>.
     * <P>
     * A client uses a <CODE>MessageConsumer</CODE> object to receive messages
     * that have been sent to a destination.
     * 
     * @param destination the <CODE>Destination</CODE> to access
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @return the MessageConsumer
     * @throws JMSException if the session fails to create a MessageConsumer due
     *                 to some internal error.
     * @throws InvalidDestinationException if an invalid destination is
     *                 specified.
     * @throws InvalidSelectorException if the message selector is invalid.
     * @since 1.1
     */
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        return createConsumer(destination, messageSelector, false);
    }

    /**
     * Creates a <CODE>MessageConsumer</CODE> for the specified destination.
     * Since <CODE>Queue</CODE> and <CODE> Topic</CODE> both inherit from
     * <CODE>Destination</CODE>, they can be used in the destination
     * parameter to create a <CODE>MessageConsumer</CODE>.
     *
     * @param destination the <CODE>Destination</CODE> to access.
     * @param messageListener the listener to use for async consumption of messages
     * @return the MessageConsumer
     * @throws JMSException if the session fails to create a consumer due to
     *                 some internal error.
     * @throws InvalidDestinationException if an invalid destination is
     *                 specified.
     * @since 1.1
     */
    public MessageConsumer createConsumer(Destination destination, MessageListener messageListener) throws JMSException {
        return createConsumer(destination, null, messageListener);
    }

    /**
     * Creates a <CODE>MessageConsumer</CODE> for the specified destination,
     * using a message selector. Since <CODE> Queue</CODE> and
     * <CODE>Topic</CODE> both inherit from <CODE>Destination</CODE>, they
     * can be used in the destination parameter to create a
     * <CODE>MessageConsumer</CODE>.
     * <P>
     * A client uses a <CODE>MessageConsumer</CODE> object to receive messages
     * that have been sent to a destination.
     *
     * @param destination the <CODE>Destination</CODE> to access
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @param messageListener the listener to use for async consumption of messages
     * @return the MessageConsumer
     * @throws JMSException if the session fails to create a MessageConsumer due
     *                 to some internal error.
     * @throws InvalidDestinationException if an invalid destination is
     *                 specified.
     * @throws InvalidSelectorException if the message selector is invalid.
     * @since 1.1
     */
    public MessageConsumer createConsumer(Destination destination, String messageSelector, MessageListener messageListener) throws JMSException {
        return createConsumer(destination, messageSelector, false, messageListener);
    }

    /**
     * Creates <CODE>MessageConsumer</CODE> for the specified destination,
     * using a message selector. This method can specify whether messages
     * published by its own connection should be delivered to it, if the
     * destination is a topic.
     * <P>
     * Since <CODE>Queue</CODE> and <CODE>Topic</CODE> both inherit from
     * <CODE>Destination</CODE>, they can be used in the destination
     * parameter to create a <CODE>MessageConsumer</CODE>.
     * <P>
     * A client uses a <CODE>MessageConsumer</CODE> object to receive messages
     * that have been published to a destination.
     * <P>
     * In some cases, a connection may both publish and subscribe to a topic.
     * The consumer <CODE>NoLocal</CODE> attribute allows a consumer to
     * inhibit the delivery of messages published by its own connection. The
     * default value for this attribute is False. The <CODE>noLocal</CODE>
     * value must be supported by destinations that are topics.
     * 
     * @param destination the <CODE>Destination</CODE> to access
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @param noLocal - if true, and the destination is a topic, inhibits the
     *                delivery of messages published by its own connection. The
     *                behavior for <CODE>NoLocal</CODE> is not specified if
     *                the destination is a queue.
     * @return the MessageConsumer
     * @throws JMSException if the session fails to create a MessageConsumer due
     *                 to some internal error.
     * @throws InvalidDestinationException if an invalid destination is
     *                 specified.
     * @throws InvalidSelectorException if the message selector is invalid.
     * @since 1.1
     */
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        return createConsumer(destination, messageSelector, noLocal, null);
    }

    /**
     * Creates <CODE>MessageConsumer</CODE> for the specified destination,
     * using a message selector. This method can specify whether messages
     * published by its own connection should be delivered to it, if the
     * destination is a topic.
     * <P>
     * Since <CODE>Queue</CODE> and <CODE>Topic</CODE> both inherit from
     * <CODE>Destination</CODE>, they can be used in the destination
     * parameter to create a <CODE>MessageConsumer</CODE>.
     * <P>
     * A client uses a <CODE>MessageConsumer</CODE> object to receive messages
     * that have been published to a destination.
     * <P>
     * In some cases, a connection may both publish and subscribe to a topic.
     * The consumer <CODE>NoLocal</CODE> attribute allows a consumer to
     * inhibit the delivery of messages published by its own connection. The
     * default value for this attribute is False. The <CODE>noLocal</CODE>
     * value must be supported by destinations that are topics.
     *
     * @param destination the <CODE>Destination</CODE> to access
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @param noLocal - if true, and the destination is a topic, inhibits the
     *                delivery of messages published by its own connection. The
     *                behavior for <CODE>NoLocal</CODE> is not specified if
     *                the destination is a queue.
     * @param messageListener the listener to use for async consumption of messages
     * @return the MessageConsumer
     * @throws JMSException if the session fails to create a MessageConsumer due
     *                 to some internal error.
     * @throws InvalidDestinationException if an invalid destination is
     *                 specified.
     * @throws InvalidSelectorException if the message selector is invalid.
     * @since 1.1
     */
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal, MessageListener messageListener) throws JMSException {
        checkClosed();

        if (destination instanceof CustomDestination) {
            CustomDestination customDestination = (CustomDestination)destination;
            return customDestination.createConsumer(this, messageSelector, noLocal);
        }

        ActiveMQPrefetchPolicy prefetchPolicy = connection.getPrefetchPolicy();
        int prefetch = 0;
        if (destination instanceof Topic) {
            prefetch = prefetchPolicy.getTopicPrefetch();
        } else {
            prefetch = prefetchPolicy.getQueuePrefetch();
        }
        ActiveMQDestination activemqDestination = ActiveMQMessageTransformation.transformDestination(destination);
        return new ActiveMQMessageConsumer(this, getNextConsumerId(), activemqDestination, null, messageSelector,
                prefetch, prefetchPolicy.getMaximumPendingMessageLimit(), noLocal, false, isAsyncDispatch(), messageListener);
    }

    /**
     * Creates a queue identity given a <CODE>Queue</CODE> name.
     * <P>
     * This facility is provided for the rare cases where clients need to
     * dynamically manipulate queue identity. It allows the creation of a queue
     * identity with a provider-specific name. Clients that depend on this
     * ability are not portable.
     * <P>
     * Note that this method is not for creating the physical queue. The
     * physical creation of queues is an administrative task and is not to be
     * initiated by the JMS API. The one exception is the creation of temporary
     * queues, which is accomplished with the <CODE>createTemporaryQueue</CODE>
     * method.
     * 
     * @param queueName the name of this <CODE>Queue</CODE>
     * @return a <CODE>Queue</CODE> with the given name
     * @throws JMSException if the session fails to create a queue due to some
     *                 internal error.
     * @since 1.1
     */
    public Queue createQueue(String queueName) throws JMSException {
        checkClosed();
        if (queueName.startsWith(ActiveMQDestination.TEMP_DESTINATION_NAME_PREFIX)) {
            return new ActiveMQTempQueue(queueName);
        }
        return new ActiveMQQueue(queueName);
    }

    /**
     * Creates a topic identity given a <CODE>Topic</CODE> name.
     * <P>
     * This facility is provided for the rare cases where clients need to
     * dynamically manipulate topic identity. This allows the creation of a
     * topic identity with a provider-specific name. Clients that depend on this
     * ability are not portable.
     * <P>
     * Note that this method is not for creating the physical topic. The
     * physical creation of topics is an administrative task and is not to be
     * initiated by the JMS API. The one exception is the creation of temporary
     * topics, which is accomplished with the <CODE>createTemporaryTopic</CODE>
     * method.
     * 
     * @param topicName the name of this <CODE>Topic</CODE>
     * @return a <CODE>Topic</CODE> with the given name
     * @throws JMSException if the session fails to create a topic due to some
     *                 internal error.
     * @since 1.1
     */
    public Topic createTopic(String topicName) throws JMSException {
        checkClosed();
        if (topicName.startsWith(ActiveMQDestination.TEMP_DESTINATION_NAME_PREFIX)) {
            return new ActiveMQTempTopic(topicName);
        }
        return new ActiveMQTopic(topicName);
    }

    /**
     * Creates a <CODE>QueueBrowser</CODE> object to peek at the messages on
     * the specified queue.
     * 
     * @param queue the <CODE>queue</CODE> to access
     * @exception InvalidDestinationException if an invalid destination is
     *                    specified
     * @since 1.1
     */
    /**
     * Creates a durable subscriber to the specified topic.
     * <P>
     * If a client needs to receive all the messages published on a topic,
     * including the ones published while the subscriber is inactive, it uses a
     * durable <CODE>TopicSubscriber</CODE>. The JMS provider retains a
     * record of this durable subscription and insures that all messages from
     * the topic's publishers are retained until they are acknowledged by this
     * durable subscriber or they have expired.
     * <P>
     * Sessions with durable subscribers must always provide the same client
     * identifier. In addition, each client must specify a name that uniquely
     * identifies (within client identifier) each durable subscription it
     * creates. Only one session at a time can have a
     * <CODE>TopicSubscriber</CODE> for a particular durable subscription.
     * <P>
     * A client can change an existing durable subscription by creating a
     * durable <CODE>TopicSubscriber</CODE> with the same name and a new topic
     * and/or message selector. Changing a durable subscriber is equivalent to
     * unsubscribing (deleting) the old one and creating a new one.
     * <P>
     * In some cases, a connection may both publish and subscribe to a topic.
     * The subscriber <CODE>NoLocal</CODE> attribute allows a subscriber to
     * inhibit the delivery of messages published by its own connection. The
     * default value for this attribute is false.
     * 
     * @param topic the non-temporary <CODE>Topic</CODE> to subscribe to
     * @param name the name used to identify this subscription
     * @return the TopicSubscriber
     * @throws JMSException if the session fails to create a subscriber due to
     *                 some internal error.
     * @throws InvalidDestinationException if an invalid topic is specified.
     * @since 1.1
     */
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        checkClosed();
        return createDurableSubscriber(topic, name, null, false);
    }

    /**
     * Creates a durable subscriber to the specified topic, using a message
     * selector and specifying whether messages published by its own connection
     * should be delivered to it.
     * <P>
     * If a client needs to receive all the messages published on a topic,
     * including the ones published while the subscriber is inactive, it uses a
     * durable <CODE>TopicSubscriber</CODE>. The JMS provider retains a
     * record of this durable subscription and insures that all messages from
     * the topic's publishers are retained until they are acknowledged by this
     * durable subscriber or they have expired.
     * <P>
     * Sessions with durable subscribers must always provide the same client
     * identifier. In addition, each client must specify a name which uniquely
     * identifies (within client identifier) each durable subscription it
     * creates. Only one session at a time can have a
     * <CODE>TopicSubscriber</CODE> for a particular durable subscription. An
     * inactive durable subscriber is one that exists but does not currently
     * have a message consumer associated with it.
     * <P>
     * A client can change an existing durable subscription by creating a
     * durable <CODE>TopicSubscriber</CODE> with the same name and a new topic
     * and/or message selector. Changing a durable subscriber is equivalent to
     * unsubscribing (deleting) the old one and creating a new one.
     * 
     * @param topic the non-temporary <CODE>Topic</CODE> to subscribe to
     * @param name the name used to identify this subscription
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @param noLocal if set, inhibits the delivery of messages published by its
     *                own connection
     * @return the Queue Browser
     * @throws JMSException if the session fails to create a subscriber due to
     *                 some internal error.
     * @throws InvalidDestinationException if an invalid topic is specified.
     * @throws InvalidSelectorException if the message selector is invalid.
     * @since 1.1
     */
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        checkClosed();

        if (topic instanceof CustomDestination) {
            CustomDestination customDestination = (CustomDestination)topic;
            return customDestination.createDurableSubscriber(this, name, messageSelector, noLocal);
        }

        connection.checkClientIDWasManuallySpecified();
        ActiveMQPrefetchPolicy prefetchPolicy = this.connection.getPrefetchPolicy();
        int prefetch = isAutoAcknowledge() && connection.isOptimizedMessageDispatch() ? prefetchPolicy.getOptimizeDurableTopicPrefetch() : prefetchPolicy.getDurableTopicPrefetch();
        int maxPrendingLimit = prefetchPolicy.getMaximumPendingMessageLimit();
        return new ActiveMQTopicSubscriber(this, getNextConsumerId(), ActiveMQMessageTransformation.transformDestination(topic), name, messageSelector, prefetch, maxPrendingLimit,
                                           noLocal, false, asyncDispatch);
    }

    /**
     * Creates a <CODE>QueueBrowser</CODE> object to peek at the messages on
     * the specified queue.
     * 
     * @param queue the <CODE>queue</CODE> to access
     * @return the Queue Browser
     * @throws JMSException if the session fails to create a browser due to some
     *                 internal error.
     * @throws InvalidDestinationException if an invalid destination is
     *                 specified
     * @since 1.1
     */
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        checkClosed();
        return createBrowser(queue, null);
    }

    /**
     * Creates a <CODE>QueueBrowser</CODE> object to peek at the messages on
     * the specified queue using a message selector.
     * 
     * @param queue the <CODE>queue</CODE> to access
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @return the Queue Browser
     * @throws JMSException if the session fails to create a browser due to some
     *                 internal error.
     * @throws InvalidDestinationException if an invalid destination is
     *                 specified
     * @throws InvalidSelectorException if the message selector is invalid.
     * @since 1.1
     */
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        checkClosed();
        return new ActiveMQQueueBrowser(this, getNextConsumerId(), ActiveMQMessageTransformation.transformDestination(queue), messageSelector, asyncDispatch);
    }

    /**
     * Creates a <CODE>TemporaryQueue</CODE> object. Its lifetime will be that
     * of the <CODE>Connection</CODE> unless it is deleted earlier.
     * 
     * @return a temporary queue identity
     * @throws JMSException if the session fails to create a temporary queue due
     *                 to some internal error.
     * @since 1.1
     */
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        checkClosed();
        return (TemporaryQueue)connection.createTempDestination(false);
    }

    /**
     * Creates a <CODE>TemporaryTopic</CODE> object. Its lifetime will be that
     * of the <CODE>Connection</CODE> unless it is deleted earlier.
     * 
     * @return a temporary topic identity
     * @throws JMSException if the session fails to create a temporary topic due
     *                 to some internal error.
     * @since 1.1
     */
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        checkClosed();
        return (TemporaryTopic)connection.createTempDestination(true);
    }

    /**
     * Creates a <CODE>QueueReceiver</CODE> object to receive messages from
     * the specified queue.
     * 
     * @param queue the <CODE>Queue</CODE> to access
     * @return
     * @throws JMSException if the session fails to create a receiver due to
     *                 some internal error.
     * @throws JMSException
     * @throws InvalidDestinationException if an invalid queue is specified.
     */
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        checkClosed();
        return createReceiver(queue, null);
    }

    /**
     * Creates a <CODE>QueueReceiver</CODE> object to receive messages from
     * the specified queue using a message selector.
     * 
     * @param queue the <CODE>Queue</CODE> to access
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @return QueueReceiver
     * @throws JMSException if the session fails to create a receiver due to
     *                 some internal error.
     * @throws InvalidDestinationException if an invalid queue is specified.
     * @throws InvalidSelectorException if the message selector is invalid.
     */
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        checkClosed();

        if (queue instanceof CustomDestination) {
            CustomDestination customDestination = (CustomDestination)queue;
            return customDestination.createReceiver(this, messageSelector);
        }

        ActiveMQPrefetchPolicy prefetchPolicy = this.connection.getPrefetchPolicy();
        return new ActiveMQQueueReceiver(this, getNextConsumerId(), ActiveMQMessageTransformation.transformDestination(queue), messageSelector, prefetchPolicy.getQueuePrefetch(),
                                         prefetchPolicy.getMaximumPendingMessageLimit(), asyncDispatch);
    }

    /**
     * Creates a <CODE>QueueSender</CODE> object to send messages to the
     * specified queue.
     * 
     * @param queue the <CODE>Queue</CODE> to access, or null if this is an
     *                unidentified producer
     * @return QueueSender
     * @throws JMSException if the session fails to create a sender due to some
     *                 internal error.
     * @throws InvalidDestinationException if an invalid queue is specified.
     */
    public QueueSender createSender(Queue queue) throws JMSException {
        checkClosed();
        if (queue instanceof CustomDestination) {
            CustomDestination customDestination = (CustomDestination)queue;
            return customDestination.createSender(this);
        }
        int timeSendOut = connection.getSendTimeout();
        return new ActiveMQQueueSender(this, ActiveMQMessageTransformation.transformDestination(queue),timeSendOut);
    }

    /**
     * Creates a nondurable subscriber to the specified topic. <p/>
     * <P>
     * A client uses a <CODE>TopicSubscriber</CODE> object to receive messages
     * that have been published to a topic. <p/>
     * <P>
     * Regular <CODE>TopicSubscriber</CODE> objects are not durable. They
     * receive only messages that are published while they are active. <p/>
     * <P>
     * In some cases, a connection may both publish and subscribe to a topic.
     * The subscriber <CODE>NoLocal</CODE> attribute allows a subscriber to
     * inhibit the delivery of messages published by its own connection. The
     * default value for this attribute is false.
     * 
     * @param topic the <CODE>Topic</CODE> to subscribe to
     * @return TopicSubscriber
     * @throws JMSException if the session fails to create a subscriber due to
     *                 some internal error.
     * @throws InvalidDestinationException if an invalid topic is specified.
     */
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        checkClosed();
        return createSubscriber(topic, null, false);
    }

    /**
     * Creates a nondurable subscriber to the specified topic, using a message
     * selector or specifying whether messages published by its own connection
     * should be delivered to it. <p/>
     * <P>
     * A client uses a <CODE>TopicSubscriber</CODE> object to receive messages
     * that have been published to a topic. <p/>
     * <P>
     * Regular <CODE>TopicSubscriber</CODE> objects are not durable. They
     * receive only messages that are published while they are active. <p/>
     * <P>
     * Messages filtered out by a subscriber's message selector will never be
     * delivered to the subscriber. From the subscriber's perspective, they do
     * not exist. <p/>
     * <P>
     * In some cases, a connection may both publish and subscribe to a topic.
     * The subscriber <CODE>NoLocal</CODE> attribute allows a subscriber to
     * inhibit the delivery of messages published by its own connection. The
     * default value for this attribute is false.
     * 
     * @param topic the <CODE>Topic</CODE> to subscribe to
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @param noLocal if set, inhibits the delivery of messages published by its
     *                own connection
     * @return TopicSubscriber
     * @throws JMSException if the session fails to create a subscriber due to
     *                 some internal error.
     * @throws InvalidDestinationException if an invalid topic is specified.
     * @throws InvalidSelectorException if the message selector is invalid.
     */
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        checkClosed();

        if (topic instanceof CustomDestination) {
            CustomDestination customDestination = (CustomDestination)topic;
            return customDestination.createSubscriber(this, messageSelector, noLocal);
        }

        ActiveMQPrefetchPolicy prefetchPolicy = this.connection.getPrefetchPolicy();
        return new ActiveMQTopicSubscriber(this, getNextConsumerId(), ActiveMQMessageTransformation.transformDestination(topic), null, messageSelector, prefetchPolicy
            .getTopicPrefetch(), prefetchPolicy.getMaximumPendingMessageLimit(), noLocal, false, asyncDispatch);
    }

    /**
     * Creates a publisher for the specified topic. <p/>
     * <P>
     * A client uses a <CODE>TopicPublisher</CODE> object to publish messages
     * on a topic. Each time a client creates a <CODE>TopicPublisher</CODE> on
     * a topic, it defines a new sequence of messages that have no ordering
     * relationship with the messages it has previously sent.
     * 
     * @param topic the <CODE>Topic</CODE> to publish to, or null if this is
     *                an unidentified producer
     * @return TopicPublisher
     * @throws JMSException if the session fails to create a publisher due to
     *                 some internal error.
     * @throws InvalidDestinationException if an invalid topic is specified.
     */
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        checkClosed();

        if (topic instanceof CustomDestination) {
            CustomDestination customDestination = (CustomDestination)topic;
            return customDestination.createPublisher(this);
        }
        int timeSendOut = connection.getSendTimeout();
        return new ActiveMQTopicPublisher(this, ActiveMQMessageTransformation.transformDestination(topic),timeSendOut);
    }

    /**
     * Unsubscribes a durable subscription that has been created by a client.
     * <P>
     * This method deletes the state being maintained on behalf of the
     * subscriber by its provider.
     * <P>
     * It is erroneous for a client to delete a durable subscription while there
     * is an active <CODE>MessageConsumer </CODE> or
     * <CODE>TopicSubscriber</CODE> for the subscription, or while a consumed
     * message is part of a pending transaction or has not been acknowledged in
     * the session.
     * 
     * @param name the name used to identify this subscription
     * @throws JMSException if the session fails to unsubscribe to the durable
     *                 subscription due to some internal error.
     * @throws InvalidDestinationException if an invalid subscription name is
     *                 specified.
     * @since 1.1
     */
    public void unsubscribe(String name) throws JMSException {
        checkClosed();
        connection.unsubscribe(name);
    }

    public void dispatch(MessageDispatch messageDispatch) {
        try {
            executor.execute(messageDispatch);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            connection.onClientInternalException(e);
        }
    }

    /**
     * Acknowledges all consumed messages of the session of this consumed
     * message.
     * <P>
     * All consumed JMS messages support the <CODE>acknowledge</CODE> method
     * for use when a client has specified that its JMS session's consumed
     * messages are to be explicitly acknowledged. By invoking
     * <CODE>acknowledge</CODE> on a consumed message, a client acknowledges
     * all messages consumed by the session that the message was delivered to.
     * <P>
     * Calls to <CODE>acknowledge</CODE> are ignored for both transacted
     * sessions and sessions specified to use implicit acknowledgement modes.
     * <P>
     * A client may individually acknowledge each message as it is consumed, or
     * it may choose to acknowledge messages as an application-defined group
     * (which is done by calling acknowledge on the last received message of the
     * group, thereby acknowledging all messages consumed by the session.)
     * <P>
     * Messages that have been received but not acknowledged may be redelivered.
     * 
     * @throws JMSException if the JMS provider fails to acknowledge the
     *                 messages due to some internal error.
     * @throws javax.jms.IllegalStateException if this method is called on a
     *                 closed session.
     * @see javax.jms.Session#CLIENT_ACKNOWLEDGE
     */
    public void acknowledge() throws JMSException {
        for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
            ActiveMQMessageConsumer c = iter.next();
            c.acknowledge();
        }
    }

    /**
     * Add a message consumer.
     * 
     * @param consumer - message consumer.
     * @throws JMSException
     */
    protected void addConsumer(ActiveMQMessageConsumer consumer) throws JMSException {
        this.consumers.add(consumer);
        if (consumer.isDurableSubscriber()) {
            stats.onCreateDurableSubscriber();
        }
        this.connection.addDispatcher(consumer.getConsumerId(), this);
    }

    /**
     * Remove the message consumer.
     * 
     * @param consumer - consumer to be removed.
     * @throws JMSException
     */
    protected void removeConsumer(ActiveMQMessageConsumer consumer) {
        this.connection.removeDispatcher(consumer.getConsumerId());
        if (consumer.isDurableSubscriber()) {
            stats.onRemoveDurableSubscriber();
        }
        this.consumers.remove(consumer);
        this.connection.removeDispatcher(consumer);
    }

    /**
     * Adds a message producer.
     * 
     * @param producer - message producer to be added.
     * @throws JMSException
     */
    protected void addProducer(ActiveMQMessageProducer producer) throws JMSException {
        this.producers.add(producer);
        this.connection.addProducer(producer.getProducerInfo().getProducerId(), producer);
    }

    /**
     * Removes a message producer.
     * 
     * @param producer - message producer to be removed.
     * @throws JMSException
     */
    protected void removeProducer(ActiveMQMessageProducer producer) {
        this.connection.removeProducer(producer.getProducerInfo().getProducerId());
        this.producers.remove(producer);
    }

    /**
     * Start this Session.
     * 
     * @throws JMSException
     */
    protected void start() throws JMSException {
        started.set(true);
        for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
            ActiveMQMessageConsumer c = iter.next();
            c.start();
        }
        executor.start();
    }

    /**
     * Stops this session.
     * 
     * @throws JMSException
     */
    protected void stop() throws JMSException {

        for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
            ActiveMQMessageConsumer c = iter.next();
            c.stop();
        }

        started.set(false);
        executor.stop();
    }

    /**
     * Returns the session id.
     * 
     * @return value - session id.
     */
    protected SessionId getSessionId() {
        return info.getSessionId();
    }

    /**
     * @return
     */
    protected ConsumerId getNextConsumerId() {
        return new ConsumerId(info.getSessionId(), consumerIdGenerator.getNextSequenceId());
    }

    /**
     * @return
     */
    protected ProducerId getNextProducerId() {
        return new ProducerId(info.getSessionId(), producerIdGenerator.getNextSequenceId());
    }

    /**
     * Sends the message for dispatch by the broker.
     * 
     * @param producer - message producer.
     * @param destination - message destination.
     * @param message - message to be sent.
     * @param deliveryMode - JMS messsage delivery mode.
     * @param priority - message priority.
     * @param timeToLive - message expiration.
     * @param producerWindow
     * @throws JMSException
     */
    protected void send(ActiveMQMessageProducer producer, ActiveMQDestination destination, Message message, int deliveryMode, int priority, long timeToLive,
                        MemoryUsage producerWindow, int sendTimeout) throws JMSException {

        checkClosed();
        if (destination.isTemporary() && connection.isDeleted(destination)) {
            throw new InvalidDestinationException("Cannot publish to a deleted Destination: " + destination);
        }
        synchronized (sendMutex) {
            // tell the Broker we are about to start a new transaction
            doStartTransaction();
            TransactionId txid = transactionContext.getTransactionId();
            long sequenceNumber = producer.getMessageSequence();

            //Set the "JMS" header fields on the orriginal message, see 1.1 spec section 3.4.11
            message.setJMSDestination(destination);
            message.setJMSDeliveryMode(deliveryMode);
            long expiration = 0L;
            if (!producer.getDisableMessageTimestamp()) {
                long timeStamp = System.currentTimeMillis();
                message.setJMSTimestamp(timeStamp);
                if (timeToLive > 0) {
                    expiration = timeToLive + timeStamp;
                }
            }
            message.setJMSExpiration(expiration);
            message.setJMSPriority(priority);
            message.setJMSRedelivered(false);

            // transform to our own message format here
            ActiveMQMessage msg = ActiveMQMessageTransformation.transformMessage(message, connection);

            // Set the message id.
            if (msg == message) {
                msg.setMessageId(new MessageId(producer.getProducerInfo().getProducerId(), sequenceNumber));
            } else {
                msg.setMessageId(new MessageId(producer.getProducerInfo().getProducerId(), sequenceNumber));
                message.setJMSMessageID(msg.getMessageId().toString());
            }
            //clear the brokerPath in case we are re-sending this message
            msg.setBrokerPath(null);


            msg.setTransactionId(txid);
            if (connection.isCopyMessageOnSend()) {
                msg = (ActiveMQMessage)msg.copy();
            }
            msg.setConnection(connection);
            msg.onSend();
            msg.setProducerId(msg.getMessageId().getProducerId());
            if (this.debug) {
                LOG.debug(getSessionId() + " sending message: " + msg);
            }
            if (sendTimeout <= 0 && !msg.isResponseRequired() && !connection.isAlwaysSyncSend() && (!msg.isPersistent() || connection.isUseAsyncSend() || txid != null)) {
                this.connection.asyncSendPacket(msg);
                if (producerWindow != null) {
                    // Since we defer lots of the marshaling till we hit the
                    // wire, this might not
                    // provide and accurate size. We may change over to doing
                    // more aggressive marshaling,
                    // to get more accurate sizes.. this is more important once
                    // users start using producer window
                    // flow control.
                    int size = msg.getSize();
                    producerWindow.increaseUsage(size);
                }
            } else {
                if (sendTimeout > 0) {
                    this.connection.syncSendPacket(msg,sendTimeout);
                }else {
                    this.connection.syncSendPacket(msg);
                }
            }

        }
    }

    /**
     * Send TransactionInfo to indicate transaction has started
     * 
     * @throws JMSException if some internal error occurs
     */
    protected void doStartTransaction() throws JMSException {
        if (getTransacted() && !transactionContext.isInXATransaction()) {
            transactionContext.begin();
        }
    }

    /**
     * Checks whether the session has unconsumed messages.
     * 
     * @return true - if there are unconsumed messages.
     */
    public boolean hasUncomsumedMessages() {
        return executor.hasUncomsumedMessages();
    }

    /**
     * Checks whether the session uses transactions.
     * 
     * @return true - if the session uses transactions.
     */
    public boolean isTransacted() {
        return this.acknowledgementMode == Session.SESSION_TRANSACTED;
    }

    /**
     * Checks whether the session used client acknowledgment.
     * 
     * @return true - if the session uses client acknowledgment.
     */
    protected boolean isClientAcknowledge() {
        return this.acknowledgementMode == Session.CLIENT_ACKNOWLEDGE;
    }

    /**
     * Checks whether the session used auto acknowledgment.
     * 
     * @return true - if the session uses client acknowledgment.
     */
    public boolean isAutoAcknowledge() {
        return acknowledgementMode == Session.AUTO_ACKNOWLEDGE;
    }

    /**
     * Checks whether the session used dup ok acknowledgment.
     * 
     * @return true - if the session uses client acknowledgment.
     */
    public boolean isDupsOkAcknowledge() {
        return acknowledgementMode == Session.DUPS_OK_ACKNOWLEDGE;
    }
    
    public boolean isIndividualAcknowledge(){
    	return acknowledgementMode == ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE;
    }

    /**
     * Returns the message delivery listener.
     * 
     * @return deliveryListener - message delivery listener.
     */
    public DeliveryListener getDeliveryListener() {
        return deliveryListener;
    }

    /**
     * Sets the message delivery listener.
     * 
     * @param deliveryListener - message delivery listener.
     */
    public void setDeliveryListener(DeliveryListener deliveryListener) {
        this.deliveryListener = deliveryListener;
    }

    /**
     * Returns the SessionInfo bean.
     * 
     * @return info - SessionInfo bean.
     * @throws JMSException
     */
    protected SessionInfo getSessionInfo() throws JMSException {
        SessionInfo info = new SessionInfo(connection.getConnectionInfo(), getSessionId().getValue());
        return info;
    }

    /**
     * Send the asynchronus command.
     * 
     * @param command - command to be executed.
     * @throws JMSException
     */
    public void asyncSendPacket(Command command) throws JMSException {
        connection.asyncSendPacket(command);
    }

    /**
     * Send the synchronus command.
     * 
     * @param command - command to be executed.
     * @return Response
     * @throws JMSException
     */
    public Response syncSendPacket(Command command) throws JMSException {
        return connection.syncSendPacket(command);
    }

    public long getNextDeliveryId() {
        return deliveryIdGenerator.getNextSequenceId();
    }

    public void redispatch(ActiveMQDispatcher dispatcher, MessageDispatchChannel unconsumedMessages) throws JMSException {

        List<MessageDispatch> c = unconsumedMessages.removeAll();
        for (MessageDispatch md : c) {
            this.connection.rollbackDuplicate(dispatcher, md.getMessage());
        }
        Collections.reverse(c);

        for (Iterator<MessageDispatch> iter = c.iterator(); iter.hasNext();) {
            MessageDispatch md = iter.next();
            executor.executeFirst(md);
        }

    }

    public boolean isRunning() {
        return started.get();
    }

    public boolean isAsyncDispatch() {
        return asyncDispatch;
    }

    public void setAsyncDispatch(boolean asyncDispatch) {
        this.asyncDispatch = asyncDispatch;
    }

    /**
     * @return Returns the sessionAsyncDispatch.
     */
    public boolean isSessionAsyncDispatch() {
        return sessionAsyncDispatch;
    }

    /**
     * @param sessionAsyncDispatch The sessionAsyncDispatch to set.
     */
    public void setSessionAsyncDispatch(boolean sessionAsyncDispatch) {
        this.sessionAsyncDispatch = sessionAsyncDispatch;
    }

    public MessageTransformer getTransformer() {
        return transformer;
    }

    public ActiveMQConnection getConnection() {
        return connection;
    }

    /**
     * Sets the transformer used to transform messages before they are sent on
     * to the JMS bus or when they are received from the bus but before they are
     * delivered to the JMS client
     */
    public void setTransformer(MessageTransformer transformer) {
        this.transformer = transformer;
    }

    public BlobTransferPolicy getBlobTransferPolicy() {
        return blobTransferPolicy;
    }

    /**
     * Sets the policy used to describe how out-of-band BLOBs (Binary Large
     * OBjects) are transferred from producers to brokers to consumers
     */
    public void setBlobTransferPolicy(BlobTransferPolicy blobTransferPolicy) {
        this.blobTransferPolicy = blobTransferPolicy;
    }

    public List getUnconsumedMessages() {
        return executor.getUnconsumedMessages();
    }

    public String toString() {
        return "ActiveMQSession {id=" + info.getSessionId() + ",started=" + started.get() + "}";
    }

    public void checkMessageListener() throws JMSException {
        if (messageListener != null) {
            throw new IllegalStateException("Cannot synchronously receive a message when a MessageListener is set");
        }
        for (Iterator<ActiveMQMessageConsumer> i = consumers.iterator(); i.hasNext();) {
            ActiveMQMessageConsumer consumer = i.next();
            if (consumer.getMessageListener() != null) {
                throw new IllegalStateException("Cannot synchronously receive a message when a MessageListener is set");
            }
        }
    }

    protected void setOptimizeAcknowledge(boolean value) {
        for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
            ActiveMQMessageConsumer c = iter.next();
            c.setOptimizeAcknowledge(value);
        }
    }

    protected void setPrefetchSize(ConsumerId id, int prefetch) {
        for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
            ActiveMQMessageConsumer c = iter.next();
            if (c.getConsumerId().equals(id)) {
                c.setPrefetchSize(prefetch);
                break;
            }
        }
    }

    protected void close(ConsumerId id) {
        for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
            ActiveMQMessageConsumer c = iter.next();
            if (c.getConsumerId().equals(id)) {
                try {
                    c.close();
                } catch (JMSException e) {
                    LOG.warn("Exception closing consumer", e);
                }
                LOG.warn("Closed consumer on Command");
                break;
            }
        }
    }

    public boolean isInUse(ActiveMQTempDestination destination) {
        for (Iterator<ActiveMQMessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
            ActiveMQMessageConsumer c = iter.next();
            if (c.isInUse(destination)) {
                return true;
            }
        }
        return false;
    }
    
    protected void sendAck(MessageAck ack) throws JMSException {
        sendAck(ack,false);
    }
    
    protected void sendAck(MessageAck ack, boolean lazy) throws JMSException {
        if (lazy || connection.isSendAcksAsync() || isTransacted()) {
            asyncSendPacket(ack);
        } else {
            syncSendPacket(ack);
        }
    }

}
