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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import org.apache.activemq.advisory.DestinationSource;
import org.apache.activemq.blob.BlobTransferPolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ControlCommand;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.management.JMSConnectionStatsImpl;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.management.StatsCapable;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.state.CommandVisitorAdapter;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ActiveMQConnection implements Connection, TopicConnection, QueueConnection, StatsCapable, Closeable, StreamConnection, TransportListener, EnhancedConnection {

    public static final String DEFAULT_USER = ActiveMQConnectionFactory.DEFAULT_USER;
    public static final String DEFAULT_PASSWORD = ActiveMQConnectionFactory.DEFAULT_PASSWORD;
    public static final String DEFAULT_BROKER_URL = ActiveMQConnectionFactory.DEFAULT_BROKER_URL;

    private static final Log LOG = LogFactory.getLog(ActiveMQConnection.class);
    private static final IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();

    public final ConcurrentHashMap<ActiveMQTempDestination, ActiveMQTempDestination> activeTempDestinations = new ConcurrentHashMap<ActiveMQTempDestination, ActiveMQTempDestination>();

    protected boolean dispatchAsync=true;
    protected boolean alwaysSessionAsync = true;

    private TaskRunnerFactory sessionTaskRunner;
    private final ThreadPoolExecutor executor;

    // Connection state variables
    private final ConnectionInfo info;
    private ExceptionListener exceptionListener;
    private ClientInternalExceptionListener clientInternalExceptionListener;
    private boolean clientIDSet;
    private boolean isConnectionInfoSentToBroker;
    private boolean userSpecifiedClientID;

    // Configuration options variables
    private ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
    private BlobTransferPolicy blobTransferPolicy;
    private RedeliveryPolicy redeliveryPolicy;
    private MessageTransformer transformer;

    private boolean disableTimeStampsByDefault;
    private boolean optimizedMessageDispatch = true;
    private boolean copyMessageOnSend = true;
    private boolean useCompression;
    private boolean objectMessageSerializationDefered;
    private boolean useAsyncSend;
    private boolean optimizeAcknowledge;
    private boolean nestedMapAndListEnabled = true;
    private boolean useRetroactiveConsumer;
    private boolean exclusiveConsumer;
    private boolean alwaysSyncSend;
    private int closeTimeout = 15000;
    private boolean watchTopicAdvisories = true;
    private long warnAboutUnstartedConnectionTimeout = 500L;
    private int sendTimeout =0;
    private boolean sendAcksAsync=true;
    private boolean checkForDuplicates = true;

    private final Transport transport;
    private final IdGenerator clientIdGenerator;
    private final JMSStatsImpl factoryStats;
    private final JMSConnectionStatsImpl stats;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean transportFailed = new AtomicBoolean(false);
    private final CopyOnWriteArrayList<ActiveMQSession> sessions = new CopyOnWriteArrayList<ActiveMQSession>();
    private final CopyOnWriteArrayList<ActiveMQConnectionConsumer> connectionConsumers = new CopyOnWriteArrayList<ActiveMQConnectionConsumer>();
    private final CopyOnWriteArrayList<ActiveMQInputStream> inputStreams = new CopyOnWriteArrayList<ActiveMQInputStream>();
    private final CopyOnWriteArrayList<ActiveMQOutputStream> outputStreams = new CopyOnWriteArrayList<ActiveMQOutputStream>();
    private final CopyOnWriteArrayList<TransportListener> transportListeners = new CopyOnWriteArrayList<TransportListener>();

    // Maps ConsumerIds to ActiveMQConsumer objects
    private final ConcurrentHashMap<ConsumerId, ActiveMQDispatcher> dispatchers = new ConcurrentHashMap<ConsumerId, ActiveMQDispatcher>();
    private final ConcurrentHashMap<ProducerId, ActiveMQMessageProducer> producers = new ConcurrentHashMap<ProducerId, ActiveMQMessageProducer>();
    private final LongSequenceGenerator sessionIdGenerator = new LongSequenceGenerator();
    private final SessionId connectionSessionId;
    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator producerIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator tempDestinationIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator localTransactionIdGenerator = new LongSequenceGenerator();

    private AdvisoryConsumer advisoryConsumer;
    private final CountDownLatch brokerInfoReceived = new CountDownLatch(1);
    private BrokerInfo brokerInfo;
    private IOException firstFailureError;
    private int producerWindowSize = ActiveMQConnectionFactory.DEFAULT_PRODUCER_WINDOW_SIZE;

    // Assume that protocol is the latest. Change to the actual protocol
    // version when a WireFormatInfo is received.
    private final AtomicInteger protocolVersion = new AtomicInteger(CommandTypes.PROTOCOL_VERSION);
    private final long timeCreated;
    private final ConnectionAudit connectionAudit = new ConnectionAudit();
    private DestinationSource destinationSource;
    private final Object ensureConnectionInfoSentMutex = new Object();
    private boolean useDedicatedTaskRunner;
    protected volatile CountDownLatch transportInterruptionProcessingComplete;
    private long consumerFailoverRedeliveryWaitPeriod;
    private final Scheduler scheduler;

    /**
     * Construct an <code>ActiveMQConnection</code>
     * 
     * @param transport
     * @param factoryStats
     * @throws Exception
     */
    protected ActiveMQConnection(final Transport transport, IdGenerator clientIdGenerator, JMSStatsImpl factoryStats) throws Exception {

        this.transport = transport;
        this.clientIdGenerator = clientIdGenerator;
        this.factoryStats = factoryStats;

        // Configure a single threaded executor who's core thread can timeout if
        // idle
        executor = new ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "ActiveMQ Connection Executor: " + transport);
                thread.setDaemon(true);
                return thread;
            }
        });
        // asyncConnectionThread.allowCoreThreadTimeOut(true);
        String uniqueId = CONNECTION_ID_GENERATOR.generateId();
        this.info = new ConnectionInfo(new ConnectionId(uniqueId));
        this.info.setManageable(true);
        this.info.setFaultTolerant(transport.isFaultTolerant());
        this.connectionSessionId = new SessionId(info.getConnectionId(), -1);

        this.transport.setTransportListener(this);

        this.stats = new JMSConnectionStatsImpl(sessions, this instanceof XAConnection);
        this.factoryStats.addConnection(this);
        this.timeCreated = System.currentTimeMillis();
        this.connectionAudit.setCheckForDuplicates(transport.isFaultTolerant());
        this.scheduler = new Scheduler("ActiveMQConnection["+uniqueId+"] Scheduler");
        this.scheduler.start();
    }

    protected void setUserName(String userName) {
        this.info.setUserName(userName);
    }

    protected void setPassword(String password) {
        this.info.setPassword(password);
    }

    /**
     * A static helper method to create a new connection
     * 
     * @return an ActiveMQConnection
     * @throws JMSException
     */
    public static ActiveMQConnection makeConnection() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        return (ActiveMQConnection)factory.createConnection();
    }

    /**
     * A static helper method to create a new connection
     * 
     * @param uri
     * @return and ActiveMQConnection
     * @throws JMSException
     */
    public static ActiveMQConnection makeConnection(String uri) throws JMSException, URISyntaxException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri);
        return (ActiveMQConnection)factory.createConnection();
    }

    /**
     * A static helper method to create a new connection
     * 
     * @param user
     * @param password
     * @param uri
     * @return an ActiveMQConnection
     * @throws JMSException
     */
    public static ActiveMQConnection makeConnection(String user, String password, String uri) throws JMSException, URISyntaxException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(user, password, new URI(uri));
        return (ActiveMQConnection)factory.createConnection();
    }

    /**
     * @return a number unique for this connection
     */
    public JMSConnectionStatsImpl getConnectionStats() {
        return stats;
    }

    /**
     * Creates a <CODE>Session</CODE> object.
     * 
     * @param transacted indicates whether the session is transacted
     * @param acknowledgeMode indicates whether the consumer or the client will
     *                acknowledge any messages it receives; ignored if the
     *                session is transacted. Legal values are
     *                <code>Session.AUTO_ACKNOWLEDGE</code>,
     *                <code>Session.CLIENT_ACKNOWLEDGE</code>, and
     *                <code>Session.DUPS_OK_ACKNOWLEDGE</code>.
     * @return a newly created session
     * @throws JMSException if the <CODE>Connection</CODE> object fails to
     *                 create a session due to some internal error or lack of
     *                 support for the specific transaction and acknowledgement
     *                 mode.
     * @see Session#AUTO_ACKNOWLEDGE
     * @see Session#CLIENT_ACKNOWLEDGE
     * @see Session#DUPS_OK_ACKNOWLEDGE
     * @since 1.1
     */
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        ensureConnectionInfoSent();
        if(!transacted) {
            if (acknowledgeMode==Session.SESSION_TRANSACTED) {
                throw new JMSException("acknowledgeMode SESSION_TRANSACTED cannot be used for an non-transacted Session");
            } else if (acknowledgeMode < Session.SESSION_TRANSACTED || acknowledgeMode > ActiveMQSession.MAX_ACK_CONSTANT) {
                throw new JMSException("invalid acknowledgeMode: " + acknowledgeMode + ". Valid values are Session.AUTO_ACKNOWLEDGE (1), " +
                        "Session.CLIENT_ACKNOWLEDGE (2), Session.DUPS_OK_ACKNOWLEDGE (3), ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE (4) or for transacted sessions Session.SESSION_TRANSACTED (0)");
            }
        }
        return new ActiveMQSession(this, getNextSessionId(), transacted ? Session.SESSION_TRANSACTED : (acknowledgeMode == Session.SESSION_TRANSACTED
            ? Session.AUTO_ACKNOWLEDGE : acknowledgeMode), isDispatchAsync(), isAlwaysSessionAsync());
    }

    /**
     * @return sessionId
     */
    protected SessionId getNextSessionId() {
        return new SessionId(info.getConnectionId(), sessionIdGenerator.getNextSequenceId());
    }

    /**
     * Gets the client identifier for this connection.
     * <P>
     * This value is specific to the JMS provider. It is either preconfigured by
     * an administrator in a <CODE> ConnectionFactory</CODE> object or assigned
     * dynamically by the application by calling the <code>setClientID</code>
     * method.
     * 
     * @return the unique client identifier
     * @throws JMSException if the JMS provider fails to return the client ID
     *                 for this connection due to some internal error.
     */
    public String getClientID() throws JMSException {
        checkClosedOrFailed();
        return this.info.getClientId();
    }

    /**
     * Sets the client identifier for this connection.
     * <P>
     * The preferred way to assign a JMS client's client identifier is for it to
     * be configured in a client-specific <CODE>ConnectionFactory</CODE>
     * object and transparently assigned to the <CODE>Connection</CODE> object
     * it creates.
     * <P>
     * Alternatively, a client can set a connection's client identifier using a
     * provider-specific value. The facility to set a connection's client
     * identifier explicitly is not a mechanism for overriding the identifier
     * that has been administratively configured. It is provided for the case
     * where no administratively specified identifier exists. If one does exist,
     * an attempt to change it by setting it must throw an
     * <CODE>IllegalStateException</CODE>. If a client sets the client
     * identifier explicitly, it must do so immediately after it creates the
     * connection and before any other action on the connection is taken. After
     * this point, setting the client identifier is a programming error that
     * should throw an <CODE>IllegalStateException</CODE>.
     * <P>
     * The purpose of the client identifier is to associate a connection and its
     * objects with a state maintained on behalf of the client by a provider.
     * The only such state identified by the JMS API is that required to support
     * durable subscriptions.
     * <P>
     * If another connection with the same <code>clientID</code> is already
     * running when this method is called, the JMS provider should detect the
     * duplicate ID and throw an <CODE>InvalidClientIDException</CODE>.
     * 
     * @param newClientID the unique client identifier
     * @throws JMSException if the JMS provider fails to set the client ID for
     *                 this connection due to some internal error.
     * @throws javax.jms.InvalidClientIDException if the JMS client specifies an
     *                 invalid or duplicate client ID.
     * @throws javax.jms.IllegalStateException if the JMS client attempts to set
     *                 a connection's client ID at the wrong time or when it has
     *                 been administratively configured.
     */
    public void setClientID(String newClientID) throws JMSException {
        checkClosedOrFailed();

        if (this.clientIDSet) {
            throw new IllegalStateException("The clientID has already been set");
        }

        if (this.isConnectionInfoSentToBroker) {
            throw new IllegalStateException("Setting clientID on a used Connection is not allowed");
        }

        this.info.setClientId(newClientID);
        this.userSpecifiedClientID = true;
        ensureConnectionInfoSent();
    }

    /**
     * Sets the default client id that the connection will use if explicitly not
     * set with the setClientId() call.
     */
    public void setDefaultClientID(String clientID) throws JMSException {
        this.info.setClientId(clientID);
        this.userSpecifiedClientID = true;
    }

    /**
     * Gets the metadata for this connection.
     * 
     * @return the connection metadata
     * @throws JMSException if the JMS provider fails to get the connection
     *                 metadata for this connection.
     * @see javax.jms.ConnectionMetaData
     */
    public ConnectionMetaData getMetaData() throws JMSException {
        checkClosedOrFailed();
        return ActiveMQConnectionMetaData.INSTANCE;
    }

    /**
     * Gets the <CODE>ExceptionListener</CODE> object for this connection. Not
     * every <CODE>Connection</CODE> has an <CODE>ExceptionListener</CODE>
     * associated with it.
     * 
     * @return the <CODE>ExceptionListener</CODE> for this connection, or
     *         null, if no <CODE>ExceptionListener</CODE> is associated with
     *         this connection.
     * @throws JMSException if the JMS provider fails to get the
     *                 <CODE>ExceptionListener</CODE> for this connection.
     * @see javax.jms.Connection#setExceptionListener(ExceptionListener)
     */
    public ExceptionListener getExceptionListener() throws JMSException {
        checkClosedOrFailed();
        return this.exceptionListener;
    }

    /**
     * Sets an exception listener for this connection.
     * <P>
     * If a JMS provider detects a serious problem with a connection, it informs
     * the connection's <CODE> ExceptionListener</CODE>, if one has been
     * registered. It does this by calling the listener's <CODE>onException
     * </CODE>
     * method, passing it a <CODE>JMSException</CODE> object describing the
     * problem.
     * <P>
     * An exception listener allows a client to be notified of a problem
     * asynchronously. Some connections only consume messages, so they would
     * have no other way to learn their connection has failed.
     * <P>
     * A connection serializes execution of its <CODE>ExceptionListener</CODE>.
     * <P>
     * A JMS provider should attempt to resolve connection problems itself
     * before it notifies the client of them.
     * 
     * @param listener the exception listener
     * @throws JMSException if the JMS provider fails to set the exception
     *                 listener for this connection.
     */
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        checkClosedOrFailed();
        this.exceptionListener = listener;
    }

    /**
     * Gets the <code>ClientInternalExceptionListener</code> object for this connection.
     * Not every <CODE>ActiveMQConnectionn</CODE> has a <CODE>ClientInternalExceptionListener</CODE>
     * associated with it.
     * 
     * @return the listener or <code>null</code> if no listener is registered with the connection.
     */
    public ClientInternalExceptionListener getClientInternalExceptionListener()
    {
        return clientInternalExceptionListener;
    }

    /**
     * Sets a client internal exception listener for this connection.
     * The connection will notify the listener, if one has been registered, of exceptions thrown by container components
     * (e.g. an EJB container in case of Message Driven Beans) during asynchronous processing of a message.
     * It does this by calling the listener's <code>onException()</code> method passing it a <code>Throwable</code>
     * describing the problem.
     * 
     * @param listener the exception listener
     */
    public void setClientInternalExceptionListener(ClientInternalExceptionListener listener)
    {
        this.clientInternalExceptionListener = listener;
    }
    
    /**
     * Starts (or restarts) a connection's delivery of incoming messages. A call
     * to <CODE>start</CODE> on a connection that has already been started is
     * ignored.
     * 
     * @throws JMSException if the JMS provider fails to start message delivery
     *                 due to some internal error.
     * @see javax.jms.Connection#stop()
     */
    public void start() throws JMSException {
        checkClosedOrFailed();
        ensureConnectionInfoSent();
        if (started.compareAndSet(false, true)) {
            for (Iterator<ActiveMQSession> i = sessions.iterator(); i.hasNext();) {
                ActiveMQSession session = i.next();
                session.start();
            }
        }
    }

    /**
     * Temporarily stops a connection's delivery of incoming messages. Delivery
     * can be restarted using the connection's <CODE>start</CODE> method. When
     * the connection is stopped, delivery to all the connection's message
     * consumers is inhibited: synchronous receives block, and messages are not
     * delivered to message listeners.
     * <P>
     * This call blocks until receives and/or message listeners in progress have
     * completed.
     * <P>
     * Stopping a connection has no effect on its ability to send messages. A
     * call to <CODE>stop</CODE> on a connection that has already been stopped
     * is ignored.
     * <P>
     * A call to <CODE>stop</CODE> must not return until delivery of messages
     * has paused. This means that a client can rely on the fact that none of
     * its message listeners will be called and that all threads of control
     * waiting for <CODE>receive</CODE> calls to return will not return with a
     * message until the connection is restarted. The receive timers for a
     * stopped connection continue to advance, so receives may time out while
     * the connection is stopped.
     * <P>
     * If message listeners are running when <CODE>stop</CODE> is invoked, the
     * <CODE>stop</CODE> call must wait until all of them have returned before
     * it may return. While these message listeners are completing, they must
     * have the full services of the connection available to them.
     * 
     * @throws JMSException if the JMS provider fails to stop message delivery
     *                 due to some internal error.
     * @see javax.jms.Connection#start()
     */
    public void stop() throws JMSException {
        checkClosedOrFailed();
        if (started.compareAndSet(true, false)) {
            synchronized(sessions) {
                for (Iterator<ActiveMQSession> i = sessions.iterator(); i.hasNext();) {
                    ActiveMQSession s = i.next();
                    s.stop();
                }
            }
        }
    }

    /**
     * Closes the connection.
     * <P>
     * Since a provider typically allocates significant resources outside the
     * JVM on behalf of a connection, clients should close these resources when
     * they are not needed. Relying on garbage collection to eventually reclaim
     * these resources may not be timely enough.
     * <P>
     * There is no need to close the sessions, producers, and consumers of a
     * closed connection.
     * <P>
     * Closing a connection causes all temporary destinations to be deleted.
     * <P>
     * When this method is invoked, it should not return until message
     * processing has been shut down in an orderly fashion. This means that all
     * message listeners that may have been running have returned, and that all
     * pending receives have returned. A close terminates all pending message
     * receives on the connection's sessions' consumers. The receives may return
     * with a message or with null, depending on whether there was a message
     * available at the time of the close. If one or more of the connection's
     * sessions' message listeners is processing a message at the time when
     * connection <CODE>close</CODE> is invoked, all the facilities of the
     * connection and its sessions must remain available to those listeners
     * until they return control to the JMS provider.
     * <P>
     * Closing a connection causes any of its sessions' transactions in progress
     * to be rolled back. In the case where a session's work is coordinated by
     * an external transaction manager, a session's <CODE>commit</CODE> and
     * <CODE> rollback</CODE> methods are not used and the result of a closed
     * session's work is determined later by the transaction manager. Closing a
     * connection does NOT force an acknowledgment of client-acknowledged
     * sessions.
     * <P>
     * Invoking the <CODE>acknowledge</CODE> method of a received message from
     * a closed connection's session must throw an
     * <CODE>IllegalStateException</CODE>. Closing a closed connection must
     * NOT throw an exception.
     * 
     * @throws JMSException if the JMS provider fails to close the connection
     *                 due to some internal error. For example, a failure to
     *                 release resources or to close a socket connection can
     *                 cause this exception to be thrown.
     */
    public void close() throws JMSException {
        try {
            // If we were running, lets stop first.
            if (!closed.get() && !transportFailed.get()) {
                stop();
            }

            synchronized (this) {
                if (!closed.get()) {
                    closing.set(true);

                    if (destinationSource != null) {
                        destinationSource.stop();
                        destinationSource = null;
                    }
                    if (advisoryConsumer != null) {
                        advisoryConsumer.dispose();
                        advisoryConsumer = null;
                    }
                    if (this.scheduler != null) {
                        try {
                            this.scheduler.stop();
                        } catch (Exception e) {
                            JMSException ex =  JMSExceptionSupport.create(e);
                            throw ex;
                        }
                    }

                    long lastDeliveredSequenceId = 0;
                    for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
                        ActiveMQSession s = i.next();
                        s.dispose();
                        lastDeliveredSequenceId = Math.max(lastDeliveredSequenceId, s.getLastDeliveredSequenceId());
                    }
                    for (Iterator<ActiveMQConnectionConsumer> i = this.connectionConsumers.iterator(); i.hasNext();) {
                        ActiveMQConnectionConsumer c = i.next();
                        c.dispose();
                    }
                    for (Iterator<ActiveMQInputStream> i = this.inputStreams.iterator(); i.hasNext();) {
                        ActiveMQInputStream c = i.next();
                        c.dispose();
                    }
                    for (Iterator<ActiveMQOutputStream> i = this.outputStreams.iterator(); i.hasNext();) {
                        ActiveMQOutputStream c = i.next();
                        c.dispose();
                    }

                    if (isConnectionInfoSentToBroker) {
                        // If we announced ourselfs to the broker.. Try to let
                        // the broker
                        // know that the connection is being shutdown.
                        RemoveInfo removeCommand = info.createRemoveCommand();
                        removeCommand.setLastDeliveredSequenceId(lastDeliveredSequenceId);
                        doSyncSendPacket(info.createRemoveCommand(), closeTimeout);
                        doAsyncSendPacket(new ShutdownInfo());
                    }

                    ServiceSupport.dispose(this.transport);

                    started.set(false);

                    // TODO if we move the TaskRunnerFactory to the connection
                    // factory
                    // then we may need to call
                    // factory.onConnectionClose(this);
                    if (sessionTaskRunner != null) {
                        sessionTaskRunner.shutdown();
                    }
                    closed.set(true);
                    closing.set(false);
                }
            }
        } finally {
            try {
                if (executor != null){
                    executor.shutdown();
                }
            }catch(Throwable e) {
                LOG.error("Error shutting down thread pool " + e,e);
            }
            factoryStats.removeConnection(this);
        }
    }

    /**
     * Tells the broker to terminate its VM. This can be used to cleanly
     * terminate a broker running in a standalone java process. Server must have
     * property enable.vm.shutdown=true defined to allow this to work.
     */
    // TODO : org.apache.activemq.message.BrokerAdminCommand not yet
    // implemented.
    /*
     * public void terminateBrokerVM() throws JMSException { BrokerAdminCommand
     * command = new BrokerAdminCommand();
     * command.setCommand(BrokerAdminCommand.SHUTDOWN_SERVER_VM);
     * asyncSendPacket(command); }
     */

    /**
     * Create a durable connection consumer for this connection (optional
     * operation). This is an expert facility not used by regular JMS clients.
     * 
     * @param topic topic to access
     * @param subscriptionName durable subscription name
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @param sessionPool the server session pool to associate with this durable
     *                connection consumer
     * @param maxMessages the maximum number of messages that can be assigned to
     *                a server session at one time
     * @return the durable connection consumer
     * @throws JMSException if the <CODE>Connection</CODE> object fails to
     *                 create a connection consumer due to some internal error
     *                 or invalid arguments for <CODE>sessionPool</CODE> and
     *                 <CODE>messageSelector</CODE>.
     * @throws javax.jms.InvalidDestinationException if an invalid destination
     *                 is specified.
     * @throws javax.jms.InvalidSelectorException if the message selector is
     *                 invalid.
     * @see javax.jms.ConnectionConsumer
     * @since 1.1
     */
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages)
        throws JMSException {
        return this.createDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages, false);
    }

    /**
     * Create a durable connection consumer for this connection (optional
     * operation). This is an expert facility not used by regular JMS clients.
     * 
     * @param topic topic to access
     * @param subscriptionName durable subscription name
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @param sessionPool the server session pool to associate with this durable
     *                connection consumer
     * @param maxMessages the maximum number of messages that can be assigned to
     *                a server session at one time
     * @param noLocal set true if you want to filter out messages published
     *                locally
     * @return the durable connection consumer
     * @throws JMSException if the <CODE>Connection</CODE> object fails to
     *                 create a connection consumer due to some internal error
     *                 or invalid arguments for <CODE>sessionPool</CODE> and
     *                 <CODE>messageSelector</CODE>.
     * @throws javax.jms.InvalidDestinationException if an invalid destination
     *                 is specified.
     * @throws javax.jms.InvalidSelectorException if the message selector is
     *                 invalid.
     * @see javax.jms.ConnectionConsumer
     * @since 1.1
     */
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages,
                                                              boolean noLocal) throws JMSException {
        checkClosedOrFailed();
        ensureConnectionInfoSent();
        SessionId sessionId = new SessionId(info.getConnectionId(), -1);
        ConsumerInfo info = new ConsumerInfo(new ConsumerId(sessionId, consumerIdGenerator.getNextSequenceId()));
        info.setDestination(ActiveMQMessageTransformation.transformDestination(topic));
        info.setSubscriptionName(subscriptionName);
        info.setSelector(messageSelector);
        info.setPrefetchSize(maxMessages);
        info.setDispatchAsync(isDispatchAsync());

        // Allows the options on the destination to configure the consumerInfo
        if (info.getDestination().getOptions() != null) {
            Map<String, String> options = new HashMap<String, String>(info.getDestination().getOptions());
            IntrospectionSupport.setProperties(this.info, options, "consumer.");
        }

        return new ActiveMQConnectionConsumer(this, sessionPool, info);
    }

    // Properties
    // -------------------------------------------------------------------------

    /**
     * Returns true if this connection has been started
     * 
     * @return true if this Connection is started
     */
    public boolean isStarted() {
        return started.get();
    }

    /**
     * Returns true if the connection is closed
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Returns true if the connection is in the process of being closed
     */
    public boolean isClosing() {
        return closing.get();
    }

    /**
     * Returns true if the underlying transport has failed
     */
    public boolean isTransportFailed() {
        return transportFailed.get();
    }

    /**
     * @return Returns the prefetchPolicy.
     */
    public ActiveMQPrefetchPolicy getPrefetchPolicy() {
        return prefetchPolicy;
    }

    /**
     * Sets the <a
     * href="http://activemq.apache.org/what-is-the-prefetch-limit-for.html">prefetch
     * policy</a> for consumers created by this connection.
     */
    public void setPrefetchPolicy(ActiveMQPrefetchPolicy prefetchPolicy) {
        this.prefetchPolicy = prefetchPolicy;
    }

    /**
     */
    public Transport getTransportChannel() {
        return transport;
    }

    /**
     * @return Returns the clientID of the connection, forcing one to be
     *         generated if one has not yet been configured.
     */
    public String getInitializedClientID() throws JMSException {
        ensureConnectionInfoSent();
        return info.getClientId();
    }

    /**
     * @return Returns the timeStampsDisableByDefault.
     */
    public boolean isDisableTimeStampsByDefault() {
        return disableTimeStampsByDefault;
    }

    /**
     * Sets whether or not timestamps on messages should be disabled or not. If
     * you disable them it adds a small performance boost.
     */
    public void setDisableTimeStampsByDefault(boolean timeStampsDisableByDefault) {
        this.disableTimeStampsByDefault = timeStampsDisableByDefault;
    }

    /**
     * @return Returns the dispatchOptimizedMessage.
     */
    public boolean isOptimizedMessageDispatch() {
        return optimizedMessageDispatch;
    }

    /**
     * If this flag is set then an larger prefetch limit is used - only
     * applicable for durable topic subscribers.
     */
    public void setOptimizedMessageDispatch(boolean dispatchOptimizedMessage) {
        this.optimizedMessageDispatch = dispatchOptimizedMessage;
    }

    /**
     * @return Returns the closeTimeout.
     */
    public int getCloseTimeout() {
        return closeTimeout;
    }

    /**
     * Sets the timeout before a close is considered complete. Normally a
     * close() on a connection waits for confirmation from the broker; this
     * allows that operation to timeout to save the client hanging if there is
     * no broker
     */
    public void setCloseTimeout(int closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    /**
     * @return ConnectionInfo
     */
    public ConnectionInfo getConnectionInfo() {
        return this.info;
    }

    public boolean isUseRetroactiveConsumer() {
        return useRetroactiveConsumer;
    }

    /**
     * Sets whether or not retroactive consumers are enabled. Retroactive
     * consumers allow non-durable topic subscribers to receive old messages
     * that were published before the non-durable subscriber started.
     */
    public void setUseRetroactiveConsumer(boolean useRetroactiveConsumer) {
        this.useRetroactiveConsumer = useRetroactiveConsumer;
    }

    public boolean isNestedMapAndListEnabled() {
        return nestedMapAndListEnabled;
    }

    /**
     * Enables/disables whether or not Message properties and MapMessage entries
     * support <a
     * href="http://activemq.apache.org/structured-message-properties-and-mapmessages.html">Nested
     * Structures</a> of Map and List objects
     */
    public void setNestedMapAndListEnabled(boolean structuredMapsEnabled) {
        this.nestedMapAndListEnabled = structuredMapsEnabled;
    }

    public boolean isExclusiveConsumer() {
        return exclusiveConsumer;
    }

    /**
     * Enables or disables whether or not queue consumers should be exclusive or
     * not for example to preserve ordering when not using <a
     * href="http://activemq.apache.org/message-groups.html">Message Groups</a>
     * 
     * @param exclusiveConsumer
     */
    public void setExclusiveConsumer(boolean exclusiveConsumer) {
        this.exclusiveConsumer = exclusiveConsumer;
    }

    /**
     * Adds a transport listener so that a client can be notified of events in
     * the underlying transport
     */
    public void addTransportListener(TransportListener transportListener) {
        transportListeners.add(transportListener);
    }

    public void removeTransportListener(TransportListener transportListener) {
        transportListeners.remove(transportListener);
    }

    public boolean isUseDedicatedTaskRunner() {
        return useDedicatedTaskRunner;
    }
    
    public void setUseDedicatedTaskRunner(boolean useDedicatedTaskRunner) {
        this.useDedicatedTaskRunner = useDedicatedTaskRunner;
    }

    public TaskRunnerFactory getSessionTaskRunner() {
        synchronized (this) {
            if (sessionTaskRunner == null) {
                sessionTaskRunner = new TaskRunnerFactory("ActiveMQ Session Task", ThreadPriorities.INBOUND_CLIENT_SESSION, false, 1000, isUseDedicatedTaskRunner());
            }
        }
        return sessionTaskRunner;
    }

    public void setSessionTaskRunner(TaskRunnerFactory sessionTaskRunner) {
        this.sessionTaskRunner = sessionTaskRunner;
    }

    public MessageTransformer getTransformer() {
        return transformer;
    }

    /**
     * Sets the transformer used to transform messages before they are sent on
     * to the JMS bus or when they are received from the bus but before they are
     * delivered to the JMS client
     */
    public void setTransformer(MessageTransformer transformer) {
        this.transformer = transformer;
    }

    /**
     * @return the statsEnabled
     */
    public boolean isStatsEnabled() {
        return this.stats.isEnabled();
    }

    /**
     * @param statsEnabled the statsEnabled to set
     */
    public void setStatsEnabled(boolean statsEnabled) {
        this.stats.setEnabled(statsEnabled);
    }

    /**
     * Returns the {@link DestinationSource} object which can be used to listen to destinations
     * being created or destroyed or to enquire about the current destinations available on the broker
     *
     * @return a lazily created destination source
     * @throws JMSException
     */
    public DestinationSource getDestinationSource() throws JMSException {
        if (destinationSource == null) {
            destinationSource = new DestinationSource(this);
            destinationSource.start();
        }
        return destinationSource;
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    /**
     * Used internally for adding Sessions to the Connection
     * 
     * @param session
     * @throws JMSException
     * @throws JMSException
     */
    protected void addSession(ActiveMQSession session) throws JMSException {
        this.sessions.add(session);
        if (sessions.size() > 1 || session.isTransacted()) {
            optimizedMessageDispatch = false;
        }
    }

    /**
     * Used interanlly for removing Sessions from a Connection
     * 
     * @param session
     */
    protected void removeSession(ActiveMQSession session) {
        this.sessions.remove(session);
        this.removeDispatcher(session);
    }

    /**
     * Add a ConnectionConsumer
     * 
     * @param connectionConsumer
     * @throws JMSException
     */
    protected void addConnectionConsumer(ActiveMQConnectionConsumer connectionConsumer) throws JMSException {
        this.connectionConsumers.add(connectionConsumer);
    }

    /**
     * Remove a ConnectionConsumer
     * 
     * @param connectionConsumer
     */
    protected void removeConnectionConsumer(ActiveMQConnectionConsumer connectionConsumer) {
        this.connectionConsumers.remove(connectionConsumer);
        this.removeDispatcher(connectionConsumer);
    }

    /**
     * Creates a <CODE>TopicSession</CODE> object.
     * 
     * @param transacted indicates whether the session is transacted
     * @param acknowledgeMode indicates whether the consumer or the client will
     *                acknowledge any messages it receives; ignored if the
     *                session is transacted. Legal values are
     *                <code>Session.AUTO_ACKNOWLEDGE</code>,
     *                <code>Session.CLIENT_ACKNOWLEDGE</code>, and
     *                <code>Session.DUPS_OK_ACKNOWLEDGE</code>.
     * @return a newly created topic session
     * @throws JMSException if the <CODE>TopicConnection</CODE> object fails
     *                 to create a session due to some internal error or lack of
     *                 support for the specific transaction and acknowledgement
     *                 mode.
     * @see Session#AUTO_ACKNOWLEDGE
     * @see Session#CLIENT_ACKNOWLEDGE
     * @see Session#DUPS_OK_ACKNOWLEDGE
     */
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return new ActiveMQTopicSession((ActiveMQSession)createSession(transacted, acknowledgeMode));
    }

    /**
     * Creates a connection consumer for this connection (optional operation).
     * This is an expert facility not used by regular JMS clients.
     * 
     * @param topic the topic to access
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @param sessionPool the server session pool to associate with this
     *                connection consumer
     * @param maxMessages the maximum number of messages that can be assigned to
     *                a server session at one time
     * @return the connection consumer
     * @throws JMSException if the <CODE>TopicConnection</CODE> object fails
     *                 to create a connection consumer due to some internal
     *                 error or invalid arguments for <CODE>sessionPool</CODE>
     *                 and <CODE>messageSelector</CODE>.
     * @throws javax.jms.InvalidDestinationException if an invalid topic is
     *                 specified.
     * @throws javax.jms.InvalidSelectorException if the message selector is
     *                 invalid.
     * @see javax.jms.ConnectionConsumer
     */
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return createConnectionConsumer(topic, messageSelector, sessionPool, maxMessages, false);
    }

    /**
     * Creates a connection consumer for this connection (optional operation).
     * This is an expert facility not used by regular JMS clients.
     * 
     * @param queue the queue to access
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @param sessionPool the server session pool to associate with this
     *                connection consumer
     * @param maxMessages the maximum number of messages that can be assigned to
     *                a server session at one time
     * @return the connection consumer
     * @throws JMSException if the <CODE>QueueConnection</CODE> object fails
     *                 to create a connection consumer due to some internal
     *                 error or invalid arguments for <CODE>sessionPool</CODE>
     *                 and <CODE>messageSelector</CODE>.
     * @throws javax.jms.InvalidDestinationException if an invalid queue is
     *                 specified.
     * @throws javax.jms.InvalidSelectorException if the message selector is
     *                 invalid.
     * @see javax.jms.ConnectionConsumer
     */
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return createConnectionConsumer(queue, messageSelector, sessionPool, maxMessages, false);
    }

    /**
     * Creates a connection consumer for this connection (optional operation).
     * This is an expert facility not used by regular JMS clients.
     * 
     * @param destination the destination to access
     * @param messageSelector only messages with properties matching the message
     *                selector expression are delivered. A value of null or an
     *                empty string indicates that there is no message selector
     *                for the message consumer.
     * @param sessionPool the server session pool to associate with this
     *                connection consumer
     * @param maxMessages the maximum number of messages that can be assigned to
     *                a server session at one time
     * @return the connection consumer
     * @throws JMSException if the <CODE>Connection</CODE> object fails to
     *                 create a connection consumer due to some internal error
     *                 or invalid arguments for <CODE>sessionPool</CODE> and
     *                 <CODE>messageSelector</CODE>.
     * @throws javax.jms.InvalidDestinationException if an invalid destination
     *                 is specified.
     * @throws javax.jms.InvalidSelectorException if the message selector is
     *                 invalid.
     * @see javax.jms.ConnectionConsumer
     * @since 1.1
     */
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return createConnectionConsumer(destination, messageSelector, sessionPool, maxMessages, false);
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages, boolean noLocal)
        throws JMSException {

        checkClosedOrFailed();
        ensureConnectionInfoSent();

        ConsumerId consumerId = createConsumerId();
        ConsumerInfo consumerInfo = new ConsumerInfo(consumerId);
        consumerInfo.setDestination(ActiveMQMessageTransformation.transformDestination(destination));
        consumerInfo.setSelector(messageSelector);
        consumerInfo.setPrefetchSize(maxMessages);
        consumerInfo.setNoLocal(noLocal);
        consumerInfo.setDispatchAsync(isDispatchAsync());

        // Allows the options on the destination to configure the consumerInfo
        if (consumerInfo.getDestination().getOptions() != null) {
            Map<String, String> options = new HashMap<String, String>(consumerInfo.getDestination().getOptions());
            IntrospectionSupport.setProperties(consumerInfo, options, "consumer.");
        }

        return new ActiveMQConnectionConsumer(this, sessionPool, consumerInfo);
    }

    /**
     * @return
     */
    private ConsumerId createConsumerId() {
        return new ConsumerId(connectionSessionId, consumerIdGenerator.getNextSequenceId());
    }

    /**
     * @return
     */
    private ProducerId createProducerId() {
        return new ProducerId(connectionSessionId, producerIdGenerator.getNextSequenceId());
    }

    /**
     * Creates a <CODE>QueueSession</CODE> object.
     * 
     * @param transacted indicates whether the session is transacted
     * @param acknowledgeMode indicates whether the consumer or the client will
     *                acknowledge any messages it receives; ignored if the
     *                session is transacted. Legal values are
     *                <code>Session.AUTO_ACKNOWLEDGE</code>,
     *                <code>Session.CLIENT_ACKNOWLEDGE</code>, and
     *                <code>Session.DUPS_OK_ACKNOWLEDGE</code>.
     * @return a newly created queue session
     * @throws JMSException if the <CODE>QueueConnection</CODE> object fails
     *                 to create a session due to some internal error or lack of
     *                 support for the specific transaction and acknowledgement
     *                 mode.
     * @see Session#AUTO_ACKNOWLEDGE
     * @see Session#CLIENT_ACKNOWLEDGE
     * @see Session#DUPS_OK_ACKNOWLEDGE
     */
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return new ActiveMQQueueSession((ActiveMQSession)createSession(transacted, acknowledgeMode));
    }

    /**
     * Ensures that the clientID was manually specified and not auto-generated.
     * If the clientID was not specified this method will throw an exception.
     * This method is used to ensure that the clientID + durableSubscriber name
     * are used correctly.
     * 
     * @throws JMSException
     */
    public void checkClientIDWasManuallySpecified() throws JMSException {
        if (!userSpecifiedClientID) {
            throw new JMSException("You cannot create a durable subscriber without specifying a unique clientID on a Connection");
        }
    }

    /**
     * send a Packet through the Connection - for internal use only
     * 
     * @param command
     * @throws JMSException
     */
    public void asyncSendPacket(Command command) throws JMSException {
        if (isClosed()) {
            throw new ConnectionClosedException();
        } else {
            doAsyncSendPacket(command);
        }
    }

	private void doAsyncSendPacket(Command command) throws JMSException {
		try {
		    this.transport.oneway(command);
		} catch (IOException e) {
		    throw JMSExceptionSupport.create(e);
		}
	}

    /**
     * Send a packet through a Connection - for internal use only
     * 
     * @param command
     * @return
     * @throws JMSException
     */
    public Response syncSendPacket(Command command) throws JMSException {
        if (isClosed()) {
            throw new ConnectionClosedException();
        } else {

            try {
                Response response = (Response)this.transport.request(command);
                if (response.isException()) {
                    ExceptionResponse er = (ExceptionResponse)response;
                    if (er.getException() instanceof JMSException) {
                        throw (JMSException)er.getException();
                    } else {
                        if (isClosed()||closing.get()) {
                            LOG.debug("Received an exception but connection is closing");
                        }
                        JMSException jmsEx = null;
                        try {
                         jmsEx = JMSExceptionSupport.create(er.getException());
                        }catch(Throwable e) {
                            LOG.error("Caught an exception trying to create a JMSException for " +er.getException(),e);
                        }
                        if(jmsEx !=null) {
                            throw jmsEx;
                        }
                    }
                }
                return response;
            } catch (IOException e) {
                throw JMSExceptionSupport.create(e);
            }
        }
    }

    /**
     * Send a packet through a Connection - for internal use only
     * 
     * @param command
     * @return
     * @throws JMSException
     */
    public Response syncSendPacket(Command command, int timeout) throws JMSException {
        if (isClosed() || closing.get()) {
            throw new ConnectionClosedException();
        } else {
            return doSyncSendPacket(command, timeout);
        }
    }

	private Response doSyncSendPacket(Command command, int timeout)
			throws JMSException {
		try {
		    Response response = (Response)this.transport.request(command, timeout);
		    if (response != null && response.isException()) {
		        ExceptionResponse er = (ExceptionResponse)response;
		        if (er.getException() instanceof JMSException) {
		            throw (JMSException)er.getException();
		        } else {
		            throw JMSExceptionSupport.create(er.getException());
		        }
		    }
		    return response;
		} catch (IOException e) {
		    throw JMSExceptionSupport.create(e);
		}
	}

    /**
     * @return statistics for this Connection
     */
    public StatsImpl getStats() {
        return stats;
    }

    /**
     * simply throws an exception if the Connection is already closed or the
     * Transport has failed
     * 
     * @throws JMSException
     */
    protected synchronized void checkClosedOrFailed() throws JMSException {
        checkClosed();
        if (transportFailed.get()) {
            throw new ConnectionFailedException(firstFailureError);
        }
    }

    /**
     * simply throws an exception if the Connection is already closed
     * 
     * @throws JMSException
     */
    protected synchronized void checkClosed() throws JMSException {
        if (closed.get()) {
            throw new ConnectionClosedException();
        }
    }

    /**
     * Send the ConnectionInfo to the Broker
     * 
     * @throws JMSException
     */
    protected void ensureConnectionInfoSent() throws JMSException {
        synchronized(this.ensureConnectionInfoSentMutex) {
            // Can we skip sending the ConnectionInfo packet??
            if (isConnectionInfoSentToBroker || closed.get()) {
                return;
            }
            //TODO shouldn't this check be on userSpecifiedClientID rather than the value of clientID?
            if (info.getClientId() == null || info.getClientId().trim().length() == 0) {
                info.setClientId(clientIdGenerator.generateId());
            }
            syncSendPacket(info.copy());
    
            this.isConnectionInfoSentToBroker = true;
            // Add a temp destination advisory consumer so that
            // We know what the valid temporary destinations are on the
            // broker without having to do an RPC to the broker.
    
            ConsumerId consumerId = new ConsumerId(new SessionId(info.getConnectionId(), -1), consumerIdGenerator.getNextSequenceId());
            if (watchTopicAdvisories) {
                advisoryConsumer = new AdvisoryConsumer(this, consumerId);
            }
        }
    }

    public synchronized boolean isWatchTopicAdvisories() {
        return watchTopicAdvisories;
    }

    public synchronized void setWatchTopicAdvisories(boolean watchTopicAdvisories) {
        this.watchTopicAdvisories = watchTopicAdvisories;
    }

    /**
     * @return Returns the useAsyncSend.
     */
    public boolean isUseAsyncSend() {
        return useAsyncSend;
    }

    /**
     * Forces the use of <a
     * href="http://activemq.apache.org/async-sends.html">Async Sends</a> which
     * adds a massive performance boost; but means that the send() method will
     * return immediately whether the message has been sent or not which could
     * lead to message loss.
     */
    public void setUseAsyncSend(boolean useAsyncSend) {
        this.useAsyncSend = useAsyncSend;
    }

    /**
     * @return true if always sync send messages
     */
    public boolean isAlwaysSyncSend() {
        return this.alwaysSyncSend;
    }

    /**
     * Set true if always require messages to be sync sent
     * 
     * @param alwaysSyncSend
     */
    public void setAlwaysSyncSend(boolean alwaysSyncSend) {
        this.alwaysSyncSend = alwaysSyncSend;
    }

    /**
     * Cleans up this connection so that it's state is as if the connection was
     * just created. This allows the Resource Adapter to clean up a connection
     * so that it can be reused without having to close and recreate the
     * connection.
     */
    public void cleanup() throws JMSException {

        if (advisoryConsumer != null && !isTransportFailed()) {
            advisoryConsumer.dispose();
            advisoryConsumer = null;
        }

        for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
            ActiveMQSession s = i.next();
            s.dispose();
        }
        for (Iterator<ActiveMQConnectionConsumer> i = this.connectionConsumers.iterator(); i.hasNext();) {
            ActiveMQConnectionConsumer c = i.next();
            c.dispose();
        }
        for (Iterator<ActiveMQInputStream> i = this.inputStreams.iterator(); i.hasNext();) {
            ActiveMQInputStream c = i.next();
            c.dispose();
        }
        for (Iterator<ActiveMQOutputStream> i = this.outputStreams.iterator(); i.hasNext();) {
            ActiveMQOutputStream c = i.next();
            c.dispose();
        }

        if (isConnectionInfoSentToBroker) {
            if (!transportFailed.get() && !closing.get()) {
                syncSendPacket(info.createRemoveCommand());
            }
            isConnectionInfoSentToBroker = false;
        }
        if (userSpecifiedClientID) {
            info.setClientId(null);
            userSpecifiedClientID = false;
        }
        clientIDSet = false;

        started.set(false);
    }

    /**
     * Changes the associated username/password that is associated with this
     * connection. If the connection has been used, you must called cleanup()
     * before calling this method.
     * 
     * @throws IllegalStateException if the connection is in used.
     */
    public void changeUserInfo(String userName, String password) throws JMSException {
        if (isConnectionInfoSentToBroker) {
            throw new IllegalStateException("changeUserInfo used Connection is not allowed");
        }
        this.info.setUserName(userName);
        this.info.setPassword(password);
    }

    /**
     * @return Returns the resourceManagerId.
     * @throws JMSException
     */
    public String getResourceManagerId() throws JMSException {
        waitForBrokerInfo();
        if (brokerInfo == null) {
            throw new JMSException("Connection failed before Broker info was received.");
        }
        return brokerInfo.getBrokerId().getValue();
    }

    /**
     * Returns the broker name if one is available or null if one is not
     * available yet.
     */
    public String getBrokerName() {
        try {
            brokerInfoReceived.await(5, TimeUnit.SECONDS);
            if (brokerInfo == null) {
                return null;
            }
            return brokerInfo.getBrokerName();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Returns the broker information if it is available or null if it is not
     * available yet.
     */
    public BrokerInfo getBrokerInfo() {
        return brokerInfo;
    }

    /**
     * @return Returns the RedeliveryPolicy.
     * @throws JMSException
     */
    public RedeliveryPolicy getRedeliveryPolicy() throws JMSException {
        return redeliveryPolicy;
    }

    /**
     * Sets the redelivery policy to be used when messages are rolled back
     */
    public void setRedeliveryPolicy(RedeliveryPolicy redeliveryPolicy) {
        this.redeliveryPolicy = redeliveryPolicy;
    }

    public BlobTransferPolicy getBlobTransferPolicy() {
        if (blobTransferPolicy == null) {
            blobTransferPolicy = createBlobTransferPolicy();
        }
        return blobTransferPolicy;
    }

    /**
     * Sets the policy used to describe how out-of-band BLOBs (Binary Large
     * OBjects) are transferred from producers to brokers to consumers
     */
    public void setBlobTransferPolicy(BlobTransferPolicy blobTransferPolicy) {
        this.blobTransferPolicy = blobTransferPolicy;
    }

    /**
     * @return Returns the alwaysSessionAsync.
     */
    public boolean isAlwaysSessionAsync() {
        return alwaysSessionAsync;
    }

    /**
     * If this flag is set then a separate thread is not used for dispatching
     * messages for each Session in the Connection. However, a separate thread
     * is always used if there is more than one session, or the session isn't in
     * auto acknowledge or duplicates ok mode
     */
    public void setAlwaysSessionAsync(boolean alwaysSessionAsync) {
        this.alwaysSessionAsync = alwaysSessionAsync;
    }

    /**
     * @return Returns the optimizeAcknowledge.
     */
    public boolean isOptimizeAcknowledge() {
        return optimizeAcknowledge;
    }

    /**
     * Enables an optimised acknowledgement mode where messages are acknowledged
     * in batches rather than individually
     * 
     * @param optimizeAcknowledge The optimizeAcknowledge to set.
     */
    public void setOptimizeAcknowledge(boolean optimizeAcknowledge) {
        this.optimizeAcknowledge = optimizeAcknowledge;
    }

    public long getWarnAboutUnstartedConnectionTimeout() {
        return warnAboutUnstartedConnectionTimeout;
    }

    /**
     * Enables the timeout from a connection creation to when a warning is
     * generated if the connection is not properly started via {@link #start()}
     * and a message is received by a consumer. It is a very common gotcha to
     * forget to <a
     * href="http://activemq.apache.org/i-am-not-receiving-any-messages-what-is-wrong.html">start
     * the connection</a> so this option makes the default case to create a
     * warning if the user forgets. To disable the warning just set the value to <
     * 0 (say -1).
     */
    public void setWarnAboutUnstartedConnectionTimeout(long warnAboutUnstartedConnectionTimeout) {
        this.warnAboutUnstartedConnectionTimeout = warnAboutUnstartedConnectionTimeout;
    }
    
    /**
     * @return the sendTimeout
     */
    public int getSendTimeout() {
        return sendTimeout;
    }

    /**
     * @param sendTimeout the sendTimeout to set
     */
    public void setSendTimeout(int sendTimeout) {
        this.sendTimeout = sendTimeout;
    }
    
    /**
     * @return the sendAcksAsync
     */
    public boolean isSendAcksAsync() {
        return sendAcksAsync;
    }

    /**
     * @param sendAcksAsync the sendAcksAsync to set
     */
    public void setSendAcksAsync(boolean sendAcksAsync) {
        this.sendAcksAsync = sendAcksAsync;
    }


    /**
     * Returns the time this connection was created
     */
    public long getTimeCreated() {
        return timeCreated;
    }

    private void waitForBrokerInfo() throws JMSException {
        try {
            brokerInfoReceived.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw JMSExceptionSupport.create(e);
        }
    }

    // Package protected so that it can be used in unit tests
    public Transport getTransport() {
        return transport;
    }

    public void addProducer(ProducerId producerId, ActiveMQMessageProducer producer) {
        producers.put(producerId, producer);
    }

    public void removeProducer(ProducerId producerId) {
        producers.remove(producerId);
    }

    public void addDispatcher(ConsumerId consumerId, ActiveMQDispatcher dispatcher) {
        dispatchers.put(consumerId, dispatcher);
    }

    public void removeDispatcher(ConsumerId consumerId) {
        dispatchers.remove(consumerId);
    }

    /**
     * @param o - the command to consume
     */
    public void onCommand(final Object o) {
        final Command command = (Command)o;
        if (!closed.get() && command != null) {
            try {
                command.visit(new CommandVisitorAdapter() {
                    @Override
                    public Response processMessageDispatch(MessageDispatch md) throws Exception {
                        waitForTransportInterruptionProcessingToComplete();
                        ActiveMQDispatcher dispatcher = dispatchers.get(md.getConsumerId());
                        if (dispatcher != null) {
                            // Copy in case a embedded broker is dispatching via
                            // vm://
                            // md.getMessage() == null to signal end of queue
                            // browse.
                            Message msg = md.getMessage();
                            if (msg != null) {
                                msg = msg.copy();
                                msg.setReadOnlyBody(true);
                                msg.setReadOnlyProperties(true);
                                msg.setRedeliveryCounter(md.getRedeliveryCounter());
                                msg.setConnection(ActiveMQConnection.this);
                                md.setMessage(msg);
                            }
                            dispatcher.dispatch(md);
                        }
                        return null;
                    }

                    @Override
                    public Response processProducerAck(ProducerAck pa) throws Exception {
                        if (pa != null && pa.getProducerId() != null) {
                            ActiveMQMessageProducer producer = producers.get(pa.getProducerId());
                            if (producer != null) {
                                producer.onProducerAck(pa);
                            }
                        }
                        return null;
                    }

                    @Override
                    public Response processBrokerInfo(BrokerInfo info) throws Exception {
                        brokerInfo = info;
                        brokerInfoReceived.countDown();
                        optimizeAcknowledge &= !brokerInfo.isFaultTolerantConfiguration();
                        getBlobTransferPolicy().setBrokerUploadUrl(info.getBrokerUploadUrl());
                        return null;
                    }

                    @Override
                    public Response processConnectionError(final ConnectionError error) throws Exception {
                        executor.execute(new Runnable() {
                            public void run() {
                                onAsyncException(error.getException());
                            }
                        });
                        return null;
                    }

                    @Override
                    public Response processControlCommand(ControlCommand command) throws Exception {
                        onControlCommand(command);
                        return null;
                    }

                    @Override
                    public Response processConnectionControl(ConnectionControl control) throws Exception {
                        onConnectionControl((ConnectionControl)command);
                        return null;
                    }

                    @Override
                    public Response processConsumerControl(ConsumerControl control) throws Exception {
                        onConsumerControl((ConsumerControl)command);
                        return null;
                    }

                    @Override
                    public Response processWireFormat(WireFormatInfo info) throws Exception {
                        onWireFormatInfo((WireFormatInfo)command);
                        return null;
                    }
                });
            } catch (Exception e) {
                onClientInternalException(e);
            }

        }
        for (Iterator<TransportListener> iter = transportListeners.iterator(); iter.hasNext();) {
            TransportListener listener = iter.next();
            listener.onCommand(command);
        }
    }

    protected void onWireFormatInfo(WireFormatInfo info) {
        protocolVersion.set(info.getVersion());
    }

    /**
     * Handles async client internal exceptions.
     * A client internal exception is usually one that has been thrown
     * by a container runtime component during asynchronous processing of a
     * message that does not affect the connection itself.
     * This method notifies the <code>ClientInternalExceptionListener</code> by invoking
     * its <code>onException</code> method, if one has been registered with this connection.
     * 
     * @param error the exception that the problem
     */
    public void onClientInternalException(final Throwable error) {
        if ( !closed.get() && !closing.get() ) {
            if ( this.clientInternalExceptionListener != null ) {
                executor.execute(new Runnable() {
                    public void run() {
                        ActiveMQConnection.this.clientInternalExceptionListener.onException(error);
                    }
                });
            } else {
                LOG.debug("Async client internal exception occurred with no exception listener registered: " 
                        + error, error);
            }
        }
    }
    /**
     * Used for handling async exceptions
     * 
     * @param error
     */
    public void onAsyncException(Throwable error) {
        if (!closed.get() && !closing.get()) {
            if (this.exceptionListener != null) {

                if (!(error instanceof JMSException)) {
                    error = JMSExceptionSupport.create(error);
                }
                final JMSException e = (JMSException)error;

                executor.execute(new Runnable() {
                    public void run() {
                        ActiveMQConnection.this.exceptionListener.onException(e);
                    }
                });

            } else {
                LOG.debug("Async exception with no exception listener: " + error, error);
            }
        }
    }

    public void onException(final IOException error) {
		onAsyncException(error);
		if (!closing.get() && !closed.get()) {
			executor.execute(new Runnable() {
				public void run() {
					transportFailed(error);
					ServiceSupport.dispose(ActiveMQConnection.this.transport);
					brokerInfoReceived.countDown();
					try {
						cleanup();
					} catch (JMSException e) {
						LOG.warn("Exception during connection cleanup, " + e, e);
					}
					for (Iterator<TransportListener> iter = transportListeners
							.iterator(); iter.hasNext();) {
						TransportListener listener = iter.next();
						listener.onException(error);
					}
				}
			});
		}
	}

    public void transportInterupted() {
        this.transportInterruptionProcessingComplete = new CountDownLatch(dispatchers.size() - (advisoryConsumer != null ? 1:0));
        if (LOG.isDebugEnabled()) {
            LOG.debug("transport interrupted, dispatchers: " + transportInterruptionProcessingComplete.getCount());
        }
        for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
            ActiveMQSession s = i.next();
            s.clearMessagesInProgress();
        }
        
        for (ActiveMQConnectionConsumer connectionConsumer : this.connectionConsumers) {
            connectionConsumer.clearMessagesInProgress();    
        }
        
        for (Iterator<TransportListener> iter = transportListeners.iterator(); iter.hasNext();) {
            TransportListener listener = iter.next();
            listener.transportInterupted();
        }
    }

    public void transportResumed() {
        for (Iterator<TransportListener> iter = transportListeners.iterator(); iter.hasNext();) {
            TransportListener listener = iter.next();
            listener.transportResumed();
        }
    }

    /**
     * Create the DestinationInfo object for the temporary destination.
     * 
     * @param topic - if its true topic, else queue.
     * @return DestinationInfo
     * @throws JMSException
     */
    protected ActiveMQTempDestination createTempDestination(boolean topic) throws JMSException {

        // Check if Destination info is of temporary type.
        ActiveMQTempDestination dest;
        if (topic) {
            dest = new ActiveMQTempTopic(info.getConnectionId(), tempDestinationIdGenerator.getNextSequenceId());
        } else {
            dest = new ActiveMQTempQueue(info.getConnectionId(), tempDestinationIdGenerator.getNextSequenceId());
        }

        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(this.info.getConnectionId());
        info.setOperationType(DestinationInfo.ADD_OPERATION_TYPE);
        info.setDestination(dest);
        syncSendPacket(info);

        dest.setConnection(this);
        activeTempDestinations.put(dest, dest);
        return dest;
    }

    /**
     * @param destination
     * @throws JMSException
     */
    public void deleteTempDestination(ActiveMQTempDestination destination) throws JMSException {

        checkClosedOrFailed();

        for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
            ActiveMQSession s = i.next();
            if (s.isInUse(destination)) {
                throw new JMSException("A consumer is consuming from the temporary destination");
            }
        }

        activeTempDestinations.remove(destination);

        DestinationInfo destInfo = new DestinationInfo();
        destInfo.setConnectionId(this.info.getConnectionId());
        destInfo.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
        destInfo.setDestination(destination);
        destInfo.setTimeout(0);
        syncSendPacket(destInfo);
    }

    public boolean isDeleted(ActiveMQDestination dest) {

        // If we are not watching the advisories.. then
        // we will assume that the temp destination does exist.
        if (advisoryConsumer == null) {
            return false;
        }

        return !activeTempDestinations.contains(dest);
    }

    public boolean isCopyMessageOnSend() {
        return copyMessageOnSend;
    }

    public LongSequenceGenerator getLocalTransactionIdGenerator() {
        return localTransactionIdGenerator;
    }

    public boolean isUseCompression() {
        return useCompression;
    }

    /**
     * Enables the use of compression of the message bodies
     */
    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    public void destroyDestination(ActiveMQDestination destination) throws JMSException {

        checkClosedOrFailed();
        ensureConnectionInfoSent();

        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(this.info.getConnectionId());
        info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
        info.setDestination(destination);
        info.setTimeout(0);
        syncSendPacket(info);

    }

    public boolean isDispatchAsync() {
        return dispatchAsync;
    }

    /**
     * Enables or disables the default setting of whether or not consumers have
     * their messages <a
     * href="http://activemq.apache.org/consumer-dispatch-async.html">dispatched
     * synchronously or asynchronously by the broker</a>. For non-durable
     * topics for example we typically dispatch synchronously by default to
     * minimize context switches which boost performance. However sometimes its
     * better to go slower to ensure that a single blocked consumer socket does
     * not block delivery to other consumers.
     * 
     * @param asyncDispatch If true then consumers created on this connection
     *                will default to having their messages dispatched
     *                asynchronously. The default value is false.
     */
    public void setDispatchAsync(boolean asyncDispatch) {
        this.dispatchAsync = asyncDispatch;
    }

    public boolean isObjectMessageSerializationDefered() {
        return objectMessageSerializationDefered;
    }

    /**
     * When an object is set on an ObjectMessage, the JMS spec requires the
     * object to be serialized by that set method. Enabling this flag causes the
     * object to not get serialized. The object may subsequently get serialized
     * if the message needs to be sent over a socket or stored to disk.
     */
    public void setObjectMessageSerializationDefered(boolean objectMessageSerializationDefered) {
        this.objectMessageSerializationDefered = objectMessageSerializationDefered;
    }

    public InputStream createInputStream(Destination dest) throws JMSException {
        return createInputStream(dest, null);
    }

    public InputStream createInputStream(Destination dest, String messageSelector) throws JMSException {
        return createInputStream(dest, messageSelector, false);
    }

    public InputStream createInputStream(Destination dest, String messageSelector, boolean noLocal) throws JMSException {
        return doCreateInputStream(dest, messageSelector, noLocal, null);
    }

    public InputStream createDurableInputStream(Topic dest, String name) throws JMSException {
        return createInputStream(dest, null, false);
    }

    public InputStream createDurableInputStream(Topic dest, String name, String messageSelector) throws JMSException {
        return createDurableInputStream(dest, name, messageSelector, false);
    }

    public InputStream createDurableInputStream(Topic dest, String name, String messageSelector, boolean noLocal) throws JMSException {
        return doCreateInputStream(dest, messageSelector, noLocal, name);
    }

    private InputStream doCreateInputStream(Destination dest, String messageSelector, boolean noLocal, String subName) throws JMSException {
        checkClosedOrFailed();
        ensureConnectionInfoSent();
        return new ActiveMQInputStream(this, createConsumerId(), ActiveMQDestination.transform(dest), messageSelector, noLocal, subName, prefetchPolicy.getInputStreamPrefetch());
    }

    /**
     * Creates a persistent output stream; individual messages will be written
     * to disk/database by the broker
     */
    public OutputStream createOutputStream(Destination dest) throws JMSException {
        return createOutputStream(dest, null, ActiveMQMessage.DEFAULT_DELIVERY_MODE, ActiveMQMessage.DEFAULT_PRIORITY, ActiveMQMessage.DEFAULT_TIME_TO_LIVE);
    }

    /**
     * Creates a non persistent output stream; messages will not be written to
     * disk
     */
    public OutputStream createNonPersistentOutputStream(Destination dest) throws JMSException {
        return createOutputStream(dest, null, DeliveryMode.NON_PERSISTENT, ActiveMQMessage.DEFAULT_PRIORITY, ActiveMQMessage.DEFAULT_TIME_TO_LIVE);
    }

    /**
     * Creates an output stream allowing full control over the delivery mode,
     * the priority and time to live of the messages and the properties added to
     * messages on the stream.
     * 
     * @param streamProperties defines a map of key-value pairs where the keys
     *                are strings and the values are primitive values (numbers
     *                and strings) which are appended to the messages similarly
     *                to using the
     *                {@link javax.jms.Message#setObjectProperty(String, Object)}
     *                method
     */
    public OutputStream createOutputStream(Destination dest, Map<String, Object> streamProperties, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosedOrFailed();
        ensureConnectionInfoSent();
        return new ActiveMQOutputStream(this, createProducerId(), ActiveMQDestination.transform(dest), streamProperties, deliveryMode, priority, timeToLive);
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
    public void unsubscribe(String name) throws InvalidDestinationException, JMSException {
        checkClosedOrFailed();
        RemoveSubscriptionInfo rsi = new RemoveSubscriptionInfo();
        rsi.setConnectionId(getConnectionInfo().getConnectionId());
        rsi.setSubscriptionName(name);
        rsi.setClientId(getConnectionInfo().getClientId());
        syncSendPacket(rsi);
    }

    /**
     * Internal send method optimized: - It does not copy the message - It can
     * only handle ActiveMQ messages. - You can specify if the send is async or
     * sync - Does not allow you to send /w a transaction.
     */
    void send(ActiveMQDestination destination, ActiveMQMessage msg, MessageId messageId, int deliveryMode, int priority, long timeToLive, boolean async) throws JMSException {
        checkClosedOrFailed();

        if (destination.isTemporary() && isDeleted(destination)) {
            throw new JMSException("Cannot publish to a deleted Destination: " + destination);
        }

        msg.setJMSDestination(destination);
        msg.setJMSDeliveryMode(deliveryMode);
        long expiration = 0L;

        if (!isDisableTimeStampsByDefault()) {
            long timeStamp = System.currentTimeMillis();
            msg.setJMSTimestamp(timeStamp);
            if (timeToLive > 0) {
                expiration = timeToLive + timeStamp;
            }
        }

        msg.setJMSExpiration(expiration);
        msg.setJMSPriority(priority);

        msg.setJMSRedelivered(false);
        msg.setMessageId(messageId);

        msg.onSend();

        msg.setProducerId(msg.getMessageId().getProducerId());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending message: " + msg);
        }

        if (async) {
            asyncSendPacket(msg);
        } else {
            syncSendPacket(msg);
        }

    }

    public void addOutputStream(ActiveMQOutputStream stream) {
        outputStreams.add(stream);
    }

    public void removeOutputStream(ActiveMQOutputStream stream) {
        outputStreams.remove(stream);
    }

    public void addInputStream(ActiveMQInputStream stream) {
        inputStreams.add(stream);
    }

    public void removeInputStream(ActiveMQInputStream stream) {
        inputStreams.remove(stream);
    }

    protected void onControlCommand(ControlCommand command) {
        String text = command.getCommand();
        if (text != null) {
            if (text.equals("shutdown")) {
                LOG.info("JVM told to shutdown");
                System.exit(0);
            }
        }
    }

    protected void onConnectionControl(ConnectionControl command) {
        if (command.isFaultTolerant()) {
            this.optimizeAcknowledge = false;
            for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
                ActiveMQSession s = i.next();
                s.setOptimizeAcknowledge(false);
            }
        }
    }

    protected void onConsumerControl(ConsumerControl command) {
        if (command.isClose()) {
            for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
                ActiveMQSession s = i.next();
                s.close(command.getConsumerId());
            }
        } else {
            for (Iterator<ActiveMQSession> i = this.sessions.iterator(); i.hasNext();) {
                ActiveMQSession s = i.next();
                s.setPrefetchSize(command.getConsumerId(), command.getPrefetch());
            }
        }
    }

    protected void transportFailed(IOException error) {
        transportFailed.set(true);
        if (firstFailureError == null) {
            firstFailureError = error;
        }
    }

    /**
     * Should a JMS message be copied to a new JMS Message object as part of the
     * send() method in JMS. This is enabled by default to be compliant with the
     * JMS specification. You can disable it if you do not mutate JMS messages
     * after they are sent for a performance boost
     */
    public void setCopyMessageOnSend(boolean copyMessageOnSend) {
        this.copyMessageOnSend = copyMessageOnSend;
    }

    @Override
    public String toString() {
        return "ActiveMQConnection {id=" + info.getConnectionId() + ",clientId=" + info.getClientId() + ",started=" + started.get() + "}";
    }

    protected BlobTransferPolicy createBlobTransferPolicy() {
        return new BlobTransferPolicy();
    }

    public int getProtocolVersion() {
        return protocolVersion.get();
    }

    public int getProducerWindowSize() {
        return producerWindowSize;
    }

    public void setProducerWindowSize(int producerWindowSize) {
        this.producerWindowSize = producerWindowSize;
    }

    public void setAuditDepth(int auditDepth) {
        connectionAudit.setAuditDepth(auditDepth);
	}

    public void setAuditMaximumProducerNumber(int auditMaximumProducerNumber) {
        connectionAudit.setAuditMaximumProducerNumber(auditMaximumProducerNumber);
	}

    protected void removeDispatcher(ActiveMQDispatcher dispatcher) {
        connectionAudit.removeDispatcher(dispatcher);
    }

    protected boolean isDuplicate(ActiveMQDispatcher dispatcher, Message message) {
        return checkForDuplicates && connectionAudit.isDuplicate(dispatcher, message);
    }

    protected void rollbackDuplicate(ActiveMQDispatcher dispatcher, Message message) {
        connectionAudit.rollbackDuplicate(dispatcher, message);
    }

	public IOException getFirstFailureError() {
		return firstFailureError;
	}
	
	protected void waitForTransportInterruptionProcessingToComplete() throws InterruptedException {
	    CountDownLatch cdl = this.transportInterruptionProcessingComplete;
	    if (cdl != null) {
            if (!closed.get() && !transportFailed.get() && cdl.getCount()>0) {
                LOG.warn("dispatch paused, waiting for outstanding dispatch interruption processing (" + cdl.getCount() + ") to complete..");
                cdl.await(10, TimeUnit.SECONDS);
            }
            signalInterruptionProcessingComplete();
        }
    }
	
	protected void transportInterruptionProcessingComplete() {
	    CountDownLatch cdl = this.transportInterruptionProcessingComplete;
	    if (cdl != null) {
	        cdl.countDown();
	        try {
	            signalInterruptionProcessingComplete();
	        } catch (InterruptedException ignored) {}
	    }
	}

    private void signalInterruptionProcessingComplete() throws InterruptedException {
        CountDownLatch cdl = this.transportInterruptionProcessingComplete;
        if (cdl.getCount()==0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("transportInterruptionProcessingComplete for: " + this.getConnectionInfo().getConnectionId());
            }
            this.transportInterruptionProcessingComplete = null;

            FailoverTransport failoverTransport = transport.narrow(FailoverTransport.class);
            if (failoverTransport != null) {
                failoverTransport.connectionInterruptProcessingComplete(this.getConnectionInfo().getConnectionId());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("notified failover transport (" + failoverTransport
                            + ") of interruption completion for: " + this.getConnectionInfo().getConnectionId());
                }
            }

        }
    }

    /*
     * specify the amount of time in milliseconds that a consumer with a transaction pending recovery
     * will wait to receive re dispatched messages.
     * default value is 0 so there is no wait by default.
     */
    public void setConsumerFailoverRedeliveryWaitPeriod(long consumerFailoverRedeliveryWaitPeriod) {
        this.consumerFailoverRedeliveryWaitPeriod = consumerFailoverRedeliveryWaitPeriod;
    }
    
    public long getConsumerFailoverRedeliveryWaitPeriod() {
        return consumerFailoverRedeliveryWaitPeriod;
    }
    
    protected Scheduler getScheduler() {
        return this.scheduler;
    }
    
    protected ThreadPoolExecutor getExecutor() {
        return this.executor;
    }

    /**
     * @return the checkForDuplicates
     */
    public boolean isCheckForDuplicates() {
        return this.checkForDuplicates;
    }

    /**
     * @param checkForDuplicates the checkForDuplicates to set
     */
    public void setCheckForDuplicates(boolean checkForDuplicates) {
        this.checkForDuplicates = checkForDuplicates;
    }
}
