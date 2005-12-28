/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ControlCommand;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.RedeliveryPolicy;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.management.JMSConnectionStatsImpl;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.management.StatsCapable;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;
import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;


public class ActiveMQConnection implements Connection, TopicConnection, QueueConnection, StatsCapable, Closeable, TransportListener, StreamConnection {

    public static final TaskRunnerFactory SESSION_TASK_RUNNER = new TaskRunnerFactory("session Task",ThreadPriorities.INBOUND_CLIENT_SESSION,true,1000);

    private static final Log log = LogFactory.getLog(ActiveMQConnection.class);
    private static final IdGenerator connectionIdGenerator = new IdGenerator();
    private static final IdGenerator clientIdGenerator = new IdGenerator();

    public static final String DEFAULT_USER = ActiveMQConnectionFactory.DEFAULT_USER;
    public static final String DEFAULT_PASSWORD = ActiveMQConnectionFactory.DEFAULT_PASSWORD;
    public static final String DEFAULT_BROKER_URL = ActiveMQConnectionFactory.DEFAULT_BROKER_URL;
     
    // Connection state variables
    private final ConnectionInfo info;
    private ExceptionListener exceptionListener;
    private String resourceManagerId;
    private boolean clientIDSet;
    private boolean isConnectionInfoSentToBroker;
    private boolean userSpecifiedClientID;
    
    // Configuration options variables
    private ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
    private RedeliveryPolicy redeliveryPolicy;
    private boolean disableTimeStampsByDefault = false;
    private boolean onSendPrepareMessageBody = true;
    private boolean optimizedMessageDispatch = true;
    private boolean copyMessageOnSend = true;
    private boolean useCompression = false;
    private boolean objectMessageSerializationDefered = false;
    protected boolean asyncDispatch = true;
    private boolean useAsyncSend = false;
    private boolean useRetroactiveConsumer;
    
    private long flowControlSleepTime = 0;
    private final JMSConnectionStatsImpl stats;
    private final JMSStatsImpl factoryStats;
    private final Transport transport;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final CopyOnWriteArrayList sessions = new CopyOnWriteArrayList();
    private final CopyOnWriteArrayList connectionConsumers = new CopyOnWriteArrayList();
    private final CopyOnWriteArrayList inputStreams = new CopyOnWriteArrayList();
    private final CopyOnWriteArrayList outputStreams = new CopyOnWriteArrayList();

    // Maps ConsumerIds to ActiveMQConsumer objects
    private final ConcurrentHashMap dispatchers = new ConcurrentHashMap();
    private final LongSequenceGenerator sessionIdGenerator = new LongSequenceGenerator();
    private final SessionId connectionSessionId;
    private final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator producerIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator tempDestinationIdGenerator = new LongSequenceGenerator();
    private final LongSequenceGenerator localTransactionIdGenerator = new LongSequenceGenerator();
    final ConcurrentHashMap activeTempDestinations = new ConcurrentHashMap();

    private AdvisoryConsumer advisoryConsumer;
    private final CountDownLatch brokerInfoReceived = new CountDownLatch(1);



    /**
     * Construct an <code>ActiveMQConnection</code>
     * @param transport 
     * @param factoryStats
     * @param userName
     * @param password
     * @throws Exception 
     */
    protected ActiveMQConnection(Transport transport, String userName, String password, JMSStatsImpl factoryStats)
            throws Exception {
        this.transport = transport;
        this.info = new ConnectionInfo(new ConnectionId(connectionIdGenerator.generateId()));
        this.info.setUserName(userName);
        this.info.setPassword(password);

        this.connectionSessionId = new SessionId(info.getConnectionId(), -1);
        
        this.factoryStats = factoryStats;
        this.factoryStats.addConnection(this);
        this.stats = new JMSConnectionStatsImpl(sessions, this instanceof XAConnection);
        this.transport.setTransportListener(this);

        transport.start();
    }

    /**
     * A static helper method to create a new connection
     * 
     * @return an ActiveMQConnection
     * @throws JMSException
     */
    public static ActiveMQConnection makeConnection() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        return (ActiveMQConnection) factory.createConnection();
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
        return (ActiveMQConnection) factory.createConnection();
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
    public static ActiveMQConnection makeConnection(String user, String password, String uri)
            throws JMSException, URISyntaxException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(user, password, new URI(uri));
        return (ActiveMQConnection) factory.createConnection();
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
     * @param transacted
     *            indicates whether the session is transacted
     * @param acknowledgeMode
     *            indicates whether the consumer or the client will acknowledge
     *            any messages it receives; ignored if the session is
     *            transacted. Legal values are
     *            <code>Session.AUTO_ACKNOWLEDGE</code>,
     *            <code>Session.CLIENT_ACKNOWLEDGE</code>, and
     *            <code>Session.DUPS_OK_ACKNOWLEDGE</code>.
     * @return a newly created session
     * @throws JMSException
     *             if the <CODE>Connection</CODE> object fails to create a
     *             session due to some internal error or lack of support for the
     *             specific transaction and acknowledgement mode.
     * @see Session#AUTO_ACKNOWLEDGE
     * @see Session#CLIENT_ACKNOWLEDGE
     * @see Session#DUPS_OK_ACKNOWLEDGE
     * @since 1.1
     */
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosed();
        ensureConnectionInfoSent();
        return new ActiveMQSession(this, getNextSessionId(), (transacted ? Session.SESSION_TRANSACTED
                : (acknowledgeMode == Session.SESSION_TRANSACTED ? Session.AUTO_ACKNOWLEDGE : acknowledgeMode)), asyncDispatch);
    }

    /**
     * @return
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
     * @throws JMSException
     *             if the JMS provider fails to return the client ID for this
     *             connection due to some internal error.
     */
    public String getClientID() throws JMSException {
        checkClosed();
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
     * an attempt to change it by setting it must throw an <CODE>IllegalStateException</CODE>.
     * If a client sets the client identifier explicitly, it must do so
     * immediately after it creates the connection and before any other action
     * on the connection is taken. After this point, setting the client
     * identifier is a programming error that should throw an <CODE>IllegalStateException</CODE>.
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
     * @param newClientID
     *            the unique client identifier
     * @throws JMSException
     *             if the JMS provider fails to set the client ID for this
     *             connection due to some internal error.
     * @throws javax.jms.InvalidClientIDException
     *             if the JMS client specifies an invalid or duplicate client
     *             ID.
     * @throws javax.jms.IllegalStateException
     *             if the JMS client attempts to set a connection's client ID at
     *             the wrong time or when it has been administratively
     *             configured.
     */
    public void setClientID(String newClientID) throws JMSException {
        checkClosed();

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
     * Gets the metadata for this connection.
     * 
     * @return the connection metadata
     * @throws JMSException
     *             if the JMS provider fails to get the connection metadata for
     *             this connection.
     * @see javax.jms.ConnectionMetaData
     */
    public ConnectionMetaData getMetaData() throws JMSException {
        checkClosed();
        return ActiveMQConnectionMetaData.INSTANCE;
    }

    /**
     * Gets the <CODE>ExceptionListener</CODE> object for this connection. Not
     * every <CODE>Connection</CODE> has an <CODE>ExceptionListener</CODE>
     * associated with it.
     * 
     * @return the <CODE>ExceptionListener</CODE> for this connection, or
     *         null. if no <CODE>ExceptionListener</CODE> is associated with
     *         this connection.
     * @throws JMSException
     *             if the JMS provider fails to get the <CODE>ExceptionListener</CODE>
     *             for this connection.
     * @see javax.jms.Connection#setExceptionListener(ExceptionListener)
     */
    public ExceptionListener getExceptionListener() throws JMSException {
        checkClosed();
        return this.exceptionListener;
    }

    /**
     * Sets an exception listener for this connection.
     * <P>
     * If a JMS provider detects a serious problem with a connection, it informs
     * the connection's <CODE> ExceptionListener</CODE>, if one has been
     * registered. It does this by calling the listener's <CODE>onException
     * </CODE> method, passing it a <CODE>JMSException</CODE> object
     * describing the problem.
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
     * @param listener
     *            the exception listener
     * @throws JMSException
     *             if the JMS provider fails to set the exception listener for
     *             this connection.
     */
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        checkClosed();
        this.exceptionListener = listener;
    }

    /**
     * Starts (or restarts) a connection's delivery of incoming messages. A call
     * to <CODE>start</CODE> on a connection that has already been started is
     * ignored.
     * 
     * @throws JMSException
     *             if the JMS provider fails to start message delivery due to
     *             some internal error.
     * @see javax.jms.Connection#stop()
     */
    public void start() throws JMSException {
        checkClosed();
        ensureConnectionInfoSent();
        if (started.compareAndSet(false, true)) {
            for (Iterator i = sessions.iterator(); i.hasNext();) {
                ActiveMQSession session = (ActiveMQSession) i.next();
                session.start();
            }
        }
    }

    /**
     * @return true if this Connection is started
     */
    protected boolean isStarted() {
        return started.get();
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
     * @throws JMSException
     *             if the JMS provider fails to stop message delivery due to
     *             some internal error.
     * @see javax.jms.Connection#start()
     */
    public void stop() throws JMSException {
        checkClosed();
        if (started.compareAndSet(true, false)) {
            for (Iterator i = sessions.iterator(); i.hasNext();) {
                ActiveMQSession s = (ActiveMQSession) i.next();
                s.stop();
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
     * a closed connection's session must throw an <CODE>IllegalStateException</CODE>.
     * Closing a closed connection must NOT throw an exception.
     * 
     * @throws JMSException
     *             if the JMS provider fails to close the connection due to some
     *             internal error. For example, a failure to release resources
     *             or to close a socket connection can cause this exception to
     *             be thrown.
     */
    public void close() throws JMSException {
        checkClosed();

        // If we were running, lets stop first.
        stop();

        synchronized (this) {
            if (!closed.get()) {
                closing.set(true);

                if( advisoryConsumer!=null ) {
                    advisoryConsumer.dispose();
                    advisoryConsumer=null;
                }

                for (Iterator i = this.sessions.iterator(); i.hasNext();) {
                    ActiveMQSession s = (ActiveMQSession) i.next();
                    s.dispose();
                }
                for (Iterator i = this.connectionConsumers.iterator(); i.hasNext();) {
                    ActiveMQConnectionConsumer c = (ActiveMQConnectionConsumer) i.next();
                    c.dispose();
                }
                for (Iterator i = this.inputStreams.iterator(); i.hasNext();) {
                    ActiveMQInputStream c = (ActiveMQInputStream) i.next();
                    c.dispose();
                }
                for (Iterator i = this.outputStreams.iterator(); i.hasNext();) {
                    ActiveMQOutputStream c = (ActiveMQOutputStream) i.next();
                    c.dispose();
                }
                
                
                if (isConnectionInfoSentToBroker) {
                    syncSendPacket(info.createRemoveCommand());
                }

                asyncSendPacket(new ShutdownInfo());
                ServiceSupport.dispose(this.transport);

                started.set(false);

                // TODO : ActiveMQConnectionFactory.onConnectionClose() not
                // yet implemented.
                // factory.onConnectionClose(this);
                
                closed.set(true);
                closing.set(false);
            }
        }
    }

    /**
     * Tells the broker to terminate its VM. This can be used to cleanly
     * terminate a broker running in a standalone java process. Server must have
     * property enable.vm.shutdown=true defined to allow this to work.
     */
    // TODO : org.apache.activemq.message.BrokerAdminCommand not yet implemented.
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
     * @param topic
     *            topic to access
     * @param subscriptionName
     *            durable subscription name
     * @param messageSelector
     *            only messages with properties matching the message selector
     *            expression are delivered. A value of null or an empty string
     *            indicates that there is no message selector for the message
     *            consumer.
     * @param sessionPool
     *            the server session pool to associate with this durable
     *            connection consumer
     * @param maxMessages
     *            the maximum number of messages that can be assigned to a
     *            server session at one time
     * @return the durable connection consumer
     * @throws JMSException
     *             if the <CODE>Connection</CODE> object fails to create a
     *             connection consumer due to some internal error or invalid
     *             arguments for <CODE>sessionPool</CODE> and <CODE>messageSelector</CODE>.
     * @throws javax.jms.InvalidDestinationException
     *             if an invalid destination is specified.
     * @throws javax.jms.InvalidSelectorException
     *             if the message selector is invalid.
     * @see javax.jms.ConnectionConsumer
     * @since 1.1
     */
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return this.createDurableConnectionConsumer(topic, subscriptionName, messageSelector, sessionPool, maxMessages, false);
    }

    /**
     * Create a durable connection consumer for this connection (optional
     * operation). This is an expert facility not used by regular JMS clients.
     * 
     * @param topic
     *            topic to access
     * @param subscriptionName
     *            durable subscription name
     * @param messageSelector
     *            only messages with properties matching the message selector
     *            expression are delivered. A value of null or an empty string
     *            indicates that there is no message selector for the message
     *            consumer.
     * @param sessionPool
     *            the server session pool to associate with this durable
     *            connection consumer
     * @param maxMessages
     *            the maximum number of messages that can be assigned to a
     *            server session at one time
     * @param noLocal
     *            set true if you want to filter out messages published locally
     * 
     * @return the durable connection consumer
     * @throws JMSException
     *             if the <CODE>Connection</CODE> object fails to create a
     *             connection consumer due to some internal error or invalid
     *             arguments for <CODE>sessionPool</CODE> and <CODE>messageSelector</CODE>.
     * @throws javax.jms.InvalidDestinationException
     *             if an invalid destination is specified.
     * @throws javax.jms.InvalidSelectorException
     *             if the message selector is invalid.
     * @see javax.jms.ConnectionConsumer
     * @since 1.1
     */
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName,
            String messageSelector, ServerSessionPool sessionPool, int maxMessages, boolean noLocal)
            throws JMSException {
        checkClosed();
        ensureConnectionInfoSent();
        SessionId sessionId = new SessionId(info.getConnectionId(), -1);
        ConsumerInfo info = new ConsumerInfo(new ConsumerId(sessionId, consumerIdGenerator
                .getNextSequenceId()));
        info.setDestination(ActiveMQMessageTransformation.transformDestination(topic));
        info.setSubcriptionName(subscriptionName);
        info.setSelector(messageSelector);
        info.setPrefetchSize(maxMessages);

        // Allows the options on the destination to configure the consumerInfo
        if( info.getDestination().getOptions()!=null ) {
            HashMap options = new HashMap(info.getDestination().getOptions());
            IntrospectionSupport.setProperties(this.info, options, "consumer.");
        }

        return new ActiveMQConnectionConsumer(this, sessionPool, info);
    }


    // Properties
    // -------------------------------------------------------------------------

    /**
     * @return Returns the prefetchPolicy.
     */
    public ActiveMQPrefetchPolicy getPrefetchPolicy() {
        return prefetchPolicy;
    }

    /**
     * @param prefetchPolicy
     *            The prefetchPolicy to set.
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
     * 
     * @return Returns the clientID of the connection, forcing one to be
     *         generated if one has not yet been configured.
     */
    public String getInitializedClientID() throws JMSException {
        ensureConnectionInfoSent();
        return info.getClientId();
    }

    /**
     * 
     * @return Returns the timeStampsDisableByDefault.
     */
    public boolean isDisableTimeStampsByDefault() {
        return disableTimeStampsByDefault;
    }

    /**
     * 
     * @param timeStampsDisableByDefault
     *            The timeStampsDisableByDefault to set.
     */
    public void setDisableTimeStampsByDefault(boolean timeStampsDisableByDefault) {
        this.disableTimeStampsByDefault = timeStampsDisableByDefault;
    }

    /**
     * 
     * @return Returns the dispatchOptimizedMessage.
     */
    public boolean isOptimizedMessageDispatch() {
        return optimizedMessageDispatch;
    }

    /**
     * 
     * @param dispatchOptimizedMessage
     *            The dispatchOptimizedMessage to set.
     */
    public void setOptimizedMessageDispatch(boolean dispatchOptimizedMessage) {
        this.optimizedMessageDispatch = dispatchOptimizedMessage;
    }

    /**
     * 
     * @return Returns the onSendPrepareMessageBody.
     */
    public boolean isOnSendPrepareMessageBody() {
        return onSendPrepareMessageBody;
    }

    /**
     * 
     * @param onSendPrepareMessageBody
     *            The onSendPrepareMessageBody to set.
     */
    public void setOnSendPrepareMessageBody(boolean onSendPrepareMessageBody) {
        this.onSendPrepareMessageBody = onSendPrepareMessageBody;
    }

    /**
     * 
     * @return ConnectionInfo
     */
    public ConnectionInfo getConnectionInfo() {
        return this.info;
    }

    public boolean isUseRetroactiveConsumer() {
        return useRetroactiveConsumer;
    }

    /**
     * Sets whether or not retroactive consumers are enabled. Retroactive consumers allow
     * non-durable topic subscribers to receive old messages that were published before the
     * non-durable subscriber started.
     */
    public void setUseRetroactiveConsumer(boolean useRetroactiveConsumer) {
        this.useRetroactiveConsumer = useRetroactiveConsumer;
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
        if (sessions.size()>1 || session.isTransacted()){
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
    }

    /**
     * Creates a <CODE>TopicSession</CODE> object.
     * 
     * @param transacted
     *            indicates whether the session is transacted
     * @param acknowledgeMode
     *            indicates whether the consumer or the client will acknowledge
     *            any messages it receives; ignored if the session is
     *            transacted. Legal values are
     *            <code>Session.AUTO_ACKNOWLEDGE</code>,
     *            <code>Session.CLIENT_ACKNOWLEDGE</code>, and
     *            <code>Session.DUPS_OK_ACKNOWLEDGE</code>.
     * @return a newly created topic session
     * @throws JMSException
     *             if the <CODE>TopicConnection</CODE> object fails to create
     *             a session due to some internal error or lack of support for
     *             the specific transaction and acknowledgement mode.
     * @see Session#AUTO_ACKNOWLEDGE
     * @see Session#CLIENT_ACKNOWLEDGE
     * @see Session#DUPS_OK_ACKNOWLEDGE
     */
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return new ActiveMQTopicSession((ActiveMQSession) createSession(transacted, acknowledgeMode));
    }

    /**
     * Creates a connection consumer for this connection (optional operation).
     * This is an expert facility not used by regular JMS clients.
     * 
     * @param topic
     *            the topic to access
     * @param messageSelector
     *            only messages with properties matching the message selector
     *            expression are delivered. A value of null or an empty string
     *            indicates that there is no message selector for the message
     *            consumer.
     * @param sessionPool
     *            the server session pool to associate with this connection
     *            consumer
     * @param maxMessages
     *            the maximum number of messages that can be assigned to a
     *            server session at one time
     * @return the connection consumer
     * @throws JMSException
     *             if the <CODE>TopicConnection</CODE> object fails to create
     *             a connection consumer due to some internal error or invalid
     *             arguments for <CODE>sessionPool</CODE> and <CODE>messageSelector</CODE>.
     * @throws javax.jms.InvalidDestinationException
     *             if an invalid topic is specified.
     * @throws javax.jms.InvalidSelectorException
     *             if the message selector is invalid.
     * @see javax.jms.ConnectionConsumer
     */
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return createConnectionConsumer(topic, messageSelector, sessionPool, maxMessages, false);
    }
    
    /**
     * Creates a connection consumer for this connection (optional operation).
     * This is an expert facility not used by regular JMS clients.
     * 
     * @param queue
     *            the queue to access
     * @param messageSelector
     *            only messages with properties matching the message selector
     *            expression are delivered. A value of null or an empty string
     *            indicates that there is no message selector for the message
     *            consumer.
     * @param sessionPool
     *            the server session pool to associate with this connection
     *            consumer
     * @param maxMessages
     *            the maximum number of messages that can be assigned to a
     *            server session at one time
     * @return the connection consumer
     * @throws JMSException
     *             if the <CODE>QueueConnection</CODE> object fails to create
     *             a connection consumer due to some internal error or invalid
     *             arguments for <CODE>sessionPool</CODE> and <CODE>messageSelector</CODE>.
     * @throws javax.jms.InvalidDestinationException
     *             if an invalid queue is specified.
     * @throws javax.jms.InvalidSelectorException
     *             if the message selector is invalid.
     * @see javax.jms.ConnectionConsumer
     */
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return createConnectionConsumer(queue, messageSelector, sessionPool, maxMessages, false);
    }

    /**
     * Creates a connection consumer for this connection (optional operation).
     * This is an expert facility not used by regular JMS clients.
     * 
     * @param destination
     *            the destination to access
     * @param messageSelector
     *            only messages with properties matching the message selector
     *            expression are delivered. A value of null or an empty string
     *            indicates that there is no message selector for the message
     *            consumer.
     * @param sessionPool
     *            the server session pool to associate with this connection
     *            consumer
     * @param maxMessages
     *            the maximum number of messages that can be assigned to a
     *            server session at one time
     * @return the connection consumer
     * @throws JMSException
     *             if the <CODE>Connection</CODE> object fails to create a
     *             connection consumer due to some internal error or invalid
     *             arguments for <CODE>sessionPool</CODE> and <CODE>messageSelector</CODE>.
     * @throws javax.jms.InvalidDestinationException
     *             if an invalid destination is specified.
     * @throws javax.jms.InvalidSelectorException
     *             if the message selector is invalid.
     * @see javax.jms.ConnectionConsumer
     * @since 1.1
     */
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        return createConnectionConsumer(destination, messageSelector, sessionPool, maxMessages, false);
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages, boolean noLocal) throws JMSException {
        
        checkClosed();
        ensureConnectionInfoSent();
        ConsumerId consumerId = createConsumerId();
        ConsumerInfo info = new ConsumerInfo(consumerId);
        info.setDestination(ActiveMQMessageTransformation.transformDestination(destination));
        info.setSelector(messageSelector);
        info.setPrefetchSize(maxMessages);
        info.setNoLocal(noLocal);
        
        // Allows the options on the destination to configure the consumerInfo
        if( info.getDestination().getOptions()!=null ) {
            HashMap options = new HashMap(info.getDestination().getOptions());
            IntrospectionSupport.setProperties(this.info, options, "consumer.");
        }
        
        return new ActiveMQConnectionConsumer(this, sessionPool, info);
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
     * @param transacted
     *            indicates whether the session is transacted
     * @param acknowledgeMode
     *            indicates whether the consumer or the client will acknowledge
     *            any messages it receives; ignored if the session is
     *            transacted. Legal values are
     *            <code>Session.AUTO_ACKNOWLEDGE</code>,
     *            <code>Session.CLIENT_ACKNOWLEDGE</code>, and
     *            <code>Session.DUPS_OK_ACKNOWLEDGE</code>.
     * @return a newly created queue session
     * @throws JMSException
     *             if the <CODE>QueueConnection</CODE> object fails to create
     *             a session due to some internal error or lack of support for
     *             the specific transaction and acknowledgement mode.
     * @see Session#AUTO_ACKNOWLEDGE
     * @see Session#CLIENT_ACKNOWLEDGE
     * @see Session#DUPS_OK_ACKNOWLEDGE
     */
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return new ActiveMQQueueSession((ActiveMQSession) createSession(transacted, acknowledgeMode));
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
            throw new JMSException(
                    "You cannot create a durable subscriber without specifying a unique clientID on a Connection");
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

            if (command.isMessage() && flowControlSleepTime > 0) {
                try {
                    Thread.sleep(flowControlSleepTime);
                } catch (InterruptedException e) {
                }
            }

            try {
                this.transport.oneway(command);
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
    public Response syncSendPacket(Command command) throws JMSException {
        if (isClosed()) {
            throw new ConnectionClosedException();
        } else {

            if (command.isMessage() && flowControlSleepTime > 0) {
                try {
                    Thread.sleep(flowControlSleepTime);
                } catch (InterruptedException e) {
                }
            }

            try {
                Response response = this.transport.request(command);
                if (response.isException()) {
                    ExceptionResponse er = (ExceptionResponse) response;
                    if (er.getException() instanceof JMSException)
                        throw (JMSException) er.getException();
                    else
                        throw JMSExceptionSupport.create(er.getException());
                }
                return response;
            } catch (IOException e) {
                throw JMSExceptionSupport.create(e);
            }
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    /**
     * @return statistics for this Connection
     */
    public StatsImpl getStats() {
        return stats;
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
        // Can we skip sending the ConnectionInfo packet??
        if (isConnectionInfoSentToBroker) {
            return;
        }

        if (info.getClientId() == null || info.getClientId().trim().length() == 0) {
            info.setClientId(clientIdGenerator.generateId());
        }
        syncSendPacket(info);
        
        this.isConnectionInfoSentToBroker = true;
        // Add a temp destination advisory consumer so that 
        // We know what the valid temporary destinations are on the 
        // broker without having to do an RPC to the broker.
        
        ConsumerId consumerId = new ConsumerId(new SessionId(info.getConnectionId(), -1),consumerIdGenerator.getNextSequenceId());
        advisoryConsumer = new AdvisoryConsumer(this, consumerId);
        
    }

    /**
     * @return Returns the useAsyncSend.
     */
    public boolean isUseAsyncSend() {
        return useAsyncSend;
    }

    /**
     * @param useAsyncSend
     *            The useAsyncSend to set.
     */
    public void setUseAsyncSend(boolean useAsyncSend) {
        this.useAsyncSend = useAsyncSend;
    }

    /**
     * Cleans up this connection so that it's state is as if the connection was
     * just created. This allows the Resource Adapter to clean up a connection
     * so that it can be reused without having to close and recreate the
     * connection.
     * 
     */
    public void cleanup() throws JMSException {
        
        if( advisoryConsumer!=null ) {
            advisoryConsumer.dispose();
            advisoryConsumer=null;
        }
        
        for (Iterator i = this.sessions.iterator(); i.hasNext();) {
            ActiveMQSession s = (ActiveMQSession) i.next();
            s.dispose();
        }
        for (Iterator i = this.connectionConsumers.iterator(); i.hasNext();) {
            ActiveMQConnectionConsumer c = (ActiveMQConnectionConsumer) i.next();
            c.dispose();
        }
        for (Iterator i = this.inputStreams.iterator(); i.hasNext();) {
            ActiveMQInputStream c = (ActiveMQInputStream) i.next();
            c.dispose();
        }
        for (Iterator i = this.outputStreams.iterator(); i.hasNext();) {
            ActiveMQOutputStream c = (ActiveMQOutputStream) i.next();
            c.dispose();
        }

        if( isConnectionInfoSentToBroker ) {
            asyncSendPacket(info.createRemoveCommand());
            isConnectionInfoSentToBroker = false;
        }        
        if( userSpecifiedClientID ) {
            info.setClientId(null);
            userSpecifiedClientID=false;
        }
        clientIDSet = false;

        started.set(false);
    }

    /**
     * Changes the associated username/password that is associated with this
     * connection. If the connection has been used, you must called cleanup()
     * before calling this method.
     * 
     * @throws IllegalStateException
     *             if the connection is in used.
     * 
     */
    public void changeUserInfo(String userName, String password) throws JMSException {
        if (isConnectionInfoSentToBroker)
            throw new IllegalStateException("changeUserInfo used Connection is not allowed");

        this.info.setUserName(userName);
        this.info.setPassword(password);
    }

    /**
     * @return Returns the resourceManagerId.
     * @throws JMSException
     */
    public String getResourceManagerId() throws JMSException {
        waitForBrokerInfo();
        if( resourceManagerId==null )
            throw new JMSException("Resource manager id could not be determined.");            
        return resourceManagerId;
    }

    /**
     * @return Returns the RedeliveryPolicy.
     * @throws JMSException
     */
    public RedeliveryPolicy getRedeliveryPolicy() throws JMSException {
        waitForBrokerInfo();
        return redeliveryPolicy;
    }

    private void waitForBrokerInfo() throws JMSException {
        try {
            brokerInfoReceived.await();
        } catch (InterruptedException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    // Package protected so that it can be used in unit tests
    Transport getTransport() {
        return transport;
    }

    public void addDispatcher(ConsumerId consumerId, ActiveMQDispatcher dispatcher) {
        dispatchers.put(consumerId, dispatcher); 
    }
    public void removeDispatcher(ConsumerId consumerId) {
        dispatchers.remove(consumerId);   
    }
    
    /**
     * @param command - the command to consume
     */
    public void onCommand(Command command) {
        if (!closed.get() && command != null) {
            if (command.isMessageDispatch()) {
                MessageDispatch md = (MessageDispatch) command;
                ActiveMQDispatcher dispatcher = (ActiveMQDispatcher) dispatchers.get(md.getConsumerId());
                if (dispatcher != null) {
                    // Copy in case a embedded broker is dispatching via vm://
                    // md.getMessage() == null to signal end of queue browse.
                    Message msg = md.getMessage();
                    if( msg!=null ) {
                        msg = msg.copy();
                        msg.setReadOnlyBody(true);
                        msg.setReadOnlyProperties(true);
                        msg.setRedeliveryCounter(md.getRedeliveryCounter());
                        msg.setConnection(this);
                        md.setMessage( msg );
                    }
                    dispatcher.dispatch(md);
                }
            } else if ( command.isBrokerInfo() ) {
                BrokerInfo brokerInfo = (BrokerInfo)command;
                resourceManagerId = brokerInfo.getBrokerId().getBrokerId();
                if( redeliveryPolicy == null ) {
                    if( brokerInfo.getRedeliveryPolicy()!=null ) {
                        redeliveryPolicy = brokerInfo.getRedeliveryPolicy();
                    } else {
                        redeliveryPolicy = new RedeliveryPolicy();
                    }
                }
                brokerInfoReceived.countDown();
            }
            else if (command instanceof ControlCommand) {
                onControlCommand((ControlCommand) command);
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
                if (!(error instanceof JMSException))
                    error = JMSExceptionSupport.create(error);
                this.exceptionListener.onException((JMSException) error);
            } else {
                log.warn("Async exception with no exception listener: " + error, error);
            }
        }
    }

    public void onException(IOException error) {
        onAsyncException(error);
        ServiceSupport.dispose(this.transport);
        brokerInfoReceived.countDown();
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
        if( topic ) {
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
        activeTempDestinations.put(dest,dest);
        return dest;
    }
    
    /**
     * 
     * @param destination
     * @throws JMSException
     */
    public void deleteTempDestination(ActiveMQTempDestination destination) throws JMSException {
        
        checkClosed();        
        activeTempDestinations.remove(destination);

        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(this.info.getConnectionId());
        info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
        info.setDestination(destination);
        info.setTimeout(1000*5);
        syncSendPacket(info);
    }



    public boolean isDeleted(ActiveMQDestination dest) {
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

    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }    

    public void destroyDestination(ActiveMQDestination destination) throws JMSException {
        
        checkClosed();
        ensureConnectionInfoSent();

        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(this.info.getConnectionId());
        info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
        info.setDestination(destination);
        info.setTimeout(1000*5);
        syncSendPacket(info);

    }

    public boolean isAsyncDispatch() {
        return asyncDispatch;
    }

    public void setAsyncDispatch(boolean asyncDispatch) {
        this.asyncDispatch = asyncDispatch;
    }

    public boolean isObjectMessageSerializationDefered() {
        return objectMessageSerializationDefered;
    }

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
        checkClosed();
        ensureConnectionInfoSent();
        return new ActiveMQInputStream(this, createConsumerId(), ActiveMQDestination.transform(dest), messageSelector, noLocal, subName, prefetchPolicy.getInputStreamPrefetch());
    }


    public OutputStream createOutputStream(Destination dest) throws JMSException {
        return createOutputStream(dest, null, ActiveMQMessage.DEFAULT_DELIVERY_MODE, ActiveMQMessage.DEFAULT_PRIORITY, ActiveMQMessage.DEFAULT_TIME_TO_LIVE);
    }

    public OutputStream createOutputStream(Destination dest, Map streamProperties, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();
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
     * is an active <CODE>MessageConsumer </CODE> or <CODE>TopicSubscriber</CODE>
     * for the subscription, or while a consumed message is part of a pending
     * transaction or has not been acknowledged in the session.
     * 
     * @param name
     *            the name used to identify this subscription
     * @throws JMSException
     *             if the session fails to unsubscribe to the durable
     *             subscription due to some internal error.
     * @throws InvalidDestinationException
     *             if an invalid subscription name is specified.
     * @since 1.1
     */
    public void unsubscribe(String name) throws JMSException {
        checkClosed();
        RemoveSubscriptionInfo rsi = new RemoveSubscriptionInfo();
        rsi.setConnectionId(getConnectionInfo().getConnectionId());
        rsi.setSubcriptionName(name);
        rsi.setClientId(getConnectionInfo().getClientId());
        syncSendPacket(rsi);
    }

    /**
     * Internal send method optimized:
     *  - It does not copy the message
     *  - It can only handle ActiveMQ messages.
     *  - You can specify if the send is async or sync 
     *  - Does not allow you to send /w a transaction.
     */
    void send(ActiveMQDestination destination, ActiveMQMessage msg, MessageId messageId, int deliveryMode, int priority, long timeToLive, boolean async) throws JMSException {
        checkClosed();

        if( destination.isTemporary() && isDeleted(destination) ) {
            throw new JMSException("Cannot publish to a deleted Destination: "+destination);
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
        msg.setMessageId( messageId );
        
        msg.onSend();
        
        msg.setProducerId(msg.getMessageId().getProducerId());

        if (log.isDebugEnabled()) {
            log.debug("Sending message: " + msg);
        }

        if( async) {
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
                log.info("JVM told to shutdown");
                System.exit(0);
            }
        }
    }

    public void setCopyMessageOnSend(boolean copyMessageOnSend) {
        this.copyMessageOnSend = copyMessageOnSend;
    }
    
    public String toString() {
        return "ActiveMQConnection {id="+info.getConnectionId()+",clientId"+info.getClientId()+",started="+started.get()+"}";
    }
}