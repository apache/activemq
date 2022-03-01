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
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.RejectedExecutionHandler;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;

import org.apache.activemq.blob.BlobTransferPolicy;
import org.apache.activemq.broker.region.policy.RedeliveryPolicyMap;
import org.apache.activemq.jndi.JNDIBaseStorable;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.management.StatsCapable;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.*;
import org.apache.activemq.util.URISupport.CompositeData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ConnectionFactory is an an Administered object, and is used for creating
 * Connections. <p/> This class also implements QueueConnectionFactory and
 * TopicConnectionFactory. You can use this connection to create both
 * QueueConnections and TopicConnections.
 *
 *
 * @see javax.jms.ConnectionFactory
 */
public class ActiveMQConnectionFactory extends JNDIBaseStorable implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory, StatsCapable, Cloneable {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQConnectionFactory.class);
    private static final String DEFAULT_BROKER_HOST;
    private static final int DEFAULT_BROKER_PORT;
    static{
        String host = null;
        String port = null;
        try {
             host = AccessController.doPrivileged(new PrivilegedAction<String>() {
                 @Override
                 public String run() {
                     String result = System.getProperty("org.apache.activemq.AMQ_HOST");
                     result = (result==null||result.isEmpty()) ?  System.getProperty("AMQ_HOST","localhost") : result;
                     return result;
                 }
             });
             port = AccessController.doPrivileged(new PrivilegedAction<String>() {
                 @Override
                 public String run() {
                     String result = System.getProperty("org.apache.activemq.AMQ_PORT");
                     result = (result==null||result.isEmpty()) ?  System.getProperty("AMQ_PORT","61616") : result;
                     return result;
                 }
             });
        }catch(Throwable e){
            LOG.debug("Failed to look up System properties for host and port",e);
        }
        host = (host == null || host.isEmpty()) ? "localhost" : host;
        port = (port == null || port.isEmpty()) ? "61616" : port;
        DEFAULT_BROKER_HOST = host;
        DEFAULT_BROKER_PORT = Integer.parseInt(port);
    }


    public static final String DEFAULT_BROKER_BIND_URL;

    static{
        final String defaultURL = "tcp://" + DEFAULT_BROKER_HOST + ":" + DEFAULT_BROKER_PORT;
        String bindURL = null;

        try {
            bindURL = AccessController.doPrivileged(new PrivilegedAction<String>() {
                @Override
                public String run() {
                    String result = System.getProperty("org.apache.activemq.BROKER_BIND_URL");
                    result = (result==null||result.isEmpty()) ?  System.getProperty("BROKER_BIND_URL",defaultURL) : result;
                    return result;
                }
            });
        }catch(Throwable e){
            LOG.debug("Failed to look up System properties for host and port",e);
        }
        bindURL = (bindURL == null || bindURL.isEmpty()) ? defaultURL : bindURL;
        DEFAULT_BROKER_BIND_URL = bindURL;
    }

    public static final String DEFAULT_BROKER_URL = "failover://"+DEFAULT_BROKER_BIND_URL;
    public static final String DEFAULT_USER = null;
    public static final String DEFAULT_PASSWORD = null;
    public static final int DEFAULT_PRODUCER_WINDOW_SIZE = 0;

    protected URI brokerURL;
    protected String userName;
    protected String password;
    protected String clientID;
    protected boolean dispatchAsync=true;
    protected boolean alwaysSessionAsync=true;

    JMSStatsImpl factoryStats = new JMSStatsImpl();

    private IdGenerator clientIdGenerator;
    private String clientIDPrefix;
    private IdGenerator connectionIdGenerator;
    private String connectionIDPrefix;

    // client policies
    private ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
    private RedeliveryPolicyMap redeliveryPolicyMap = new RedeliveryPolicyMap();
    {
        redeliveryPolicyMap.setDefaultEntry(new RedeliveryPolicy());
    }
    private BlobTransferPolicy blobTransferPolicy = new BlobTransferPolicy();
    private MessageTransformer transformer;

    private boolean disableTimeStampsByDefault;
    private boolean optimizedMessageDispatch = true;
    private long optimizeAcknowledgeTimeOut = 300;
    private long optimizedAckScheduledAckInterval = 0;
    private boolean copyMessageOnSend = true;
    private boolean useCompression;
    private boolean objectMessageSerializationDefered;
    private boolean useAsyncSend;
    private boolean optimizeAcknowledge;
    private int closeTimeout = 15000;
    private boolean useRetroactiveConsumer;
    private boolean exclusiveConsumer;
    private boolean nestedMapAndListEnabled = true;
    private boolean alwaysSyncSend;
    private boolean watchTopicAdvisories = true;
    private int producerWindowSize = DEFAULT_PRODUCER_WINDOW_SIZE;
    private long warnAboutUnstartedConnectionTimeout = 500L;
    private int sendTimeout = 0;
    private int connectResponseTimeout = 0;
    private boolean sendAcksAsync=true;
    private TransportListener transportListener;
    private ExceptionListener exceptionListener;
    private int auditDepth = ActiveMQMessageAudit.DEFAULT_WINDOW_SIZE;
    private int auditMaximumProducerNumber = ActiveMQMessageAudit.MAXIMUM_PRODUCER_COUNT;
    private boolean useDedicatedTaskRunner;
    private long consumerFailoverRedeliveryWaitPeriod = 0;
    private boolean checkForDuplicates = true;
    private ClientInternalExceptionListener clientInternalExceptionListener;
    private boolean messagePrioritySupported = false;
    private boolean transactedIndividualAck = false;
    private boolean nonBlockingRedelivery = false;
    private int maxThreadPoolSize = ActiveMQConnection.DEFAULT_THREAD_POOL_SIZE;
    private TaskRunnerFactory sessionTaskRunner;
    private RejectedExecutionHandler rejectedTaskHandler = null;
    protected int xaAckMode = -1; // ensure default init before setting via brokerUrl introspection in sub class
    private boolean rmIdFromConnectionId = false;
    private boolean consumerExpiryCheckEnabled = true;
    private List<String> trustedPackages = Arrays.asList(ClassLoadingAwareObjectInputStream.serializablePackages);
    private boolean trustAllPackages = false;

    // /////////////////////////////////////////////
    //
    // ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory Methods
    //
    // /////////////////////////////////////////////

    public ActiveMQConnectionFactory() {
        this(DEFAULT_BROKER_URL);
    }

    public ActiveMQConnectionFactory(String brokerURL) {
        this(createURI(brokerURL));
    }

    public ActiveMQConnectionFactory(URI brokerURL) {
        setBrokerURL(brokerURL.toString());
    }

    public ActiveMQConnectionFactory(String userName, String password, URI brokerURL) {
        setUserName(userName);
        setPassword(password);
        setBrokerURL(brokerURL.toString());
    }

    public ActiveMQConnectionFactory(String userName, String password, String brokerURL) {
        setUserName(userName);
        setPassword(password);
        setBrokerURL(brokerURL);
    }

    /**
     * Returns a copy of the given connection factory
     */
    public ActiveMQConnectionFactory copy() {
        try {
            return (ActiveMQConnectionFactory)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("This should never happen: " + e, e);
        }
    }

    /*boolean*
     * @param brokerURL
     * @return
     * @throws URISyntaxException
     */
    private static URI createURI(String brokerURL) {
        try {
            return new URI(brokerURL);
        } catch (URISyntaxException e) {
            throw (IllegalArgumentException)new IllegalArgumentException("Invalid broker URI: " + brokerURL).initCause(e);
        }
    }

    /**
     * @return Returns the Connection.
     */
    @Override
    public Connection createConnection() throws JMSException {
        return createActiveMQConnection();
    }

    /**
     * @return Returns the Connection.
     */
    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return createActiveMQConnection(userName, password);
    }

    /**
     * @return Returns the QueueConnection.
     * @throws JMSException
     */
    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return createActiveMQConnection().enforceQueueOnlyConnection();
    }

    /**
     * @return Returns the QueueConnection.
     */
    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return createActiveMQConnection(userName, password).enforceQueueOnlyConnection();
    }

    /**
     * @return Returns the TopicConnection.
     * @throws JMSException
     */
    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return createActiveMQConnection();
    }

    /**
     * @return Returns the TopicConnection.
     */
    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return createActiveMQConnection(userName, password);
    }

    /**
     * @return the StatsImpl associated with this ConnectionFactory.
     */
    @Override
    public StatsImpl getStats() {
        return this.factoryStats;
    }

    // /////////////////////////////////////////////
    //
    // Implementation methods.
    //
    // /////////////////////////////////////////////

    protected ActiveMQConnection createActiveMQConnection() throws JMSException {
        return createActiveMQConnection(userName, password);
    }

    /**
     * Creates a Transport based on this object's connection settings. Separated
     * from createActiveMQConnection to allow for subclasses to override.
     *
     * @return The newly created Transport.
     * @throws JMSException If unable to create trasnport.
     */
    protected Transport createTransport() throws JMSException {
        try {
            URI connectBrokerUL = brokerURL;
            String scheme = brokerURL.getScheme();
            if (scheme == null) {
                throw new IOException("Transport not scheme specified: [" + brokerURL + "]");
            }
            if (scheme.equals("auto")) {
                connectBrokerUL = new URI(brokerURL.toString().replace("auto", "tcp"));
            } else if (scheme.equals("auto+ssl")) {
                connectBrokerUL = new URI(brokerURL.toString().replace("auto+ssl", "ssl"));
            } else if (scheme.equals("auto+nio")) {
                connectBrokerUL = new URI(brokerURL.toString().replace("auto+nio", "nio"));
            } else if (scheme.equals("auto+nio+ssl")) {
                connectBrokerUL = new URI(brokerURL.toString().replace("auto+nio+ssl", "nio+ssl"));
            }

            return TransportFactory.connect(connectBrokerUL);
        } catch (Exception e) {
            throw JMSExceptionSupport.create("Could not create Transport. Reason: " + e, e);
        }
    }

    /**
     * @return Returns the Connection.
     */
    protected ActiveMQConnection createActiveMQConnection(String userName, String password) throws JMSException {
        if (brokerURL == null) {
            throw new ConfigurationException("brokerURL not set.");
        }
        ActiveMQConnection connection = null;
        try {
            Transport transport = createTransport();
            connection = createActiveMQConnection(transport, factoryStats);

            connection.setUserName(userName);
            connection.setPassword(password);

            configureConnection(connection);

            transport.start();

            if (clientID != null) {
                connection.setDefaultClientID(clientID);
            }

            return connection;
        } catch (JMSException e) {
            // Clean up!
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
            throw e;
        } catch (Exception e) {
            // Clean up!
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
            throw JMSExceptionSupport.create("Could not connect to broker URL: " + brokerURL + ". Reason: " + e, e);
        }
    }

    protected ActiveMQConnection createActiveMQConnection(Transport transport, JMSStatsImpl stats) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnection(transport, getClientIdGenerator(),
                getConnectionIdGenerator(), stats);
        return connection;
    }

    protected void configureConnection(ActiveMQConnection connection) throws JMSException {
        connection.setPrefetchPolicy(getPrefetchPolicy());
        connection.setDisableTimeStampsByDefault(isDisableTimeStampsByDefault());
        connection.setOptimizedMessageDispatch(isOptimizedMessageDispatch());
        connection.setCopyMessageOnSend(isCopyMessageOnSend());
        connection.setUseCompression(isUseCompression());
        connection.setObjectMessageSerializationDefered(isObjectMessageSerializationDefered());
        connection.setDispatchAsync(isDispatchAsync());
        connection.setUseAsyncSend(isUseAsyncSend());
        connection.setAlwaysSyncSend(isAlwaysSyncSend());
        connection.setAlwaysSessionAsync(isAlwaysSessionAsync());
        connection.setOptimizeAcknowledge(isOptimizeAcknowledge());
        connection.setOptimizeAcknowledgeTimeOut(getOptimizeAcknowledgeTimeOut());
        connection.setOptimizedAckScheduledAckInterval(getOptimizedAckScheduledAckInterval());
        connection.setUseRetroactiveConsumer(isUseRetroactiveConsumer());
        connection.setExclusiveConsumer(isExclusiveConsumer());
        connection.setRedeliveryPolicyMap(getRedeliveryPolicyMap());
        connection.setTransformer(getTransformer());
        connection.setBlobTransferPolicy(getBlobTransferPolicy().copy());
        connection.setWatchTopicAdvisories(isWatchTopicAdvisories());
        connection.setProducerWindowSize(getProducerWindowSize());
        connection.setWarnAboutUnstartedConnectionTimeout(getWarnAboutUnstartedConnectionTimeout());
        connection.setSendTimeout(getSendTimeout());
        connection.setCloseTimeout(getCloseTimeout());
        connection.setSendAcksAsync(isSendAcksAsync());
        connection.setAuditDepth(getAuditDepth());
        connection.setAuditMaximumProducerNumber(getAuditMaximumProducerNumber());
        connection.setUseDedicatedTaskRunner(isUseDedicatedTaskRunner());
        connection.setConsumerFailoverRedeliveryWaitPeriod(getConsumerFailoverRedeliveryWaitPeriod());
        connection.setCheckForDuplicates(isCheckForDuplicates());
        connection.setMessagePrioritySupported(isMessagePrioritySupported());
        connection.setTransactedIndividualAck(isTransactedIndividualAck());
        connection.setNonBlockingRedelivery(isNonBlockingRedelivery());
        connection.setMaxThreadPoolSize(getMaxThreadPoolSize());
        connection.setSessionTaskRunner(getSessionTaskRunner());
        connection.setRejectedTaskHandler(getRejectedTaskHandler());
        connection.setNestedMapAndListEnabled(isNestedMapAndListEnabled());
        connection.setRmIdFromConnectionId(isRmIdFromConnectionId());
        connection.setConsumerExpiryCheckEnabled(isConsumerExpiryCheckEnabled());
        connection.setTrustedPackages(getTrustedPackages());
        connection.setTrustAllPackages(isTrustAllPackages());
        connection.setConnectResponseTimeout(getConnectResponseTimeout());
        if (transportListener != null) {
            connection.addTransportListener(transportListener);
        }
        if (exceptionListener != null) {
            connection.setExceptionListener(exceptionListener);
        }
        if (clientInternalExceptionListener != null) {
            connection.setClientInternalExceptionListener(clientInternalExceptionListener);
        }
    }

    // /////////////////////////////////////////////
    //
    // Property Accessors
    //
    // /////////////////////////////////////////////

	public String getBrokerURL() {
        return brokerURL == null ? null : brokerURL.toString();
    }

    /**
     * Sets the <a
     * href="http://activemq.apache.org/configuring-transports.html">connection
     * URL</a> used to connect to the ActiveMQ broker.
     */
    public void setBrokerURL(String brokerURL) {
        this.brokerURL = createURI(brokerURL);

        // Use all the properties prefixed with 'jms.' to set the connection
        // factory
        // options.
        if (this.brokerURL.getQuery() != null) {
            // It might be a standard URI or...
            try {

                Map<String,String> map = URISupport.parseQuery(this.brokerURL.getQuery());
                Map<String,Object> jmsOptionsMap = IntrospectionSupport.extractProperties(map, "jms.");
                if (buildFromMap(jmsOptionsMap)) {
                    if (!jmsOptionsMap.isEmpty()) {
                        String msg = "There are " + jmsOptionsMap.size()
                            + " jms options that couldn't be set on the ConnectionFactory."
                            + " Check the options are spelled correctly."
                            + " Unknown parameters=[" + jmsOptionsMap + "]."
                            + " This connection factory cannot be started.";
                        throw new IllegalArgumentException(msg);
                    }

                    this.brokerURL = URISupport.createRemainingURI(this.brokerURL, map);
                }

            } catch (URISyntaxException e) {
            }

        } else {

            // It might be a composite URI.
            try {
                CompositeData data = URISupport.parseComposite(this.brokerURL);
                Map<String,Object> jmsOptionsMap = IntrospectionSupport.extractProperties(data.getParameters(), "jms.");
                if (buildFromMap(jmsOptionsMap)) {
                    if (!jmsOptionsMap.isEmpty()) {
                        String msg = "There are " + jmsOptionsMap.size()
                            + " jms options that couldn't be set on the ConnectionFactory."
                            + " Check the options are spelled correctly."
                            + " Unknown parameters=[" + jmsOptionsMap + "]."
                            + " This connection factory cannot be started.";
                        throw new IllegalArgumentException(msg);
                    }

                    this.brokerURL = data.toURI();
                }
            } catch (URISyntaxException e) {
            }
        }
    }

    public String getClientID() {
        return clientID;
    }

    /**
     * Sets the JMS clientID to use for the created connection. Note that this
     * can only be used by one connection at once so generally its a better idea
     * to set the clientID on a Connection
     */
    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public boolean isCopyMessageOnSend() {
        return copyMessageOnSend;
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

    public boolean isDisableTimeStampsByDefault() {
        return disableTimeStampsByDefault;
    }

    /**
     * Sets whether or not timestamps on messages should be disabled or not. If
     * you disable them it adds a small performance boost.
     */
    public void setDisableTimeStampsByDefault(boolean disableTimeStampsByDefault) {
        this.disableTimeStampsByDefault = disableTimeStampsByDefault;
    }

    public boolean isOptimizedMessageDispatch() {
        return optimizedMessageDispatch;
    }

    /**
     * If this flag is set then an larger prefetch limit is used - only
     * applicable for durable topic subscribers.
     */
    public void setOptimizedMessageDispatch(boolean optimizedMessageDispatch) {
        this.optimizedMessageDispatch = optimizedMessageDispatch;
    }

    public String getPassword() {
        return password;
    }

    /**
     * Sets the JMS password used for connections created from this factory
     */
    public void setPassword(String password) {
        this.password = password;
    }

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

    public boolean isUseAsyncSend() {
        return useAsyncSend;
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

    public synchronized boolean isWatchTopicAdvisories() {
        return watchTopicAdvisories;
    }

    public synchronized void setWatchTopicAdvisories(boolean watchTopicAdvisories) {
        this.watchTopicAdvisories = watchTopicAdvisories;
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

    public String getUserName() {
        return userName;
    }

    /**
     * Sets the JMS userName used by connections created by this factory
     */
    public void setUserName(String userName) {
        this.userName = userName;
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

    public RedeliveryPolicy getRedeliveryPolicy() {
        return redeliveryPolicyMap.getDefaultEntry();
    }

    /**
     * Sets the global default redelivery policy to be used when a message is delivered
     * but the session is rolled back
     */
    public void setRedeliveryPolicy(RedeliveryPolicy redeliveryPolicy) {
        this.redeliveryPolicyMap.setDefaultEntry(redeliveryPolicy);
    }

    public RedeliveryPolicyMap getRedeliveryPolicyMap() {
        return this.redeliveryPolicyMap;
    }

    /**
     * Sets the global redelivery policy mapping to be used when a message is delivered
     * but the session is rolled back
     */
    public void setRedeliveryPolicyMap(RedeliveryPolicyMap redeliveryPolicyMap) {
        this.redeliveryPolicyMap = redeliveryPolicyMap;
    }

    public MessageTransformer getTransformer() {
        return transformer;
    }

    /**
     * @return the sendTimeout (in milliseconds)
     */
    public int getSendTimeout() {
        return sendTimeout;
    }

    /**
     * @param sendTimeout the sendTimeout to set (in milliseconds)
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
     * @return the messagePrioritySupported
     */
    public boolean isMessagePrioritySupported() {
        return this.messagePrioritySupported;
    }

    /**
     * @param messagePrioritySupported the messagePrioritySupported to set
     */
    public void setMessagePrioritySupported(boolean messagePrioritySupported) {
        this.messagePrioritySupported = messagePrioritySupported;
    }


    /**
     * Sets the transformer used to transform messages before they are sent on
     * to the JMS bus or when they are received from the bus but before they are
     * delivered to the JMS client
     */
    public void setTransformer(MessageTransformer transformer) {
        this.transformer = transformer;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void buildFromProperties(Properties properties) {

        if (properties == null) {
            properties = new Properties();
        }

        String temp = properties.getProperty(Context.PROVIDER_URL);
        if (temp == null || temp.length() == 0) {
            temp = properties.getProperty("brokerURL");
        }
        if (temp != null && temp.length() > 0) {
            setBrokerURL(temp);
        }

        Map<String, Object> p = new HashMap(properties);
        buildFromMap(p);
    }

    public boolean buildFromMap(Map<String, Object> properties) {
        boolean rc = false;

        ActiveMQPrefetchPolicy p = new ActiveMQPrefetchPolicy();
        if (IntrospectionSupport.setProperties(p, properties, "prefetchPolicy.")) {
            setPrefetchPolicy(p);
            rc = true;
        }

        RedeliveryPolicy rp = new RedeliveryPolicy();
        if (IntrospectionSupport.setProperties(rp, properties, "redeliveryPolicy.")) {
            setRedeliveryPolicy(rp);
            rc = true;
        }

        BlobTransferPolicy blobTransferPolicy = new BlobTransferPolicy();
        if (IntrospectionSupport.setProperties(blobTransferPolicy, properties, "blobTransferPolicy.")) {
            setBlobTransferPolicy(blobTransferPolicy);
            rc = true;
        }

        rc |= IntrospectionSupport.setProperties(this, properties);

        return rc;
    }

    @Override
    public void populateProperties(Properties props) {
        props.setProperty("dispatchAsync", Boolean.toString(isDispatchAsync()));

        if (getBrokerURL() != null) {
            props.setProperty(Context.PROVIDER_URL, getBrokerURL());
            props.setProperty("brokerURL", getBrokerURL());
        }

        if (getClientID() != null) {
            props.setProperty("clientID", getClientID());
        }

        IntrospectionSupport.getProperties(getPrefetchPolicy(), props, "prefetchPolicy.");
        IntrospectionSupport.getProperties(getRedeliveryPolicy(), props, "redeliveryPolicy.");
        IntrospectionSupport.getProperties(getBlobTransferPolicy(), props, "blobTransferPolicy.");

        props.setProperty("copyMessageOnSend", Boolean.toString(isCopyMessageOnSend()));
        props.setProperty("disableTimeStampsByDefault", Boolean.toString(isDisableTimeStampsByDefault()));
        props.setProperty("objectMessageSerializationDefered", Boolean.toString(isObjectMessageSerializationDefered()));
        props.setProperty("optimizedMessageDispatch", Boolean.toString(isOptimizedMessageDispatch()));

        if (getPassword() != null) {
            props.setProperty("password", getPassword());
        }

        props.setProperty("useAsyncSend", Boolean.toString(isUseAsyncSend()));
        props.setProperty("useCompression", Boolean.toString(isUseCompression()));
        props.setProperty("useRetroactiveConsumer", Boolean.toString(isUseRetroactiveConsumer()));
        props.setProperty("watchTopicAdvisories", Boolean.toString(isWatchTopicAdvisories()));

        if (getUserName() != null) {
            props.setProperty("userName", getUserName());
        }

        props.setProperty("closeTimeout", Integer.toString(getCloseTimeout()));
        props.setProperty("alwaysSessionAsync", Boolean.toString(isAlwaysSessionAsync()));
        props.setProperty("optimizeAcknowledge", Boolean.toString(isOptimizeAcknowledge()));
        props.setProperty("statsEnabled", Boolean.toString(isStatsEnabled()));
        props.setProperty("alwaysSyncSend", Boolean.toString(isAlwaysSyncSend()));
        props.setProperty("producerWindowSize", Integer.toString(getProducerWindowSize()));
        props.setProperty("sendTimeout", Integer.toString(getSendTimeout()));
        props.setProperty("connectResponseTimeout", Integer.toString(getConnectResponseTimeout()));
        props.setProperty("sendAcksAsync",Boolean.toString(isSendAcksAsync()));
        props.setProperty("auditDepth", Integer.toString(getAuditDepth()));
        props.setProperty("auditMaximumProducerNumber", Integer.toString(getAuditMaximumProducerNumber()));
        props.setProperty("checkForDuplicates", Boolean.toString(isCheckForDuplicates()));
        props.setProperty("messagePrioritySupported", Boolean.toString(isMessagePrioritySupported()));
        props.setProperty("transactedIndividualAck", Boolean.toString(isTransactedIndividualAck()));
        props.setProperty("nonBlockingRedelivery", Boolean.toString(isNonBlockingRedelivery()));
        props.setProperty("maxThreadPoolSize", Integer.toString(getMaxThreadPoolSize()));
        props.setProperty("nestedMapAndListEnabled", Boolean.toString(isNestedMapAndListEnabled()));
        props.setProperty("consumerFailoverRedeliveryWaitPeriod", Long.toString(getConsumerFailoverRedeliveryWaitPeriod()));
        props.setProperty("rmIdFromConnectionId", Boolean.toString(isRmIdFromConnectionId()));
        props.setProperty("consumerExpiryCheckEnabled", Boolean.toString(isConsumerExpiryCheckEnabled()));
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
     *                asynchronously. The default value is true.
     */
    public void setDispatchAsync(boolean asyncDispatch) {
        this.dispatchAsync = asyncDispatch;
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
     * @return Returns the alwaysSessionAsync.
     */
    public boolean isAlwaysSessionAsync() {
        return alwaysSessionAsync;
    }

    /**
     * If this flag is not set then a separate thread is not used for dispatching messages for each Session in
     * the Connection. However, a separate thread is always used if there is more than one session, or the session
     * isn't in auto acknowledge or duplicates ok mode.  By default this value is set to true and session dispatch
     * happens asynchronously.
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
     * @param optimizeAcknowledge The optimizeAcknowledge to set.
     */
    public void setOptimizeAcknowledge(boolean optimizeAcknowledge) {
        this.optimizeAcknowledge = optimizeAcknowledge;
    }

    /**
     * The max time in milliseconds between optimized ack batches
     * @param optimizeAcknowledgeTimeOut
     */
    public void setOptimizeAcknowledgeTimeOut(long optimizeAcknowledgeTimeOut) {
        this.optimizeAcknowledgeTimeOut =  optimizeAcknowledgeTimeOut;
    }

    public long getOptimizeAcknowledgeTimeOut() {
        return optimizeAcknowledgeTimeOut;
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

    public String getClientIDPrefix() {
        return clientIDPrefix;
    }

    /**
     * Sets the prefix used by autogenerated JMS Client ID values which are used
     * if the JMS client does not explicitly specify on.
     *
     * @param clientIDPrefix
     */
    public void setClientIDPrefix(String clientIDPrefix) {
        this.clientIDPrefix = clientIDPrefix;
    }

    protected synchronized IdGenerator getClientIdGenerator() {
        if (clientIdGenerator == null) {
            if (clientIDPrefix != null) {
                clientIdGenerator = new IdGenerator(clientIDPrefix);
            } else {
                clientIdGenerator = new IdGenerator();
            }
        }
        return clientIdGenerator;
    }

    protected void setClientIdGenerator(IdGenerator clientIdGenerator) {
        this.clientIdGenerator = clientIdGenerator;
    }

    /**
     * Sets the prefix used by connection id generator
     * @param connectionIDPrefix
     */
    public void setConnectionIDPrefix(String connectionIDPrefix) {
        this.connectionIDPrefix = connectionIDPrefix;
    }

    protected synchronized IdGenerator getConnectionIdGenerator() {
        if (connectionIdGenerator == null) {
            if (connectionIDPrefix != null) {
                connectionIdGenerator = new IdGenerator(connectionIDPrefix);
            } else {
                connectionIdGenerator = new IdGenerator();
            }
        }
        return connectionIdGenerator;
    }

    protected void setConnectionIdGenerator(IdGenerator connectionIdGenerator) {
        this.connectionIdGenerator = connectionIdGenerator;
    }

    /**
     * @return the statsEnabled
     */
    public boolean isStatsEnabled() {
        return this.factoryStats.isEnabled();
    }

    /**
     * @param statsEnabled the statsEnabled to set
     */
    public void setStatsEnabled(boolean statsEnabled) {
        this.factoryStats.setEnabled(statsEnabled);
    }

    public synchronized int getProducerWindowSize() {
        return producerWindowSize;
    }

    public synchronized void setProducerWindowSize(int producerWindowSize) {
        this.producerWindowSize = producerWindowSize;
    }

    public long getWarnAboutUnstartedConnectionTimeout() {
        return warnAboutUnstartedConnectionTimeout;
    }

    /**
     * Enables the timeout from a connection creation to when a warning is
     * generated if the connection is not properly started via
     * {@link Connection#start()} and a message is received by a consumer. It is
     * a very common gotcha to forget to <a
     * href="http://activemq.apache.org/i-am-not-receiving-any-messages-what-is-wrong.html">start
     * the connection</a> so this option makes the default case to create a
     * warning if the user forgets. To disable the warning just set the value to <
     * 0 (say -1).
     */
    public void setWarnAboutUnstartedConnectionTimeout(long warnAboutUnstartedConnectionTimeout) {
        this.warnAboutUnstartedConnectionTimeout = warnAboutUnstartedConnectionTimeout;
    }

    public TransportListener getTransportListener() {
        return transportListener;
    }

    /**
     * Allows a listener to be configured on the ConnectionFactory so that when this factory is used
     * with frameworks which don't expose the Connection such as Spring JmsTemplate, you can still register
     * a transport listener.
     *
     * @param transportListener sets the listener to be registered on all connections
     * created by this factory
     */
    public void setTransportListener(TransportListener transportListener) {
        this.transportListener = transportListener;
    }


    public ExceptionListener getExceptionListener() {
        return exceptionListener;
    }

    /**
     * Allows an {@link ExceptionListener} to be configured on the ConnectionFactory so that when this factory
     * is used by frameworks which don't expose the Connection such as Spring JmsTemplate, you can register
     * an exception listener.
     * <p> Note: access to this exceptionLinstener will <b>not</b> be serialized if it is associated with more than
     * on connection (as it will be if more than one connection is subsequently created by this connection factory)
     * @param exceptionListener sets the exception listener to be registered on all connections
     * created by this factory
     */
    public void setExceptionListener(ExceptionListener exceptionListener) {
        this.exceptionListener = exceptionListener;
    }

    public int getAuditDepth() {
        return auditDepth;
    }

    public void setAuditDepth(int auditDepth) {
        this.auditDepth = auditDepth;
    }

    public int getAuditMaximumProducerNumber() {
        return auditMaximumProducerNumber;
    }

    public void setAuditMaximumProducerNumber(int auditMaximumProducerNumber) {
        this.auditMaximumProducerNumber = auditMaximumProducerNumber;
    }

    public void setUseDedicatedTaskRunner(boolean useDedicatedTaskRunner) {
        this.useDedicatedTaskRunner = useDedicatedTaskRunner;
    }

    public boolean isUseDedicatedTaskRunner() {
        return useDedicatedTaskRunner;
    }

    public void setConsumerFailoverRedeliveryWaitPeriod(long consumerFailoverRedeliveryWaitPeriod) {
        this.consumerFailoverRedeliveryWaitPeriod = consumerFailoverRedeliveryWaitPeriod;
    }

    public long getConsumerFailoverRedeliveryWaitPeriod() {
        return consumerFailoverRedeliveryWaitPeriod;
    }

    public ClientInternalExceptionListener getClientInternalExceptionListener() {
        return clientInternalExceptionListener;
    }

    /**
     * Allows an {@link ClientInternalExceptionListener} to be configured on the ConnectionFactory so that when this factory
     * is used by frameworks which don't expose the Connection such as Spring JmsTemplate, you can register
     * an exception listener.
     * <p> Note: access to this clientInternalExceptionListener will <b>not</b> be serialized if it is associated with more than
     * on connection (as it will be if more than one connection is subsequently created by this connection factory)
     * @param clientInternalExceptionListener sets the exception listener to be registered on all connections
     * created by this factory
     */
    public void setClientInternalExceptionListener(
            ClientInternalExceptionListener clientInternalExceptionListener) {
        this.clientInternalExceptionListener = clientInternalExceptionListener;
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

    public boolean isTransactedIndividualAck() {
         return transactedIndividualAck;
     }

     /**
      * when true, submit individual transacted acks immediately rather than with transaction completion.
      * This allows the acks to represent delivery status which can be persisted on rollback
      * Used in conjunction with org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter#setRewriteOnRedelivery(boolean)  true
      */
     public void setTransactedIndividualAck(boolean transactedIndividualAck) {
         this.transactedIndividualAck = transactedIndividualAck;
     }


     public boolean isNonBlockingRedelivery() {
         return nonBlockingRedelivery;
     }

     /**
      * When true a MessageConsumer will not stop Message delivery before re-delivering Messages
      * from a rolled back transaction.  This implies that message order will not be preserved and
      * also will result in the TransactedIndividualAck option to be enabled.
      */
     public void setNonBlockingRedelivery(boolean nonBlockingRedelivery) {
         this.nonBlockingRedelivery = nonBlockingRedelivery;
     }

    public int getMaxThreadPoolSize() {
        return maxThreadPoolSize;
    }

    public void setMaxThreadPoolSize(int maxThreadPoolSize) {
        this.maxThreadPoolSize = maxThreadPoolSize;
    }

    public TaskRunnerFactory getSessionTaskRunner() {
        return sessionTaskRunner;
    }

    public void setSessionTaskRunner(TaskRunnerFactory sessionTaskRunner) {
        this.sessionTaskRunner = sessionTaskRunner;
    }

    public RejectedExecutionHandler getRejectedTaskHandler() {
        return rejectedTaskHandler;
    }

    public void setRejectedTaskHandler(RejectedExecutionHandler rejectedTaskHandler) {
        this.rejectedTaskHandler = rejectedTaskHandler;
    }

    /**
     * Gets the configured time interval that is used to force all MessageConsumers that have optimizedAcknowledge enabled
     * to send an ack for any outstanding Message Acks.  By default this value is set to zero meaning that the consumers
     * will not do any background Message acknowledgment.
     *
     * @return the scheduledOptimizedAckInterval
     */
    public long getOptimizedAckScheduledAckInterval() {
        return optimizedAckScheduledAckInterval;
    }

    /**
     * Sets the amount of time between scheduled sends of any outstanding Message Acks for consumers that
     * have been configured with optimizeAcknowledge enabled.
     *
     * @param optimizedAckScheduledAckInterval the scheduledOptimizedAckInterval to set
     */
    public void setOptimizedAckScheduledAckInterval(long optimizedAckScheduledAckInterval) {
        this.optimizedAckScheduledAckInterval = optimizedAckScheduledAckInterval;
    }


    public boolean isRmIdFromConnectionId() {
        return rmIdFromConnectionId;
    }

    /**
     * uses the connection id as the resource identity for XAResource.isSameRM
     * ensuring join will only occur on a single connection
     */
    public void setRmIdFromConnectionId(boolean rmIdFromConnectionId) {
        this.rmIdFromConnectionId = rmIdFromConnectionId;
    }

    /**
     * @return true if MessageConsumer instance will check for expired messages before dispatch.
     */
    public boolean isConsumerExpiryCheckEnabled() {
        return consumerExpiryCheckEnabled;
    }

    /**
     * Controls whether message expiration checking is done in each MessageConsumer
     * prior to dispatching a message.  Disabling this check can lead to consumption
     * of expired messages.
     *
     * @param consumerExpiryCheckEnabled
     *        controls whether expiration checking is done prior to dispatch.
     */
    public void setConsumerExpiryCheckEnabled(boolean consumerExpiryCheckEnabled) {
        this.consumerExpiryCheckEnabled = consumerExpiryCheckEnabled;
    }

    public List<String> getTrustedPackages() {
        return trustedPackages;
    }

    public void setTrustedPackages(List<String> trustedPackages) {
        this.trustedPackages = trustedPackages;
    }

    public boolean isTrustAllPackages() {
        return trustAllPackages;
    }

    public void setTrustAllPackages(boolean trustAllPackages) {
        this.trustAllPackages = trustAllPackages;
    }

	public int getConnectResponseTimeout() {
		return connectResponseTimeout;
	}

	public void setConnectResponseTimeout(int connectResponseTimeout) {
		this.connectResponseTimeout = connectResponseTimeout;
	}
}
