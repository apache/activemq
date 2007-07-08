/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.activemq.blob.BlobTransferPolicy;
import org.apache.activemq.jndi.JNDIBaseStorable;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.management.StatsCapable;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.URISupport.CompositeData;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/**
 * A ConnectionFactory is an an Administered object, and is used for creating
 * Connections. <p/> This class also implements QueueConnectionFactory and
 * TopicConnectionFactory. You can use this connection to create both
 * QueueConnections and TopicConnections.
 *
 * @version $Revision: 1.9 $
 * @see javax.jms.ConnectionFactory
 */
public class ActiveMQConnectionFactory extends JNDIBaseStorable implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory, StatsCapable, Cloneable {

    public static final String DEFAULT_BROKER_URL = "tcp://localhost:61616";
    public static final String DEFAULT_USER = null;
    public static final String DEFAULT_PASSWORD = null;
    public static final int DEFAULT_PRODUCER_WINDOW_SIZE = 0;

    private IdGenerator clientIdGenerator;
    private String clientIDPrefix;
    protected URI brokerURL;
    protected String userName;
    protected String password;
    protected String clientID;

    // client policies
    private ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
    private RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
    private BlobTransferPolicy blobTransferPolicy = new BlobTransferPolicy();
    private MessageTransformer transformer;

    private boolean disableTimeStampsByDefault = false;
    private boolean optimizedMessageDispatch = true;
    private boolean copyMessageOnSend = true;
    private boolean useCompression = false;
    private boolean objectMessageSerializationDefered = false;
    protected boolean dispatchAsync = false;
    protected boolean alwaysSessionAsync=true;
    private boolean useAsyncSend = false;
    private boolean optimizeAcknowledge = false;
    private int closeTimeout = 15000;
    private boolean useRetroactiveConsumer;
    private boolean exclusiveConsumer;
    private boolean nestedMapAndListEnabled = true;
    JMSStatsImpl factoryStats = new JMSStatsImpl();
    private boolean alwaysSyncSend;
    private boolean watchTopicAdvisories=true;
    private int producerWindowSize=DEFAULT_PRODUCER_WINDOW_SIZE;
    private long warnAboutUnstartedConnectionTimeout = 500L;

    static protected final Executor DEFAULT_CONNECTION_EXECUTOR = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
            public Thread newThread(Runnable run) {
                Thread thread = new Thread(run);
                thread.setPriority(ThreadPriorities.INBOUND_CLIENT_CONNECTION);
                return thread;
            }
        });

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

    /**
     * Returns a copy of the given connection factory
     */
    public ActiveMQConnectionFactory copy() {
        try {
            return (ActiveMQConnectionFactory) super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException("This should never happen: " + e, e);
        }
    }

    /**
     * @param brokerURL
     * @return
     * @throws URISyntaxException
     */
    private static URI createURI(String brokerURL) {
        try {
            return new URI(brokerURL);
        }
        catch (URISyntaxException e) {
            throw (IllegalArgumentException) new IllegalArgumentException("Invalid broker URI: " + brokerURL).initCause(e);
        }
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
     * @return Returns the Connection.
     */
    public Connection createConnection() throws JMSException {
        return createActiveMQConnection();
    }

    /**
     * @return Returns the Connection.
     */
    public Connection createConnection(String userName, String password) throws JMSException {
        return createActiveMQConnection(userName, password);
    }

    /**
     * @return Returns the QueueConnection.
     * @throws JMSException
     */
    public QueueConnection createQueueConnection() throws JMSException {
        return createActiveMQConnection();
    }

    /**
     * @return Returns the QueueConnection.
     */
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return createActiveMQConnection(userName, password);
    }

    /**
     * @return Returns the TopicConnection.
     * @throws JMSException
     */
    public TopicConnection createTopicConnection() throws JMSException {
        return createActiveMQConnection();
    }

    /**
     * @return Returns the TopicConnection.
     */
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return createActiveMQConnection(userName, password);
    }

    public StatsImpl getStats() {
        // TODO
        return null;
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
     * Creates a Transport based on this object's connection settings.
     * 
     * Separated from createActiveMQConnection to allow for subclasses to
     *      override.
     * 
     * @return The newly created Transport.
     * @throws JMSException If unable to create trasnport.
     * 
     * @author sepandm@gmail.com
     */
    protected Transport createTransport() throws JMSException {
        try {
            return TransportFactory.connect(brokerURL,DEFAULT_CONNECTION_EXECUTOR);
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
        ActiveMQConnection connection=null;
        try {
        	Transport transport = createTransport();
        	connection = createActiveMQConnection(transport, factoryStats);
        	
            connection.setUserName(userName);
            connection.setPassword(password);

            configureConnection(connection);

            transport.start();

            if( clientID !=null )
                connection.setDefaultClientID(clientID);

            return connection;
        }
        catch (JMSException e) {
        	// Clean up!
        	try { connection.close(); } catch ( Throwable ignore ) {}
            throw e;
        }
        catch (Exception e) {
        	// Clean up!
        	try { connection.close(); } catch ( Throwable ignore ) {}
        	throw JMSExceptionSupport.create("Could not connect to broker URL: " + brokerURL + ". Reason: " + e, e);
        }
    }

    protected ActiveMQConnection createActiveMQConnection(Transport transport, JMSStatsImpl stats) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnection(transport, getClientIdGenerator(), stats);
        return connection;
    }


    protected void configureConnection(ActiveMQConnection connection) {
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
        connection.setUseRetroactiveConsumer(isUseRetroactiveConsumer());
        connection.setRedeliveryPolicy(getRedeliveryPolicy());
        connection.setTransformer(getTransformer());
        connection.setBlobTransferPolicy(getBlobTransferPolicy().copy());
        connection.setWatchTopicAdvisories(isWatchTopicAdvisories());
        connection.setProducerWindowSize(getProducerWindowSize());
        connection.setWarnAboutUnstartedConnectionTimeout(getWarnAboutUnstartedConnectionTimeout());
    }

    // /////////////////////////////////////////////
    //
    // Property Accessors
    //
    // /////////////////////////////////////////////

    public String getBrokerURL() {
        return brokerURL==null?null:brokerURL.toString();
    }
    
    /**
     * Sets the <a
     * href="http://activemq.apache.org/configuring-transports.html">connection
     * URL</a> used to connect to the ActiveMQ broker.
     */
    public void setBrokerURL(String brokerURL) {
        this.brokerURL = createURI(brokerURL);

        // Use all the properties prefixed with 'jms.' to set the connection factory
        // options.
        if( this.brokerURL.getQuery() !=null ) {
            // It might be a standard URI or...
            try {

                Map map = URISupport.parseQuery(this.brokerURL.getQuery());
                if( buildFromMap(IntrospectionSupport.extractProperties(map, "jms.")) ) {
                    this.brokerURL = URISupport.createRemainingURI(this.brokerURL, map);
                }

            } catch (URISyntaxException e) {
            }

        } else {

            // It might be a composite URI.
            try {
                CompositeData data = URISupport.parseComposite(this.brokerURL);
                if( buildFromMap(IntrospectionSupport.extractProperties(data.getParameters(), "jms.")) ) {
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
     * Sets the JMS clientID to use for the created connection. Note that this can only be used by one connection at once so generally its a better idea
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

    public  ActiveMQPrefetchPolicy getPrefetchPolicy() {
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
     * Sets the policy used to describe how out-of-band BLOBs (Binary Large OBjects)
     * are transferred from producers to brokers to consumers
     */
    public void setBlobTransferPolicy(BlobTransferPolicy blobTransferPolicy) {
        this.blobTransferPolicy = blobTransferPolicy;
    }

    /**
     * Forces the use of <a
     * href="http://activemq.apache.org/async-sends.html">Async Sends</a>
     * which adds a massive performance boost; but means that the send() method
     * will return immediately whether the message has been sent or not which
     * could lead to message loss.
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
    public boolean isAlwaysSyncSend(){
        return this.alwaysSyncSend;
    }

    /**
     * Set true if always require messages to be sync sent
     * @param alwaysSyncSend
     */
    public void setAlwaysSyncSend(boolean alwaysSyncSend){
        this.alwaysSyncSend=alwaysSyncSend;
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
     * Sets whether or not retroactive consumers are enabled. Retroactive consumers allow
     * non-durable topic subscribers to receive old messages that were published before the
     * non-durable subscriber started.
     */
    public void setUseRetroactiveConsumer(boolean useRetroactiveConsumer) {
        this.useRetroactiveConsumer = useRetroactiveConsumer;
    }

    public boolean isExclusiveConsumer() {
        return exclusiveConsumer;
    }

    /**
     * Enables or disables whether or not queue consumers should be exclusive or not
     * for example to preserve ordering when not using
     * <a href="http://activemq.apache.org/message-groups.html">Message Groups</a>
     *
     * @param exclusiveConsumer
     */
    public void setExclusiveConsumer(boolean exclusiveConsumer) {
        this.exclusiveConsumer = exclusiveConsumer;
    }

    public RedeliveryPolicy getRedeliveryPolicy() {
        return redeliveryPolicy;
    }

    /**
     * Sets the global redelivery policy to be used when a message is delivered but the session is rolled back
     */
    public void setRedeliveryPolicy(RedeliveryPolicy redeliveryPolicy) {
        this.redeliveryPolicy = redeliveryPolicy;
    }

    public MessageTransformer getTransformer() {
        return transformer;
    }

    /**
     * Sets the transformer used to transform messages before they are sent on to the JMS bus
     * or when they are received from the bus but before they are delivered to the JMS client
     */
    public void setTransformer(MessageTransformer transformer) {
        this.transformer = transformer;
    }

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
        
        buildFromMap(properties);    	
    }
    
    public boolean buildFromMap(Map properties) {
    	boolean rc=false;
    	
        ActiveMQPrefetchPolicy p = new ActiveMQPrefetchPolicy(); 
        if( IntrospectionSupport.setProperties(p, properties, "prefetchPolicy.") ) {
        	setPrefetchPolicy(p);
        	rc = true;
        }

        RedeliveryPolicy rp = new RedeliveryPolicy();
        if ( IntrospectionSupport.setProperties(rp, properties, "redeliveryPolicy.") ) {
            setRedeliveryPolicy(rp);
            rc = true;
        }

        BlobTransferPolicy blobTransferPolicy = new BlobTransferPolicy();
        if ( IntrospectionSupport.setProperties(blobTransferPolicy, properties, "blobTransferPolicy.") ) {
            setBlobTransferPolicy(blobTransferPolicy);
            rc = true;
        }

        rc |= IntrospectionSupport.setProperties(this, properties);

        return rc;
    }

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
        props.setProperty("statsEnabled",Boolean.toString(isStatsEnabled()));
        props.setProperty("alwaysSyncSend",Boolean.toString(isAlwaysSyncSend()));
        props.setProperty("producerWindowSize", Integer.toString(getProducerWindowSize()));
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
     * synchronously or asynchronously by the broker</a>.
     * 
     * For non-durable topics for example we typically dispatch synchronously by
     * default to minimize context switches which boost performance. However
     * sometimes its better to go slower to ensure that a single blocked
     * consumer socket does not block delivery to other consumers.
     * 
     * @param asyncDispatch
     *            If true then consumers created on this connection will default
     *            to having their messages dispatched asynchronously. The
     *            default value is false.
     */
    public void setDispatchAsync(boolean asyncDispatch) {
        this.dispatchAsync = asyncDispatch;
    }

    /**
     * @return Returns the closeTimeout.
     */
    public int getCloseTimeout(){
        return closeTimeout;
    }

    /**
     * Sets the timeout before a close is considered complete. Normally a
     * close() on a connection waits for confirmation from the broker; this
     * allows that operation to timeout to save the client hanging if there is
     * no broker
     */
    public void setCloseTimeout(int closeTimeout){
        this.closeTimeout=closeTimeout;
    }

    /**
     * @return Returns the alwaysSessionAsync.
     */
    public boolean isAlwaysSessionAsync(){
        return alwaysSessionAsync;
    }

    /**
     * If this flag is set then a separate thread is not used for dispatching
     * messages for each Session in the Connection. However, a separate thread
     * is always used if there is more than one session, or the session isn't in
     * auto acknowledge or duplicates ok mode
     */
    public void setAlwaysSessionAsync(boolean alwaysSessionAsync){
        this.alwaysSessionAsync=alwaysSessionAsync;
    }

    /**
     * @return Returns the optimizeAcknowledge.
     */
    public boolean isOptimizeAcknowledge(){
        return optimizeAcknowledge;
    }

    /**
     * @param optimizeAcknowledge The optimizeAcknowledge to set.
     */
    public void setOptimizeAcknowledge(boolean optimizeAcknowledge){
        this.optimizeAcknowledge=optimizeAcknowledge;
    }
    
    public boolean isNestedMapAndListEnabled() {
        return nestedMapAndListEnabled ;
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
     * Sets the prefix used by autogenerated JMS Client ID values which are
     * used if the JMS client does not explicitly specify on.
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
            }
            else {
                clientIdGenerator = new IdGenerator();
            }
        }
        return clientIdGenerator;
    }

    protected void setClientIdGenerator(IdGenerator clientIdGenerator) {
        this.clientIdGenerator = clientIdGenerator;
    }

    
    /**
     * @return the statsEnabled
     */
    public boolean isStatsEnabled(){
        return this.factoryStats.isEnabled();
    }

    
    /**
     * @param statsEnabled the statsEnabled to set
     */
    public void setStatsEnabled(boolean statsEnabled){
        this.factoryStats.setEnabled(statsEnabled);
    }

	synchronized public int getProducerWindowSize() {
		return producerWindowSize;
	}

	synchronized public void setProducerWindowSize(int producerWindowSize) {
		this.producerWindowSize = producerWindowSize;
	}


    public long getWarnAboutUnstartedConnectionTimeout() {
        return warnAboutUnstartedConnectionTimeout;
    }

    /**
     * Enables the timeout from a connection creation to when a warning is generated
     * if the connection is not properly started via {@link Connection#start()} and a message is received by a consumer.
     *
     * It is a very common gotcha to forget to
     * <a href="http://activemq.apache.org/i-am-not-receiving-any-messages-what-is-wrong.html">start the connection</a>
     * so this option makes the default case to create a warning if the user forgets.
     * To disable the warning just set the value to < 0 (say -1).
     */
    public void setWarnAboutUnstartedConnectionTimeout(long warnAboutUnstartedConnectionTimeout) {
        this.warnAboutUnstartedConnectionTimeout = warnAboutUnstartedConnectionTimeout;
    }
}
