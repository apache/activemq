/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;
import javax.naming.Referenceable;
import javax.naming.Reference;
import javax.naming.NamingException;

import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.management.StatsCapable;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.URISupport.CompositeData;
import org.apache.activemq.jndi.JNDIBaseStorable;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.ScheduledThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadFactory;

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

    protected URI brokerURL;
    protected String userName;
    protected String password;
    protected String clientID;

    // optimization flags
    private ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
    private RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();

    private boolean disableTimeStampsByDefault = false;
    private boolean optimizedMessageDispatch = true;
    private boolean copyMessageOnSend = true;
    private boolean useCompression = false;
    private boolean objectMessageSerializationDefered = false;
    protected boolean asyncDispatch = false;
    protected boolean alwaysSessionAsync=true;
    private boolean useAsyncSend = false;
    private boolean optimizeAcknowledge = false;
    private int closeTimeout = 15000;
    private boolean useRetroactiveConsumer;

    JMSStatsImpl factoryStats = new JMSStatsImpl();

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
     * @return Returns the Connection.
     */
    protected ActiveMQConnection createActiveMQConnection(String userName, String password) throws JMSException {
        if (brokerURL == null) {
            throw new ConfigurationException("brokerURL not set.");
        }
        Transport transport;
        try {
            transport = TransportFactory.connect(brokerURL,DEFAULT_CONNECTION_EXECUTOR);
            ActiveMQConnection connection = createActiveMQConnection(transport, factoryStats);

            connection.setUserName(userName);
            connection.setPassword(password);
            connection.setPrefetchPolicy(getPrefetchPolicy());
            connection.setDisableTimeStampsByDefault(isDisableTimeStampsByDefault());
            connection.setOptimizedMessageDispatch(isOptimizedMessageDispatch());
            connection.setCopyMessageOnSend(isCopyMessageOnSend());
            connection.setUseCompression(isUseCompression());
            connection.setObjectMessageSerializationDefered(isObjectMessageSerializationDefered());
            connection.setAsyncDispatch(isAsyncDispatch());
            connection.setUseAsyncSend(isUseAsyncSend());
            connection.setAlwaysSessionAsync(isAlwaysSessionAsync());
            connection.setOptimizeAcknowledge(isOptimizeAcknowledge());
            connection.setUseRetroactiveConsumer(isUseRetroactiveConsumer());
            connection.setRedeliveryPolicy(getRedeliveryPolicy());

            transport.start();

            if( clientID !=null )
                connection.setClientID(clientID);

            return connection;
        }
        catch (JMSException e) {
            throw e;
        }
        catch (Exception e) {
            throw JMSExceptionSupport.create("Could not connect to broker URL: " + brokerURL + ". Reason: " + e, e);
        }
    }

    protected ActiveMQConnection createActiveMQConnection(Transport transport, JMSStatsImpl stats) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnection(transport, stats);
        return connection;
    }

    // /////////////////////////////////////////////
    //
    // Property Accessors
    //
    // /////////////////////////////////////////////

    public String getBrokerURL() {
        return brokerURL==null?null:brokerURL.toString();
    }
    public void setBrokerURL(String brokerURL) {
        this.brokerURL = createURI(brokerURL);

        // Use all the properties prefixed with 'jms.' to set the connection factory
        // options.
        if( this.brokerURL.getQuery() !=null ) {
            // It might be a standard URI or...
            try {

                Map map = URISupport.parseQuery(this.brokerURL.getQuery());
                if( IntrospectionSupport.setProperties(this, map, "jms.") ) {
                    this.brokerURL = URISupport.createRemainingURI(this.brokerURL, map);
                }

            } catch (URISyntaxException e) {
            }

        } else {

            // It might be a composite URI.
            try {
                CompositeData data = URISupport.parseComposite(this.brokerURL);
                if( IntrospectionSupport.setProperties(this, data.getParameters(), "jms.") ) {
                    this.brokerURL = data.toURI();
                }
            } catch (URISyntaxException e) {
            }
        }
    }

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public boolean isCopyMessageOnSend() {
        return copyMessageOnSend;
    }

    public void setCopyMessageOnSend(boolean copyMessageOnSend) {
        this.copyMessageOnSend = copyMessageOnSend;
    }

    public boolean isDisableTimeStampsByDefault() {
        return disableTimeStampsByDefault;
    }

    public void setDisableTimeStampsByDefault(boolean disableTimeStampsByDefault) {
        this.disableTimeStampsByDefault = disableTimeStampsByDefault;
    }

    public boolean isOptimizedMessageDispatch() {
        return optimizedMessageDispatch;
    }

    public void setOptimizedMessageDispatch(boolean optimizedMessageDispatch) {
        this.optimizedMessageDispatch = optimizedMessageDispatch;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public ActiveMQPrefetchPolicy getPrefetchPolicy() {
        return prefetchPolicy;
    }

    public void setPrefetchPolicy(ActiveMQPrefetchPolicy prefetchPolicy) {
        this.prefetchPolicy = prefetchPolicy;
    }

    public boolean isUseAsyncSend() {
        return useAsyncSend;
    }

    public void setUseAsyncSend(boolean useAsyncSend) {
        this.useAsyncSend = useAsyncSend;
    }

    public String getUserName() {
        return userName;
    }

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

    public RedeliveryPolicy getRedeliveryPolicy() {
        return redeliveryPolicy;
    }

    /**
     * Sets the global redelivery policy to be used when a message is delivered but the session is rolled back
     */
    public void setRedeliveryPolicy(RedeliveryPolicy redeliveryPolicy) {
        this.redeliveryPolicy = redeliveryPolicy;
    }

    public void buildFromProperties(Properties properties) {

        if (properties == null) {
            properties = new Properties();
        }

        IntrospectionSupport.setProperties(this, properties);

        String temp = properties.getProperty(Context.PROVIDER_URL);
        if (temp == null || temp.length() == 0) {
            temp = properties.getProperty("brokerURL");
        }
        if (temp != null && temp.length() > 0) {
            setBrokerURL(temp);
        }
    }

    public void populateProperties(Properties props) {
        props.setProperty("asyncDispatch", Boolean.toString(isAsyncDispatch()));

        if (getBrokerURL() != null) {
            props.setProperty(Context.PROVIDER_URL, getBrokerURL());
            props.setProperty("brokerURL", getBrokerURL());
        }

        if (getClientID() != null) {
            props.setProperty("clientID", getClientID());
        }

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

        if (getUserName() != null) {
            props.setProperty("userName", getUserName());
        }
        
        props.setProperty("closeTimeout", Integer.toString(getCloseTimeout()));
        props.setProperty("alwaysSessionAsync", Boolean.toString(isAlwaysSessionAsync()));
        props.setProperty("optimizeAcknowledge", Boolean.toString(isOptimizeAcknowledge()));

    }

    public boolean isUseCompression() {
        return useCompression;
    }

    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    public boolean isObjectMessageSerializationDefered() {
        return objectMessageSerializationDefered;
    }

    public void setObjectMessageSerializationDefered(boolean objectMessageSerializationDefered) {
        this.objectMessageSerializationDefered = objectMessageSerializationDefered;
    }

    public boolean isAsyncDispatch() {
        return asyncDispatch;
    }

    public void setAsyncDispatch(boolean asyncDispatch) {
        this.asyncDispatch = asyncDispatch;
    }

    /**
     * @return Returns the closeTimeout.
     */
    public int getCloseTimeout(){
        return closeTimeout;
    }

    /**
     * @param closeTimeout The closeTimeout to set.
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
     * @param alwaysSessionAsync The alwaysSessionAsync to set.
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
}
