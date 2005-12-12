/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
 *
 **/
package org.activemq;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.Context;

import org.activemq.management.JMSStatsImpl;
import org.activemq.management.StatsCapable;
import org.activemq.management.StatsImpl;
import org.activemq.transport.Transport;
import org.activemq.transport.TransportFactory;
import org.activemq.util.IntrospectionSupport;
import org.activemq.util.JMSExceptionSupport;
import org.activemq.util.URISupport;
import org.activemq.util.URISupport.CompositeData;

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
public class ActiveMQConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory, StatsCapable {

    public static final String DEFAULT_BROKER_URL = "tcp://localhost:61616";
    public static final String DEFAULT_USER = null;
    public static final String DEFAULT_PASSWORD = null;

    protected URI brokerURL;
    protected String userName;
    protected String password;
    protected String clientID;

    protected boolean useEmbeddedBroker;

    // optimization flags
    private ActiveMQPrefetchPolicy prefetchPolicy = new ActiveMQPrefetchPolicy();
    private boolean disableTimeStampsByDefault = false;
    private boolean onSendPrepareMessageBody = true;
    private boolean optimizedMessageDispatch = true;
    private boolean copyMessageOnSend = true;
    private boolean useCompression = false;
    private boolean objectMessageSerializationDefered = false;
    protected boolean asyncDispatch = true;
    private boolean useAsyncSend = false;
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
        return createActiveMQConnection(userName, password);
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
        return createActiveMQConnection(userName, password);
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
        return createActiveMQConnection(userName, password);
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

    /**
     * @return Returns the Connection.
     */
    private ActiveMQConnection createActiveMQConnection(String userName, String password) throws JMSException {
        if (brokerURL == null) {
            throw new ConfigurationException("brokerURL not set.");
        }
        Transport transport;
        try {
            transport = TransportFactory.connect(brokerURL,DEFAULT_CONNECTION_EXECUTOR);
            ActiveMQConnection connection = new ActiveMQConnection(transport, userName, password, factoryStats);

            connection.setPrefetchPolicy(getPrefetchPolicy());
            connection.setDisableTimeStampsByDefault(isDisableTimeStampsByDefault());
            connection.setOnSendPrepareMessageBody(isOnSendPrepareMessageBody());
            connection.setOptimizedMessageDispatch(isOptimizedMessageDispatch());
            connection.setCopyMessageOnSend(isCopyMessageOnSend());
            connection.setUseCompression(isUseCompression());
            connection.setObjectMessageSerializationDefered(isObjectMessageSerializationDefered());
            connection.setAsyncDispatch(isAsyncDispatch());
            connection.setUseAsyncSend(isUseAsyncSend());
            connection.setUseRetroactiveConsumer(isUseRetroactiveConsumer());
            
            return connection;
        }
        catch (JMSException e) {
            throw e;
        }
        catch (Exception e) {
            throw JMSExceptionSupport.create("Could not connect to broker URL: " + brokerURL + ". Reason: " + e, e);
        }
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

    public boolean isUseEmbeddedBroker() {
        return useEmbeddedBroker;
    }

    public void setUseEmbeddedBroker(boolean useEmbeddedBroker) {
        this.useEmbeddedBroker = useEmbeddedBroker;
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

    /**
     * set the properties for this instance as retrieved from JNDI
     * 
     * @param properties
     */
    public void setProperties(Properties properties) throws URISyntaxException {
        
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

    public boolean isOnSendPrepareMessageBody() {
        return onSendPrepareMessageBody;
    }

    public void setOnSendPrepareMessageBody(boolean onSendPrepareMessageBody) {
        this.onSendPrepareMessageBody = onSendPrepareMessageBody;
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

}
