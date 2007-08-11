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
package org.apache.activemq.ra;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Knows how to connect to one ActiveMQ server. It can then activate endpoints
 * and deliver messages to those end points using the connection configure in the
 * resource adapter. <p/>Must override equals and hashCode (JCA spec 16.4)
 *
 * @org.apache.xbean.XBean element="resourceAdapter" rootElement="true"
 * description="The JCA Resource Adaptor for ActiveMQ"
 *
 * @version $Revision$
 */
public class ActiveMQResourceAdapter implements MessageResourceAdapter, Serializable {

    private static final long serialVersionUID = -5417363537865649130L;
    private static final Log log = LogFactory.getLog(ActiveMQResourceAdapter.class);
    
    private final HashMap endpointWorkers = new HashMap();
    private final ActiveMQConnectionRequestInfo info = new ActiveMQConnectionRequestInfo();

    private BootstrapContext bootstrapContext;
    private String brokerXmlConfig;
    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;

    /**
     * 
     */
    public ActiveMQResourceAdapter() {
    	super();
    }

    /**
     * @see javax.resource.spi.ResourceAdapter#start(javax.resource.spi.BootstrapContext)
     */
    public void start(BootstrapContext bootstrapContext) throws ResourceAdapterInternalException {
        this.bootstrapContext = bootstrapContext;
        if (brokerXmlConfig!=null && brokerXmlConfig.trim().length()>0 ) {
            try {
                broker = BrokerFactory.createBroker(new URI(brokerXmlConfig));
                broker.start();
            } catch (Throwable e) {
                throw new ResourceAdapterInternalException("Failed to startup an embedded broker: "+brokerXmlConfig+", due to: "+e, e);
            }
        }
    }

    /**
     * @see org.apache.activemq.ra.MessageResourceAdapter#makeConnection()
     */
    public ActiveMQConnection makeConnection() throws JMSException {
        if (connectionFactory != null) {
            return makeConnection(info, connectionFactory);
        }
        return makeConnection(info);
    }

    /**
     */
    public ActiveMQConnection makeConnection(ActiveMQConnectionRequestInfo info) throws JMSException {

        ActiveMQConnectionFactory connectionFactory = createConnectionFactory(info);
        return makeConnection(info, connectionFactory);
    }

    /**
     * @see org.apache.activemq.ra.MessageResourceAdapter#makeConnection(org.apache.activemq.ra.ActiveMQConnectionRequestInfo, org.apache.activemq.ActiveMQConnectionFactory)
     */
    public ActiveMQConnection makeConnection(ActiveMQConnectionRequestInfo info, ActiveMQConnectionFactory connectionFactory) throws JMSException {
        String userName = info.getUserName();
        String password = info.getPassword();
        ActiveMQConnection physicalConnection = (ActiveMQConnection) connectionFactory.createConnection(userName, password);

        String clientId = info.getClientid();
        if (clientId != null && clientId.length() > 0) {
            physicalConnection.setClientID(clientId);
        }
        return physicalConnection;
    }

    /**
     * @param activationSpec
     */
    public ActiveMQConnection makeConnection(MessageActivationSpec activationSpec) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = createConnectionFactory(info);
        String userName = defaultValue(activationSpec.getUserName(), info.getUserName());
        String password = defaultValue(activationSpec.getPassword(), info.getPassword());
        String clientId = activationSpec.getClientId();
        if (clientId != null) {
            connectionFactory.setClientID(clientId);
        }
        else {
            if (activationSpec.isDurableSubscription()) {
                log.warn("No clientID specified for durable subscription: " + activationSpec);
            }
        }
        ActiveMQConnection physicalConnection = (ActiveMQConnection) connectionFactory.createConnection(userName, password);

        // have we configured a redelivery policy
        RedeliveryPolicy redeliveryPolicy = activationSpec.redeliveryPolicy();
        if (redeliveryPolicy != null) {
            physicalConnection.setRedeliveryPolicy(redeliveryPolicy);
        }
        return physicalConnection;
    }

    /**
     * @param info
     * @throws JMSException
     * @throws URISyntaxException
     */
    synchronized private ActiveMQConnectionFactory createConnectionFactory(ActiveMQConnectionRequestInfo info) throws JMSException {
        ActiveMQConnectionFactory factory = connectionFactory;
        if (factory != null && info.isConnectionFactoryConfigured()) {
            factory = factory.copy();
        }
        else if (factory == null) {
            factory = new ActiveMQConnectionFactory();
        }
        info.configure(factory);
        return factory;
    }

    private String defaultValue(String value, String defaultValue) {
        if (value != null)
            return value;
        return defaultValue;
    }

    /**
     * @see javax.resource.spi.ResourceAdapter#stop()
     */
    public void stop() {
        while (endpointWorkers.size() > 0) {
            ActiveMQEndpointActivationKey key = (ActiveMQEndpointActivationKey) endpointWorkers.keySet().iterator().next();
            endpointDeactivation(key.getMessageEndpointFactory(), key.getActivationSpec());
        }
        if (broker != null) {
            ServiceSupport.dispose(broker);
            broker = null;
        }
        this.bootstrapContext = null;
    }

    /**
     * @see org.apache.activemq.ra.MessageResourceAdapter#getBootstrapContext()
     */
    public BootstrapContext getBootstrapContext() {
        return bootstrapContext;
    }

    /**
     * @see javax.resource.spi.ResourceAdapter#endpointActivation(javax.resource.spi.endpoint.MessageEndpointFactory,
     *      javax.resource.spi.ActivationSpec)
     */
    public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec activationSpec)
            throws ResourceException {

        // spec section 5.3.3
        if (!equals(activationSpec.getResourceAdapter())) {
            throw new ResourceException("Activation spec not initialized with this ResourceAdapter instance (" + activationSpec.getResourceAdapter() + " != " + this + ")");
        }

        if (!(activationSpec instanceof MessageActivationSpec)) {
            throw new NotSupportedException("That type of ActicationSpec not supported: " + activationSpec.getClass());
        }

        ActiveMQEndpointActivationKey key = new ActiveMQEndpointActivationKey(endpointFactory,
                (MessageActivationSpec) activationSpec);
        // This is weird.. the same endpoint activated twice.. must be a
        // container error.
        if (endpointWorkers.containsKey(key)) {
            throw new IllegalStateException("Endpoint previously activated");
        }

        ActiveMQEndpointWorker worker = new ActiveMQEndpointWorker(this, key);

        endpointWorkers.put(key, worker);
        worker.start();
    }

    /**
     * @see javax.resource.spi.ResourceAdapter#endpointDeactivation(javax.resource.spi.endpoint.MessageEndpointFactory,
     *      javax.resource.spi.ActivationSpec)
     */
    public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec activationSpec) {

        if (activationSpec instanceof MessageActivationSpec) {
            ActiveMQEndpointActivationKey key = new ActiveMQEndpointActivationKey(endpointFactory, (MessageActivationSpec) activationSpec);
            ActiveMQEndpointWorker worker = (ActiveMQEndpointWorker) endpointWorkers.remove(key);
            if (worker == null) {
                // This is weird.. that endpoint was not activated.. oh well..
                // this method
                // does not throw exceptions so just return.
                return;
            }
            try {
                worker.stop();
            } catch (InterruptedException e) {
                // We interrupted.. we won't throw an exception but will stop
                // waiting for the worker
                // to stop.. we tried our best. Keep trying to interrupt the
                // thread.
                Thread.currentThread().interrupt();
            }

        }

    }

    /**
     * We only connect to one resource manager per ResourceAdapter instance, so
     * any ActivationSpec will return the same XAResource.
     *
     * @see javax.resource.spi.ResourceAdapter#getXAResources(javax.resource.spi.ActivationSpec[])
     */
    public XAResource[] getXAResources(ActivationSpec[] activationSpecs) throws ResourceException {
        Connection connection = null;
        try {
            connection = makeConnection();
            if (connection instanceof XAConnection) {
                XASession session = ((XAConnection) connection).createXASession();
                XAResource xaResource = session.getXAResource();
                return new XAResource[] { xaResource };
            }
            return new XAResource[] {};
        } catch (JMSException e) {
            throw new ResourceException(e);
        } finally {
            try {
                connection.close();
            } catch (Throwable ignore) {
            	//
            }
        }
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // Java Bean getters and setters for this ResourceAdapter class.
    //
    // ///////////////////////////////////////////////////////////////////////

    /**
     * @return client id
     */
    public String getClientid() {
        return emptyToNull(info.getClientid());
    }

    /**
     * @return password
     */
    public String getPassword() {
        return emptyToNull(info.getPassword());
    }

    /**
     * @return server URL
     */
    public String getServerUrl() {
        return info.getServerUrl();
    }

    /**
     * @return user name
     */
    public String getUserName() {
        return emptyToNull(info.getUserName());
    }

    /**
     * @param clientid
     */
    public void setClientid(String clientid) {
        info.setClientid(clientid);
    }

    /**
     * @param password
     */
    public void setPassword(String password) {
        info.setPassword(password);
    }

    /**
     * @param url
     */
    public void setServerUrl(String url) {
        info.setServerUrl(url);
    }

    /**
     * @param userid
     */
    public void setUserName(String userid) {
        info.setUserName(userid);
    }

    /**
     * @see org.apache.activemq.ra.MessageResourceAdapter#getBrokerXmlConfig()
     */
    public String getBrokerXmlConfig() {
        return brokerXmlConfig;
    }

    /**
     * Sets the <a href="http://activemq.org/Xml+Configuration">XML
     * configuration file </a> used to configure the ActiveMQ broker via Spring
     * if using embedded mode.
     *
     * @param brokerXmlConfig
     *            is the filename which is assumed to be on the classpath unless
     *            a URL is specified. So a value of <code>foo/bar.xml</code>
     *            would be assumed to be on the classpath whereas
     *            <code>file:dir/file.xml</code> would use the file system.
     *            Any valid URL string is supported.
     */
    public void setBrokerXmlConfig(String brokerXmlConfig) {
        this.brokerXmlConfig=brokerXmlConfig;
    }

    /**
     * @return durable topic prefetch
     */
    public Integer getDurableTopicPrefetch() {
        return info.getDurableTopicPrefetch();
    }

    /**
     * @return initial redelivery delay
     */
    public Long getInitialRedeliveryDelay() {
        return info.getInitialRedeliveryDelay();
    }

    /**
     * @return input stream prefetch
     */
    public Integer getInputStreamPrefetch() {
        return info.getInputStreamPrefetch();
    }

    /**
     * @return maximum redeliveries
     */
    public Integer getMaximumRedeliveries() {
        return info.getMaximumRedeliveries();
    }

    /**
     * @return queue browser prefetch
     */
    public Integer getQueueBrowserPrefetch() {
        return info.getQueueBrowserPrefetch();
    }

    /**
     * @return queue prefetch
     */
    public Integer getQueuePrefetch() {
        return info.getQueuePrefetch();
    }

    /**
     * @return redelivery backoff multiplier
     */
    public Short getRedeliveryBackOffMultiplier() {
        return info.getRedeliveryBackOffMultiplier();
    }

    /**
     * @return redelivery use exponential backoff
     */
    public Boolean getRedeliveryUseExponentialBackOff() {
        return info.getRedeliveryUseExponentialBackOff();
    }

    /**
     * @return topic prefetch
     */
    public Integer getTopicPrefetch() {
        return info.getTopicPrefetch();
    }

    /**
     * @return use inbound session enabled
     */
    public boolean isUseInboundSessionEnabled() {
        return info.isUseInboundSessionEnabled();
    }

    /**
     * @param i
     */
    public void setAllPrefetchValues(Integer i) {
        info.setAllPrefetchValues(i);
    }

    /**
     * @param durableTopicPrefetch
     */
    public void setDurableTopicPrefetch(Integer durableTopicPrefetch) {
        info.setDurableTopicPrefetch(durableTopicPrefetch);
    }

    /**
     * @param value
     */
    public void setInitialRedeliveryDelay(Long value) {
        info.setInitialRedeliveryDelay(value);
    }

    /**
     * @param inputStreamPrefetch
     */
    public void setInputStreamPrefetch(Integer inputStreamPrefetch) {
        info.setInputStreamPrefetch(inputStreamPrefetch);
    }

    /**
     * @param value
     */
    public void setMaximumRedeliveries(Integer value) {
        info.setMaximumRedeliveries(value);
    }

    /**
     * @param queueBrowserPrefetch
     */
    public void setQueueBrowserPrefetch(Integer queueBrowserPrefetch) {
        info.setQueueBrowserPrefetch(queueBrowserPrefetch);
    }

    /**
     * @param queuePrefetch
     */
    public void setQueuePrefetch(Integer queuePrefetch) {
        info.setQueuePrefetch(queuePrefetch);
    }

    /**
     * @param value
     */
    public void setRedeliveryBackOffMultiplier(Short value) {
        info.setRedeliveryBackOffMultiplier(value);
    }

    /**
     * @param value
     */
    public void setRedeliveryUseExponentialBackOff(Boolean value) {
        info.setRedeliveryUseExponentialBackOff(value);
    }

    /**
     * @param topicPrefetch
     */
    public void setTopicPrefetch(Integer topicPrefetch) {
        info.setTopicPrefetch(topicPrefetch);
    }

    /**
     * @return Returns the info.
     */
    public ActiveMQConnectionRequestInfo getInfo() {
        return info;
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageResourceAdapter)) {
            return false;
        }

        final MessageResourceAdapter activeMQResourceAdapter = (MessageResourceAdapter) o;

        if (!info.equals(activeMQResourceAdapter.getInfo())) {
            return false;
        }
        if ( notEqual(brokerXmlConfig, activeMQResourceAdapter.getBrokerXmlConfig()) ) {
            return false;
        }

        return true;
    }

    private boolean notEqual(Object o1, Object o2) {
        return (o1 == null ^ o2 == null) || (o1 != null && !o1.equals(o2));
    }


    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        int result;
        result = info.hashCode();
        if( brokerXmlConfig !=null ) {
            result ^= brokerXmlConfig.hashCode();
        }
        return result;
    }

    private String emptyToNull(String value) {
        if (value == null || value.length() == 0) {
            return null;
        }
        return value;
    }

    /**
     * @return use inbound session
     */
    public Boolean getUseInboundSession() {
        return info.getUseInboundSession();
    }

    /**
     * @param useInboundSession
     */
    public void setUseInboundSession(Boolean useInboundSession) {
        info.setUseInboundSession(useInboundSession);
    }

    /**
     * @see org.apache.activemq.ra.MessageResourceAdapter#getConnectionFactory()
     */
    public ActiveMQConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * This allows a connection factory to be configured and shared between a ResourceAdaptor and outbound messaging.
     * Note that setting the connectionFactory will overload many of the properties on this POJO such as the redelivery
     * and prefetch policies; the properties on the connectionFactory will be used instead.
     */
    public void setConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }


}
