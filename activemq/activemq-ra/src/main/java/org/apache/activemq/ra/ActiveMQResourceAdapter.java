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
package org.apache.activemq.ra;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

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
public class ActiveMQResourceAdapter implements ResourceAdapter, Serializable {

    private static final long serialVersionUID = -5417363537865649130L;
    private static final Log log = LogFactory.getLog(ActiveMQResourceAdapter.class);
    
    private final HashMap endpointWorkers = new HashMap();
    private final ActiveMQConnectionRequestInfo info = new ActiveMQConnectionRequestInfo();

    private BootstrapContext bootstrapContext;
    private String brokerXmlConfig;
    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;

    public ActiveMQResourceAdapter() {
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
                throw new ResourceAdapterInternalException("Failed to startup an embedded broker: "+e, e);
            }
        }
    }

    public ActiveMQConnection makeConnection() throws JMSException {
        if (connectionFactory != null) {
            return makeConnection(info, connectionFactory);
        }
        else {
            return makeConnection(info);
        }
    }

    /**
     */
    public ActiveMQConnection makeConnection(ActiveMQConnectionRequestInfo info) throws JMSException {

        ActiveMQConnectionFactory connectionFactory = createConnectionFactory(info);
        return makeConnection(info, connectionFactory);
    }

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
    public ActiveMQConnection makeConnection(ActiveMQActivationSpec activationSpec) throws JMSException {
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
     * @return
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
     * @return
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
        if (activationSpec.getResourceAdapter() != this) {
            throw new ResourceException("Activation spec not initialized with this ResourceAdapter instance");
        }

        if (activationSpec.getClass().equals(ActiveMQActivationSpec.class)) {

            ActiveMQEndpointActivationKey key = new ActiveMQEndpointActivationKey(endpointFactory,
                    (ActiveMQActivationSpec) activationSpec);
            // This is weird.. the same endpoint activated twice.. must be a
            // container error.
            if (endpointWorkers.containsKey(key)) {
                throw new IllegalStateException("Endpoint previously activated");
            }

            ActiveMQEndpointWorker worker = new ActiveMQEndpointWorker(this, key);

            endpointWorkers.put(key, worker);
            worker.start();

        } else {
            throw new NotSupportedException("That type of ActicationSpec not supported: " + activationSpec.getClass());
        }

    }

    /**
     * @see javax.resource.spi.ResourceAdapter#endpointDeactivation(javax.resource.spi.endpoint.MessageEndpointFactory,
     *      javax.resource.spi.ActivationSpec)
     */
    public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec activationSpec) {

        if (activationSpec.getClass().equals(ActiveMQActivationSpec.class)) {
            ActiveMQEndpointActivationKey key = new ActiveMQEndpointActivationKey(endpointFactory, (ActiveMQActivationSpec) activationSpec);
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
            } else {
                return new XAResource[] {};
            }
        } catch (JMSException e) {
            throw new ResourceException(e);
        } finally {
            try {
                connection.close();
            } catch (Throwable ignore) {
            }
        }
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // Java Bean getters and setters for this ResourceAdapter class.
    //
    // ///////////////////////////////////////////////////////////////////////

    /**
     * @return
     */
    public String getClientid() {
        return emptyToNull(info.getClientid());
    }

    /**
     * @return
     */
    public String getPassword() {
        return emptyToNull(info.getPassword());
    }

    /**
     * @return
     */
    public String getServerUrl() {
        return info.getServerUrl();
    }

    /**
     * @return
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
     * @see #setUseEmbeddedBroker(Boolean)
     */
    public void setBrokerXmlConfig(String brokerXmlConfig) {
        this.brokerXmlConfig=brokerXmlConfig;
    }

    public Integer getDurableTopicPrefetch() {
        return info.getDurableTopicPrefetch();
    }

    public Long getInitialRedeliveryDelay() {
        return info.getInitialRedeliveryDelay();
    }

    public Integer getInputStreamPrefetch() {
        return info.getInputStreamPrefetch();
    }

    public Integer getMaximumRedeliveries() {
        return info.getMaximumRedeliveries();
    }

    public Integer getQueueBrowserPrefetch() {
        return info.getQueueBrowserPrefetch();
    }

    public Integer getQueuePrefetch() {
        return info.getQueuePrefetch();
    }

    public Short getRedeliveryBackOffMultiplier() {
        return info.getRedeliveryBackOffMultiplier();
    }

    public Boolean getRedeliveryUseExponentialBackOff() {
        return info.getRedeliveryUseExponentialBackOff();
    }

    public Integer getTopicPrefetch() {
        return info.getTopicPrefetch();
    }

    public boolean isUseInboundSessionEnabled() {
        return info.isUseInboundSessionEnabled();
    }

    public void setAllPrefetchValues(Integer i) {
        info.setAllPrefetchValues(i);
    }

    public void setDurableTopicPrefetch(Integer durableTopicPrefetch) {
        info.setDurableTopicPrefetch(durableTopicPrefetch);
    }

    public void setInitialRedeliveryDelay(Long value) {
        info.setInitialRedeliveryDelay(value);
    }

    public void setInputStreamPrefetch(Integer inputStreamPrefetch) {
        info.setInputStreamPrefetch(inputStreamPrefetch);
    }

    public void setMaximumRedeliveries(Integer value) {
        info.setMaximumRedeliveries(value);
    }

    public void setQueueBrowserPrefetch(Integer queueBrowserPrefetch) {
        info.setQueueBrowserPrefetch(queueBrowserPrefetch);
    }

    public void setQueuePrefetch(Integer queuePrefetch) {
        info.setQueuePrefetch(queuePrefetch);
    }

    public void setRedeliveryBackOffMultiplier(Short value) {
        info.setRedeliveryBackOffMultiplier(value);
    }

    public void setRedeliveryUseExponentialBackOff(Boolean value) {
        info.setRedeliveryUseExponentialBackOff(value);
    }

    public void setTopicPrefetch(Integer topicPrefetch) {
        info.setTopicPrefetch(topicPrefetch);
    }

    /**
     * @return Returns the info.
     */
    public ActiveMQConnectionRequestInfo getInfo() {
        return info;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActiveMQResourceAdapter)) {
            return false;
        }

        final ActiveMQResourceAdapter activeMQResourceAdapter = (ActiveMQResourceAdapter) o;

        if (!info.equals(activeMQResourceAdapter.info)) {
            return false;
        }
        if ( notEqual(brokerXmlConfig, activeMQResourceAdapter.brokerXmlConfig) ) {
            return false;
        }

        return true;
    }

    private boolean notEqual(Object o1, Object o2) {
        return (o1 == null ^ o2 == null) || (o1 != null && !o1.equals(o2));
    }


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

    public Boolean getUseInboundSession() {
        return info.getUseInboundSession();
    }

    public void setUseInboundSession(Boolean useInboundSession) {
        info.setUseInboundSession(useInboundSession);
    }

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
