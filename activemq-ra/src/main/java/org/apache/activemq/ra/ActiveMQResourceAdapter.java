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

import java.net.URI;
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

/**
 * Knows how to connect to one ActiveMQ server. It can then activate endpoints
 * and deliver messages to those end points using the connection configure in
 * the resource adapter. <p/>Must override equals and hashCode (JCA spec 16.4)
 * 
 * @org.apache.xbean.XBean element="resourceAdapter" rootElement="true"
 *                         description="The JCA Resource Adaptor for ActiveMQ"
 * @version $Revision$
 */
public class ActiveMQResourceAdapter extends ActiveMQConnectionSupport implements MessageResourceAdapter {

    private final HashMap<ActiveMQEndpointActivationKey, ActiveMQEndpointWorker> endpointWorkers = new HashMap<ActiveMQEndpointActivationKey, ActiveMQEndpointWorker>();

    private BootstrapContext bootstrapContext;
    private String brokerXmlConfig;
    private BrokerService broker;
    private Thread brokerStartThread;
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
        if (brokerXmlConfig != null && brokerXmlConfig.trim().length() > 0) {
            brokerStartThread = new Thread("Starting ActiveMQ Broker") {
                @Override
                public void run () {
                    try {
                        synchronized( ActiveMQResourceAdapter.this ) {
                            broker = BrokerFactory.createBroker(new URI(brokerXmlConfig));
                        }
                        broker.start();
                    } catch (Throwable e) {
                        log.warn("Could not start up embeded ActiveMQ Broker '"+brokerXmlConfig+"': "+e.getMessage());
                        log.debug("Reason for: "+e.getMessage(), e);
                    }
                }
            };
            brokerStartThread.setDaemon(true);
            brokerStartThread.start();
            
            // Wait up to 5 seconds for the broker to start up in the async thread.. otherwise keep going without it..
            try {
                brokerStartThread.join(1000*5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }                
        }
    }

    /**
     * @see org.apache.activemq.ra.MessageResourceAdapter#makeConnection()
     */
    public ActiveMQConnection makeConnection() throws JMSException {
        if( connectionFactory == null ) {
            return makeConnection(getInfo());
        } else {
            return makeConnection(getInfo(), connectionFactory);
        }
    }

    /**
     * @param activationSpec
     */
    public ActiveMQConnection makeConnection(MessageActivationSpec activationSpec) throws JMSException {
        ActiveMQConnectionFactory cf = getConnectionFactory();
        if (cf == null) {
            cf = createConnectionFactory(getInfo());
        }
        String userName = defaultValue(activationSpec.getUserName(), getInfo().getUserName());
        String password = defaultValue(activationSpec.getPassword(), getInfo().getPassword());
        String clientId = activationSpec.getClientId();
        if (clientId != null) {
            cf.setClientID(clientId);
        } else {
            if (activationSpec.isDurableSubscription()) {
                log.warn("No clientID specified for durable subscription: " + activationSpec);
            }
        }
        ActiveMQConnection physicalConnection = (ActiveMQConnection) cf.createConnection(userName, password);

        // have we configured a redelivery policy
        RedeliveryPolicy redeliveryPolicy = activationSpec.redeliveryPolicy();
        if (redeliveryPolicy != null) {
            physicalConnection.setRedeliveryPolicy(redeliveryPolicy);
        }
        return physicalConnection;
    }

    /**
     * @see javax.resource.spi.ResourceAdapter#stop()
     */
    public void stop() {
        while (endpointWorkers.size() > 0) {
            ActiveMQEndpointActivationKey key = endpointWorkers.keySet().iterator().next();
            endpointDeactivation(key.getMessageEndpointFactory(), key.getActivationSpec());
        }
        
        synchronized( this ) {
            if (broker != null) {
                if( brokerStartThread.isAlive() ) {
                    brokerStartThread.interrupt();
                }
                ServiceSupport.dispose(broker);
                broker = null;
            }
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
    public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec activationSpec) throws ResourceException {

        // spec section 5.3.3
        if (!equals(activationSpec.getResourceAdapter())) {
            throw new ResourceException("Activation spec not initialized with this ResourceAdapter instance (" + activationSpec.getResourceAdapter() + " != " + this + ")");
        }

        if (!(activationSpec instanceof MessageActivationSpec)) {
            throw new NotSupportedException("That type of ActicationSpec not supported: " + activationSpec.getClass());
        }

        ActiveMQEndpointActivationKey key = new ActiveMQEndpointActivationKey(endpointFactory, (MessageActivationSpec)activationSpec);
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
            ActiveMQEndpointActivationKey key = new ActiveMQEndpointActivationKey(endpointFactory, (MessageActivationSpec)activationSpec);
            ActiveMQEndpointWorker worker = endpointWorkers.remove(key);
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
                XASession session = ((XAConnection)connection).createXASession();
                XAResource xaResource = session.getXAResource();
                return new XAResource[] {
                    xaResource
                };
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
     * @param brokerXmlConfig is the filename which is assumed to be on the
     *                classpath unless a URL is specified. So a value of
     *                <code>foo/bar.xml</code> would be assumed to be on the
     *                classpath whereas <code>file:dir/file.xml</code> would
     *                use the file system. Any valid URL string is supported.
     */
    public void setBrokerXmlConfig(String brokerXmlConfig) {
        this.brokerXmlConfig = brokerXmlConfig;
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

        final MessageResourceAdapter activeMQResourceAdapter = (MessageResourceAdapter)o;

        if (!getInfo().equals(activeMQResourceAdapter.getInfo())) {
            return false;
        }
        if (notEqual(brokerXmlConfig, activeMQResourceAdapter.getBrokerXmlConfig())) {
            return false;
        }

        return true;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        int result;
        result = getInfo().hashCode();
        if (brokerXmlConfig != null) {
            result ^= brokerXmlConfig.hashCode();
        }
        return result;
    }

    public ActiveMQConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ActiveMQConnectionFactory aConnectionFactory) {
        this.connectionFactory = aConnectionFactory;
    }


    }
