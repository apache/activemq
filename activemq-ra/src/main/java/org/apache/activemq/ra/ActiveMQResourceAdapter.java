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
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;
import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.TransactionContext;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.TransactionContext.toXAException;

/**
 * Knows how to connect to one ActiveMQ server. It can then activate endpoints
 * and deliver messages to those end points using the connection configure in
 * the resource adapter. <p/>Must override equals and hashCode (JCA spec 16.4)
 *
 * @org.apache.xbean.XBean element="resourceAdapter" rootElement="true"
 *                         description="The JCA Resource Adaptor for ActiveMQ"
 *
 */
public class ActiveMQResourceAdapter extends ActiveMQConnectionSupport implements Serializable, MessageResourceAdapter {
    private static final long serialVersionUID = 360805587169336959L;
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQResourceAdapter.class);
    private transient final HashMap<ActiveMQEndpointActivationKey, ActiveMQEndpointWorker> endpointWorkers = new HashMap<ActiveMQEndpointActivationKey, ActiveMQEndpointWorker>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private transient BootstrapContext bootstrapContext;
    private String brokerXmlConfig;
    private transient BrokerService broker;
    private transient Thread brokerStartThread;
    private ActiveMQConnectionFactory connectionFactory;
    private transient ReconnectingXAResource reconnectingXaResource;

    /**
     *
     */
    public ActiveMQResourceAdapter() {
        super();
    }

    /**
     * @see javax.resource.spi.ResourceAdapter#start(javax.resource.spi.BootstrapContext)
     */
    @Override
    public void start(BootstrapContext bootstrapContext) throws ResourceAdapterInternalException {
        log.debug("Start: " + this.getInfo());
        this.bootstrapContext = bootstrapContext;
        if (brokerXmlConfig != null && brokerXmlConfig.trim().length() > 0) {
            brokerStartThread = new Thread("Starting ActiveMQ Broker") {
                @Override
                public void run () {
                    try {
                        // ensure RAR resources are available to xbean (needed for weblogic)
                        log.debug("original thread context classLoader: " + Thread.currentThread().getContextClassLoader());
                        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                        log.debug("current (from getClass()) thread context classLoader: " + Thread.currentThread().getContextClassLoader());

                        synchronized( ActiveMQResourceAdapter.this ) {
                            broker = BrokerFactory.createBroker(new URI(brokerXmlConfig));
                        }
                        broker.start();
                        // Default the ServerUrl to the local broker if not specified in the ra.xml
                        if (getServerUrl() == null) {
                            setServerUrl("vm://" + broker.getBrokerName() + "?create=false");
                        }
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
        started.compareAndSet(false, true);
    }

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
    @Override
    public ActiveMQConnection makeConnection(MessageActivationSpec activationSpec) throws JMSException {
        ActiveMQConnectionFactory cf = getConnectionFactory();
        if (cf == null) {
            cf = createConnectionFactory(getInfo(), activationSpec);
        }
        String userName = defaultValue(activationSpec.getUserName(), getInfo().getUserName());
        String password = defaultValue(activationSpec.getPassword(), getInfo().getPassword());
        String clientId = defaultValue(activationSpec.getClientId(), getInfo().getClientid());
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
    @Override
    public void stop() {
        log.debug("Stop: " + this.getInfo());
        started.compareAndSet(true, false);
        synchronized (endpointWorkers) {
            while (endpointWorkers.size() > 0) {
                ActiveMQEndpointActivationKey key = endpointWorkers.keySet().iterator().next();
                endpointDeactivation(key.getMessageEndpointFactory(), key.getActivationSpec());
            }
        }

        synchronized( this ) {
            if (broker != null) {
                if( brokerStartThread.isAlive() ) {
                    brokerStartThread.interrupt();
                }
                ServiceSupport.dispose(broker);
                broker = null;
            }
            if (reconnectingXaResource != null) {
                reconnectingXaResource.stop();
            }
        }

        this.bootstrapContext = null;
        this.reconnectingXaResource = null;
    }

    /**
     * @see org.apache.activemq.ra.MessageResourceAdapter#getBootstrapContext()
     */
    @Override
    public BootstrapContext getBootstrapContext() {
        return bootstrapContext;
    }

    /**
     * @see javax.resource.spi.ResourceAdapter#endpointActivation(javax.resource.spi.endpoint.MessageEndpointFactory,
     *      javax.resource.spi.ActivationSpec)
     */
    @Override
    public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec activationSpec) throws ResourceException {

        // spec section 5.3.3
        if (!equals(activationSpec.getResourceAdapter())) {
            throw new ResourceException("Activation spec not initialized with this ResourceAdapter instance (" + activationSpec.getResourceAdapter() + " != " + this + ")");
        }

        if (!(activationSpec instanceof MessageActivationSpec)) {
            throw new NotSupportedException("That type of ActivationSpec not supported: " + activationSpec.getClass());
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
    @Override
    public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec activationSpec) {
        if (activationSpec instanceof MessageActivationSpec) {
            ActiveMQEndpointActivationKey key = new ActiveMQEndpointActivationKey(endpointFactory, (MessageActivationSpec)activationSpec);
            ActiveMQEndpointWorker worker = null;
            synchronized (endpointWorkers) {
                worker = endpointWorkers.remove(key);
            }
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
    @Override
    public XAResource[] getXAResources(ActivationSpec[] activationSpecs) throws ResourceException {
        LOG.debug("getXAResources: activationSpecs" + (activationSpecs != null ? Arrays.asList(activationSpecs) : "[]") + ", info: " + getInfo());
        if (!started.get()) {
            LOG.debug("RAR[" + this.getInfo() + "] stopped or undeployed; no connection available for xa recovery");
            return new XAResource[]{};
        }
        try {
            synchronized ( this ) {
                if (reconnectingXaResource == null) {
                    LOG.debug("Init XAResource with: " + this.getInfo());
                    reconnectingXaResource = new ReconnectingXAResource(new TransactionContext(makeConnection()));
                }
            }

            return new XAResource[]{reconnectingXaResource};

        } catch (Exception e) {
            throw new ResourceException(e);
        }
    }

    private void ensureConnection(TransactionContext xaRecoveryTransactionContext) throws XAException {
        final ActiveMQConnection existingConnection  = xaRecoveryTransactionContext.getConnection();
        if (existingConnection == null || existingConnection.isTransportFailed()) {
            try {
                LOG.debug("reconnect XAResource with: " + this.getInfo(), existingConnection == null ? "" : existingConnection.getFirstFailureError());
                xaRecoveryTransactionContext.setConnection(makeConnection());
            } catch (JMSException e) {
                throw toXAException(e);
            } finally {
                if (existingConnection != null) {
                    try {
                        existingConnection.close();
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private class ReconnectingXAResource implements XAResource {
        protected TransactionContext delegate;

        ReconnectingXAResource(TransactionContext delegate) {
            this.delegate = delegate;
        }

        @Override
        public void commit(Xid xid, boolean b) throws XAException {
            ensureConnection(delegate);
            delegate.commit(xid, b);
        }

        @Override
        public void end(Xid xid, int i) throws XAException {
            ensureConnection(delegate);
            delegate.end(xid, i);
        }

        @Override
        public void forget(Xid xid) throws XAException {
            ensureConnection(delegate);
            delegate.forget(xid);
        }

        @Override
        public int getTransactionTimeout() throws XAException {
            ensureConnection(delegate);
            return delegate.getTransactionTimeout();
        }

        @Override
        public boolean isSameRM(XAResource xaResource) throws XAException {
            if (this == xaResource) {
                return true;
            }
            if (!(xaResource instanceof ReconnectingXAResource)) {
                return false;
            }

            ensureConnection(delegate);
            return delegate.isSameRM(((ReconnectingXAResource)xaResource).delegate);
        }

        @Override
        public int prepare(Xid xid) throws XAException {
            ensureConnection(delegate);
            return delegate.prepare(xid);
        }

        @Override
        public Xid[] recover(int i) throws XAException {
            ensureConnection(delegate);
            return delegate.recover(i);
        }

        @Override
        public void rollback(Xid xid) throws XAException {
            ensureConnection(delegate);
            delegate.rollback(xid);

        }

        @Override
        public boolean setTransactionTimeout(int i) throws XAException {
            ensureConnection(delegate);
            return delegate.setTransactionTimeout(i);
        }

        @Override
        public void start(Xid xid, int i) throws XAException {
            ensureConnection(delegate);
            delegate.start(xid, i);
        }

        public void stop() {
            try {
                delegate.getConnection().close();
            } catch (Throwable ignored) {}
        }
    };

    // ///////////////////////////////////////////////////////////////////////
    //
    // Java Bean getters and setters for this ResourceAdapter class.
    //
    // ///////////////////////////////////////////////////////////////////////

    /**
     * @see org.apache.activemq.ra.MessageResourceAdapter#getBrokerXmlConfig()
     */
    @Override
    public String getBrokerXmlConfig() {
        return brokerXmlConfig;
    }

    /**
     * Sets the <a href="https://activemq.apache.org/xml-configuration">XML
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
