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
package org.apache.activemq.network.jms;

import static org.apache.activemq.network.jms.ReconnectionPolicy.INFINITE;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.Destination;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This bridge joins the gap between foreign JMS providers and ActiveMQ As some
 * JMS providers are still only in compliance with JMS v1.0.1 , this bridge itself
 * aimed to be in compliance with the JMS 1.0.2 specification.
 */
public abstract class JmsConnector implements Service {

    private static int nextId;
    private static final Logger LOG = LoggerFactory.getLogger(JmsConnector.class);

    protected boolean preferJndiDestinationLookup = false;
    protected JndiLookupFactory jndiLocalTemplate;
    protected JndiLookupFactory jndiOutboundTemplate;
    protected JmsMesageConvertor inboundMessageConvertor;
    protected JmsMesageConvertor outboundMessageConvertor;
    protected AtomicBoolean initialized = new AtomicBoolean(false);
    protected AtomicBoolean localSideInitialized = new AtomicBoolean(false);
    protected AtomicBoolean foreignSideInitialized = new AtomicBoolean(false);
    protected AtomicBoolean started = new AtomicBoolean(false);
    protected AtomicBoolean failed = new AtomicBoolean();
    protected AtomicReference<Connection> foreignConnection = new AtomicReference<Connection>();
    protected AtomicReference<Connection> localConnection = new AtomicReference<Connection>();
    protected ActiveMQConnectionFactory embeddedConnectionFactory;
    protected int replyToDestinationCacheSize = 10000;
    protected String outboundUsername;
    protected String outboundPassword;
    protected String localUsername;
    protected String localPassword;
    protected String outboundClientId;
    protected String localClientId;
    protected LRUCache<Destination, DestinationBridge> replyToBridges = createLRUCache();

    private ReconnectionPolicy policy = new ReconnectionPolicy();
    protected ThreadPoolExecutor connectionService;
    private final List<DestinationBridge> inboundBridges = new CopyOnWriteArrayList<DestinationBridge>();
    private final List<DestinationBridge> outboundBridges = new CopyOnWriteArrayList<DestinationBridge>();
    private String name;

    private static LRUCache<Destination, DestinationBridge> createLRUCache() {
        return new LRUCache<Destination, DestinationBridge>() {
            private static final long serialVersionUID = -7446792754185879286L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<Destination, DestinationBridge> enty) {
                if (size() > maxCacheSize) {
                    Iterator<Map.Entry<Destination, DestinationBridge>> iter = entrySet().iterator();
                    Map.Entry<Destination, DestinationBridge> lru = iter.next();
                    remove(lru.getKey());
                    DestinationBridge bridge = lru.getValue();
                    try {
                        bridge.stop();
                        LOG.info("Expired bridge: {}", bridge);
                    } catch (Exception e) {
                        LOG.warn("Stopping expired bridge {} caused an exception", bridge, e);
                    }
                }
                return false;
            }
        };
    }

    public boolean init() {
        boolean result = initialized.compareAndSet(false, true);
        if (result) {
            if (jndiLocalTemplate == null) {
                jndiLocalTemplate = new JndiLookupFactory();
            }
            if (jndiOutboundTemplate == null) {
                jndiOutboundTemplate = new JndiLookupFactory();
            }
            if (inboundMessageConvertor == null) {
                inboundMessageConvertor = new SimpleJmsMessageConvertor();
            }
            if (outboundMessageConvertor == null) {
                outboundMessageConvertor = new SimpleJmsMessageConvertor();
            }
            replyToBridges.setMaxCacheSize(getReplyToDestinationCacheSize());

            connectionService = createExecutor();

            // Subclasses can override this to customize their own it.
            result = doConnectorInit();
        }
        return result;
    }

    protected boolean doConnectorInit() {

        // We try to make a connection via a sync call first so that the
        // JmsConnector is fully initialized before the start call returns
        // in order to avoid missing any messages that are dispatched
        // immediately after startup.  If either side fails we queue an
        // asynchronous task to manage the reconnect attempts.

        try {
            initializeLocalConnection();
            localSideInitialized.set(true);
        } catch(Exception e) {
            // Queue up the task to attempt the local connection.
            scheduleAsyncLocalConnectionReconnect();
        }

        try {
            initializeForeignConnection();
            foreignSideInitialized.set(true);
        } catch(Exception e) {
            // Queue up the task for the foreign connection now.
            scheduleAsyncForeignConnectionReconnect();
        }

        return true;
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            init();
            for (DestinationBridge bridge : inboundBridges) {
                bridge.start();
            }
            for (DestinationBridge bridge : outboundBridges) {
                bridge.start();
            }
            LOG.info("JMS Connector {} started", getName());
        }
    }

    @Override
    public void stop() throws Exception {
        if (started.compareAndSet(true, false)) {

            ThreadPoolUtils.shutdown(connectionService);
            connectionService = null;

            if (foreignConnection.get() != null) {
                try {
                    foreignConnection.get().close();
                } catch (Exception e) {
                }
            }

            if (localConnection.get() != null) {
                try {
                    localConnection.get().close();
                } catch (Exception e) {
                }
            }

            for (DestinationBridge bridge : inboundBridges) {
                bridge.stop();
            }
            for (DestinationBridge bridge : outboundBridges) {
                bridge.stop();
            }
            LOG.info("JMS Connector {} stopped", getName());
        }
    }

    public void clearBridges() {
        inboundBridges.clear();
        outboundBridges.clear();
        replyToBridges.clear();
    }

    protected abstract Destination createReplyToBridge(Destination destination, Connection consumerConnection, Connection producerConnection);

    /**
     * One way to configure the local connection - this is called by The
     * BrokerService when the Connector is embedded
     *
     * @param service
     */
    public void setBrokerService(BrokerService service) {
        embeddedConnectionFactory = new ActiveMQConnectionFactory(service.getVmConnectorURI());
    }

    public Connection getLocalConnection() {
        return this.localConnection.get();
    }

    public Connection getForeignConnection() {
        return this.foreignConnection.get();
    }

    /**
     * @return Returns the jndiTemplate.
     */
    public JndiLookupFactory getJndiLocalTemplate() {
        return jndiLocalTemplate;
    }

    /**
     * @param jndiTemplate The jndiTemplate to set.
     */
    public void setJndiLocalTemplate(JndiLookupFactory jndiTemplate) {
        this.jndiLocalTemplate = jndiTemplate;
    }

    /**
     * @return Returns the jndiOutboundTemplate.
     */
    public JndiLookupFactory getJndiOutboundTemplate() {
        return jndiOutboundTemplate;
    }

    /**
     * @param jndiOutboundTemplate The jndiOutboundTemplate to set.
     */
    public void setJndiOutboundTemplate(JndiLookupFactory jndiOutboundTemplate) {
        this.jndiOutboundTemplate = jndiOutboundTemplate;
    }

    /**
     * @return Returns the inboundMessageConvertor.
     */
    public JmsMesageConvertor getInboundMessageConvertor() {
        return inboundMessageConvertor;
    }

    /**
     * @param jmsMessageConvertor The jmsMessageConvertor to set.
     */
    public void setInboundMessageConvertor(JmsMesageConvertor jmsMessageConvertor) {
        this.inboundMessageConvertor = jmsMessageConvertor;
    }

    /**
     * @return Returns the outboundMessageConvertor.
     */
    public JmsMesageConvertor getOutboundMessageConvertor() {
        return outboundMessageConvertor;
    }

    /**
     * @param outboundMessageConvertor The outboundMessageConvertor to set.
     */
    public void setOutboundMessageConvertor(JmsMesageConvertor outboundMessageConvertor) {
        this.outboundMessageConvertor = outboundMessageConvertor;
    }

    /**
     * @return Returns the replyToDestinationCacheSize.
     */
    public int getReplyToDestinationCacheSize() {
        return replyToDestinationCacheSize;
    }

    /**
     * @param replyToDestinationCacheSize The replyToDestinationCacheSize to set.
     */
    public void setReplyToDestinationCacheSize(int replyToDestinationCacheSize) {
        this.replyToDestinationCacheSize = replyToDestinationCacheSize;
    }

    /**
     * @return Returns the localPassword.
     */
    public String getLocalPassword() {
        return localPassword;
    }

    /**
     * @param localPassword The localPassword to set.
     */
    public void setLocalPassword(String localPassword) {
        this.localPassword = localPassword;
    }

    /**
     * @return Returns the localUsername.
     */
    public String getLocalUsername() {
        return localUsername;
    }

    /**
     * @param localUsername The localUsername to set.
     */
    public void setLocalUsername(String localUsername) {
        this.localUsername = localUsername;
    }

    /**
     * @return Returns the outboundPassword.
     */
    public String getOutboundPassword() {
        return outboundPassword;
    }

    /**
     * @param outboundPassword The outboundPassword to set.
     */
    public void setOutboundPassword(String outboundPassword) {
        this.outboundPassword = outboundPassword;
    }

    /**
     * @return Returns the outboundUsername.
     */
    public String getOutboundUsername() {
        return outboundUsername;
    }

    /**
     * @param outboundUsername The outboundUsername to set.
     */
    public void setOutboundUsername(String outboundUsername) {
        this.outboundUsername = outboundUsername;
    }

    /**
     * @return the outboundClientId
     */
    public String getOutboundClientId() {
        return outboundClientId;
    }

    /**
     * @param outboundClientId the outboundClientId to set
     */
    public void setOutboundClientId(String outboundClientId) {
        this.outboundClientId = outboundClientId;
    }

    /**
     * @return the localClientId
     */
    public String getLocalClientId() {
        return localClientId;
    }

    /**
     * @param localClientId the localClientId to set
     */
    public void setLocalClientId(String localClientId) {
        this.localClientId = localClientId;
    }

    /**
     * @return the currently configured reconnection policy.
     */
    public ReconnectionPolicy getReconnectionPolicy() {
        return this.policy;
    }

    /**
     * @param policy The new reconnection policy this {@link JmsConnector} should use.
     */
    public void setReconnectionPolicy(ReconnectionPolicy policy) {
        this.policy = policy;
    }

    /**
     * @return the preferJndiDestinationLookup
     */
    public boolean isPreferJndiDestinationLookup() {
        return preferJndiDestinationLookup;
    }

    /**
     * Sets whether the connector should prefer to first try to find a destination in JNDI before
     * using JMS semantics to create a Destination.  By default the connector will first use JMS
     * semantics and then fall-back to JNDI lookup, setting this value to true will reverse that
     * ordering.
     *
     * @param preferJndiDestinationLookup the preferJndiDestinationLookup to set
     */
    public void setPreferJndiDestinationLookup(boolean preferJndiDestinationLookup) {
        this.preferJndiDestinationLookup = preferJndiDestinationLookup;
    }

    /**
     * @return returns true if the {@link JmsConnector} is connected to both brokers.
     */
    public boolean isConnected() {
        return localConnection.get() != null && foreignConnection.get() != null;
    }

    protected void addInboundBridge(DestinationBridge bridge) {
        if (!inboundBridges.contains(bridge)) {
            inboundBridges.add(bridge);
        }
    }

    protected void addOutboundBridge(DestinationBridge bridge) {
        if (!outboundBridges.contains(bridge)) {
            outboundBridges.add(bridge);
        }
    }

    protected void removeInboundBridge(DestinationBridge bridge) {
        inboundBridges.remove(bridge);
    }

    protected void removeOutboundBridge(DestinationBridge bridge) {
        outboundBridges.remove(bridge);
    }

    public String getName() {
        if (name == null) {
            name = "Connector:" + getNextId();
        }
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private static synchronized int getNextId() {
        return nextId++;
    }

    public boolean isFailed() {
        return this.failed.get();
    }

    /**
     * Performs the work of connection to the local side of the Connection.
     * <p>
     * This creates the initial connection to the local end of the {@link JmsConnector}
     * and then sets up all the destination bridges with the information needed to bridge
     * on the local side of the connection.
     *
     * @throws Exception if the connection cannot be established for any reason.
     */
    protected abstract void initializeLocalConnection() throws Exception;

    /**
     * Performs the work of connection to the foreign side of the Connection.
     * <p>
     * This creates the initial connection to the foreign end of the {@link JmsConnector}
     * and then sets up all the destination bridges with the information needed to bridge
     * on the foreign side of the connection.
     *
     * @throws Exception if the connection cannot be established for any reason.
     */
    protected abstract void initializeForeignConnection() throws Exception;

    /**
     * Callback method that the Destination bridges can use to report an exception to occurs
     * during normal bridging operations.
     *
     * @param connection
     * 		The connection that was in use when the failure occured.
     */
    void handleConnectionFailure(Connection connection) {

        // Can happen if async exception listener kicks in at the same time.
        if (connection == null || !this.started.get()) {
            return;
        }

        LOG.info("JmsConnector handling loss of connection [{}]", connection.toString());

        // TODO - How do we handle the re-wiring of replyToBridges in this case.
        replyToBridges.clear();

        if (this.foreignConnection.compareAndSet(connection, null)) {

            // Stop the inbound bridges when the foreign connection is dropped since
            // the bridge has no consumer and needs to be restarted once a new connection
            // to the foreign side is made.
            for (DestinationBridge bridge : inboundBridges) {
                try {
                    bridge.stop();
                } catch(Exception e) {
                }
            }

            // We got here first and cleared the connection, now we queue a reconnect.
            this.connectionService.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        doInitializeConnection(false);
                    } catch (Exception e) {
                        LOG.error("Failed to initialize foreign connection for the JMSConnector", e);
                    }
                }
            });

        } else if (this.localConnection.compareAndSet(connection, null)) {

            // Stop the outbound bridges when the local connection is dropped since
            // the bridge has no consumer and needs to be restarted once a new connection
            // to the local side is made.
            for (DestinationBridge bridge : outboundBridges) {
                try {
                    bridge.stop();
                } catch(Exception e) {
                }
            }

            // We got here first and cleared the connection, now we queue a reconnect.
            this.connectionService.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        doInitializeConnection(true);
                    } catch (Exception e) {
                        LOG.error("Failed to initialize local connection for the JMSConnector", e);
                    }
                }
            });
        }
    }

    private void scheduleAsyncLocalConnectionReconnect() {
        this.connectionService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    doInitializeConnection(true);
                } catch (Exception e) {
                    LOG.error("Failed to initialize local connection for the JMSConnector", e);
                }
            }
        });
    }

    private void scheduleAsyncForeignConnectionReconnect() {
        this.connectionService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    doInitializeConnection(false);
                } catch (Exception e) {
                    LOG.error("Failed to initialize foreign connection for the JMSConnector", e);
                }
            }
        });
    }

    private void doInitializeConnection(boolean local) throws Exception {

        ThreadPoolExecutor connectionService = this.connectionService;
        int attempt = 0;

        final int maxRetries;
        if (local) {
            maxRetries = !localSideInitialized.get() ? policy.getMaxInitialConnectAttempts() :
                                                       policy.getMaxReconnectAttempts();
        } else {
            maxRetries = !foreignSideInitialized.get() ? policy.getMaxInitialConnectAttempts() :
                                                         policy.getMaxReconnectAttempts();
        }

        do {
            if (attempt > 0) {
                try {
                    long nextDelay = policy.getNextDelay(attempt);
                    LOG.debug("Bridge reconnect attempt {} waiting {}ms before next attempt.", attempt, nextDelay);
                    Thread.sleep(nextDelay);
                } catch(InterruptedException e) {
                }
            }

            if (connectionService.isTerminating()) {
                return;
            }

            try {

                if (local) {
                    initializeLocalConnection();
                    localSideInitialized.set(true);
                } else {
                    initializeForeignConnection();
                    foreignSideInitialized.set(true);
                }

                // Once we are connected we ensure all the bridges are started.
                if (localConnection.get() != null && foreignConnection.get() != null) {
                    for (DestinationBridge bridge : inboundBridges) {
                        bridge.start();
                    }
                    for (DestinationBridge bridge : outboundBridges) {
                        bridge.start();
                    }
                }

                return;
            } catch(Exception e) {
                LOG.debug("Failed to establish initial {} connection for JmsConnector [{}]", new Object[]{ (local ? "local" : "foreign"), attempt }, e);
            } finally {
                attempt++;
            }
        }
        while ((maxRetries == INFINITE || maxRetries > attempt) && !connectionService.isShutdown());

        this.failed.set(true);
    }

    private final ThreadFactory factory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "JmsConnector Async Connection Task: ");
            thread.setDaemon(true);
            return thread;
        }
    };

    private ThreadPoolExecutor createExecutor() {
        ThreadPoolExecutor exec = new ThreadPoolExecutor(0, 2, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), factory);
        exec.allowCoreThreadTimeOut(true);
        return exec;
    }
}
