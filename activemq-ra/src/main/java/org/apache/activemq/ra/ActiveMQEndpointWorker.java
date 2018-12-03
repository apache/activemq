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

import java.lang.reflect.Method;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkManager;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionConsumer;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  $Date$
 */
public class ActiveMQEndpointWorker {

    public static final Method ON_MESSAGE_METHOD;
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQEndpointWorker.class);

    private static final long INITIAL_RECONNECT_DELAY = 1000; // 1 second.
    private static final long MAX_RECONNECT_DELAY = 1000 * 30; // 30 seconds.
    private static final ThreadLocal<Session> THREAD_LOCAL = new ThreadLocal<Session>();

    static {
        try {
            ON_MESSAGE_METHOD = MessageListener.class.getMethod("onMessage", new Class[] {
                Message.class
            });
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected final ActiveMQEndpointActivationKey endpointActivationKey;
    protected final MessageEndpointFactory endpointFactory;
    protected final WorkManager workManager;
    protected final boolean transacted;

    private final ActiveMQDestination dest;
    private final Work connectWork;
    private final AtomicBoolean connecting = new AtomicBoolean(false);    
    private final Object shutdownMutex = new String("shutdownMutex");
    
    private ActiveMQConnection connection;
    private ActiveMQConnectionConsumer consumer;
    private ServerSessionPoolImpl serverSessionPool;
    private boolean running;

    protected ActiveMQEndpointWorker(final MessageResourceAdapter adapter, ActiveMQEndpointActivationKey key) throws ResourceException {
        this.endpointActivationKey = key;
        this.endpointFactory = endpointActivationKey.getMessageEndpointFactory();
        this.workManager = adapter.getBootstrapContext().getWorkManager();
        try {
            this.transacted = endpointFactory.isDeliveryTransacted(ON_MESSAGE_METHOD);
        } catch (NoSuchMethodException e) {
            throw new ResourceException("Endpoint does not implement the onMessage method.");
        }

        connectWork = new Work() {
            long currentReconnectDelay = INITIAL_RECONNECT_DELAY;

            public void release() {
                //
            }

            public void run() {
                currentReconnectDelay = INITIAL_RECONNECT_DELAY;
                MessageActivationSpec activationSpec = endpointActivationKey.getActivationSpec();
                if (LOG.isInfoEnabled()) {
                    LOG.info("Establishing connection to broker [" + adapter.getInfo().getServerUrl() + "]");
                }

                while (connecting.get() && running) {
                    try {
                        connection = adapter.makeConnection(activationSpec);
                        connection.setExceptionListener(new ExceptionListener() {
                            public void onException(JMSException error) {
                                if (!serverSessionPool.isClosing()) {
                                    // initiate reconnection only once, i.e. on initial exception
                                    // and only if not already trying to connect
                                    LOG.error("Connection to broker failed: " + error.getMessage(), error);
                                    if (connecting.compareAndSet(false, true)) {
                                        synchronized (connectWork) {
                                            disconnect();
                                            serverSessionPool.closeSessions();
                                            connect();
                                        }
                                    } else {
                                        // connection attempt has already been initiated
                                        LOG.info("Connection attempt already in progress, ignoring connection exception");
                                    }
                                }
                            }
                        });
                        connection.start();

                        if (activationSpec.isDurableSubscription()) {
                            consumer = (ActiveMQConnectionConsumer) connection.createDurableConnectionConsumer(
                                    (Topic) dest,
                                    activationSpec.getSubscriptionName(),
                                    emptyToNull(activationSpec.getMessageSelector()),
                                    serverSessionPool,
                                    connection.getPrefetchPolicy().getDurableTopicPrefetch(),
                                    activationSpec.getNoLocalBooleanValue());
                        } else {
                            consumer = (ActiveMQConnectionConsumer) connection.createConnectionConsumer(
                                    dest,
                                    emptyToNull(activationSpec.getMessageSelector()),
                                    serverSessionPool,
                                    getPrefetch(activationSpec, connection, dest),
                                    activationSpec.getNoLocalBooleanValue());
                        }


                        if (connecting.compareAndSet(true, false)) {
                            if (LOG.isInfoEnabled()) {
                                LOG.info("Successfully established connection to broker [" + adapter.getInfo().getServerUrl() + "]");
                            }
                        } else {
                            LOG.error("Could not release connection lock");
                        }

                        if (consumer.getConsumerInfo().getCurrentPrefetchSize() == 0) {
                            LOG.error("Endpoint " + endpointActivationKey.getActivationSpec() + " will not receive any messages due to broker 'zero prefetch' configuration for: " + dest);
                        }

                    } catch (JMSException error) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Failed to connect: " + error.getMessage(), error);
                        }
                        disconnect();
                        pause(error);
                    }
                }
            }

            private int getPrefetch(MessageActivationSpec activationSpec, ActiveMQConnection connection, ActiveMQDestination destination) {
                if (destination.isTopic()) {
                    return connection.getPrefetchPolicy().getTopicPrefetch();
                } else if (destination.isQueue()) {
                    return connection.getPrefetchPolicy().getQueuePrefetch();
                } else {
                    return activationSpec.getMaxMessagesPerSessionsIntValue() * activationSpec.getMaxSessionsIntValue();
                }
            }
            
            private void pause(JMSException error) {
                if (currentReconnectDelay == MAX_RECONNECT_DELAY) {
                    LOG.error("Failed to connect to broker [" + adapter.getInfo().getServerUrl() + "]: " 
                            + error.getMessage(), error);
                    LOG.error("Endpoint will try to reconnect to the JMS broker in " + (MAX_RECONNECT_DELAY / 1000) + " seconds");
                }
                try {
                    synchronized ( shutdownMutex ) {
                        // shutdownMutex will be notified by stop() method in
                        // order to accelerate shutdown of endpoint
                        shutdownMutex.wait(currentReconnectDelay);
                    }
                } catch ( InterruptedException e ) {
                    Thread.interrupted();
                }
                currentReconnectDelay *= 2;
                if (currentReconnectDelay > MAX_RECONNECT_DELAY) {
                    currentReconnectDelay = MAX_RECONNECT_DELAY;
                }                
            }
        };

        MessageActivationSpec activationSpec = endpointActivationKey.getActivationSpec();
        if (activationSpec.isUseJndi()) {
            try {
                InitialContext initialContext = new InitialContext();
                dest = (ActiveMQDestination) initialContext.lookup(activationSpec.getDestination());
            }
            catch (NamingException exc) {
                throw new ResourceException("JNDI lookup failed for "
                    + activationSpec.getDestination());
            }
        }
        else {
            if ("javax.jms.Queue".equals(activationSpec.getDestinationType())) {
                dest = new ActiveMQQueue(activationSpec.getDestination());
            }
            else if ("javax.jms.Topic".equals(activationSpec.getDestinationType())) {
                dest = new ActiveMQTopic(activationSpec.getDestination());
            }
            else {
                throw new ResourceException("Unknown destination type: "
                    + activationSpec.getDestinationType());
            }
        }
    }

    /**
     * @param c
     */
    public static void safeClose(Connection c) {
        try {
            if (c != null) {
                LOG.debug("Closing connection to broker");
                c.close();
            }
        } catch (JMSException e) {
            LOG.trace("failed to close c {}", c, e);
        }
    }

    /**
     * @param cc
     */
    public static void safeClose(ConnectionConsumer cc) {
        try {
            if (cc != null) {
                LOG.debug("Closing ConnectionConsumer");
                cc.close();
            }
        } catch (JMSException e) {
            LOG.trace("failed to close cc {}", cc, e);
        }
    }

    /**
     * 
     */
    public void start() throws ResourceException {
        synchronized (connectWork) {
            if (running)
            return;
        running = true;

            if ( connecting.compareAndSet(false, true) ) {
                LOG.info("Starting");
        serverSessionPool = new ServerSessionPoolImpl(this, endpointActivationKey.getActivationSpec().getMaxSessionsIntValue());
        connect();
            } else {
                LOG.warn("Ignoring start command, EndpointWorker is already trying to connect");
    }
        }
    }

    /**
     * 
     */
    public void stop() throws InterruptedException {
        synchronized (shutdownMutex) {
            if (!running)
                return;
            running = false;
            LOG.info("Stopping");
            // wake up pausing reconnect attempt
            shutdownMutex.notifyAll();
            try {
                serverSessionPool.close();
            } catch (Throwable ignored) {
                LOG.debug("Unexpected error on server session pool close", ignored);
            }
        }
        disconnect();
    }

    private boolean isRunning() {
        return running;
    }

    private void connect() {
        synchronized ( connectWork ) {
            if (!running) {
                return;
            }

            try {
                workManager.scheduleWork(connectWork, WorkManager.INDEFINITE, null, null);
            } catch (WorkException e) {
                running = false;
                LOG.error("Work Manager did not accept work: ", e);
            }
        }
    }

    /**
     * 
     */
    private void disconnect() {
        synchronized ( connectWork ) {
        safeClose(consumer);
        consumer = null;
        safeClose(connection);
        connection = null;
    }
            }

    protected void registerThreadSession(Session session) {
        THREAD_LOCAL.set(session);
    }

    protected void unregisterThreadSession(Session session) {
        THREAD_LOCAL.set(null);
    }

    // for testing
    public void setConnection(ActiveMQConnection activeMQConnection) {
        this.connection = activeMQConnection;
    }

    protected ActiveMQConnection getConnection() {
        // make sure we only return a working connection
        // in particular make sure that we do not return null
        // after the resource adapter got disconnected from
        // the broker via the disconnect() method
        synchronized ( connectWork ) {
            return connection;
        }
    }

    private String emptyToNull(String value) {
        if (value == null || value.length() == 0) {
            return null;
        }
        return value;
    }

}
