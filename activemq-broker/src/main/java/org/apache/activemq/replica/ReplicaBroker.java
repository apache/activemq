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
package org.apache.activemq.replica;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public class ReplicaBroker extends BrokerFilter implements MutativeRoleBroker {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBroker.class);
    private final ScheduledExecutorService brokerConnectionPoller = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService periodicAckPoller = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean isConnecting = new AtomicBoolean();
    private final AtomicReference<ActiveMQConnection> connection = new AtomicReference<>();
    private final AtomicReference<ActiveMQSession> connectionSession = new AtomicReference<>();
    private final AtomicReference<ActiveMQMessageConsumer> eventConsumer = new AtomicReference<>();
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final ReplicaPolicy replicaPolicy;
    private final PeriodAcknowledge periodAcknowledgeCallBack;
    private final WebConsoleAccessController webConsoleAccessController;
    private ActionListenerCallback actionListenerCallback;
    private ReplicaBrokerEventListener messageListener;
    private ScheduledFuture<?> replicationScheduledFuture;
    private ScheduledFuture<?> ackPollerScheduledFuture;

    public ReplicaBroker(Broker next, ReplicaReplicationQueueSupplier queueProvider, ReplicaPolicy replicaPolicy,
            WebConsoleAccessController webConsoleAccessController) {
        super(next);
        this.queueProvider = queueProvider;
        this.replicaPolicy = replicaPolicy;
        this.periodAcknowledgeCallBack = new PeriodAcknowledge(replicaPolicy);
        this.webConsoleAccessController = webConsoleAccessController;
    }

    @Override
    public void start() throws Exception {
        super.start();
        init();

        webConsoleAccessController.stop();
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping Source broker");
        stopAllConnections();
        super.stop();
    }

    @Override
    public void initializeRoleChangeCallBack(ActionListenerCallback actionListenerCallback) {
        this.actionListenerCallback = actionListenerCallback;
    }

    @Override
    public void stopBeforeRoleChange(boolean force) throws Exception {
        logger.info("Stopping broker replication. Forced: [{}]", force);
        messageListener.deinitialize();
        removeReplicationQueues();
        stopAllConnections();
    }

    @Override
    public void startAfterRoleChange() throws Exception {
        logger.info("Starting Replica broker");
        init();
    }

    private void init() {
        logger.info("Initializing Replica broker");
        getBrokerService().stopAllConnectors(new ServiceStopper());
        queueProvider.initializeSequenceQueue();
        replicationScheduledFuture = brokerConnectionPoller.scheduleAtFixedRate(this::beginReplicationIdempotent, 5, 5, TimeUnit.SECONDS);
        ackPollerScheduledFuture = periodicAckPoller.scheduleAtFixedRate(() -> {
            synchronized (periodAcknowledgeCallBack) {
                try {
                    periodAcknowledgeCallBack.acknowledge();
                } catch (Exception e) {
                    logger.error("Failed to Acknowledge replication Queue message {}", e.getMessage());
                }
            }
        }, replicaPolicy.getReplicaAckPeriod(), replicaPolicy.getReplicaAckPeriod(), TimeUnit.MILLISECONDS);
        messageListener = new ReplicaBrokerEventListener(getNext(), queueProvider, periodAcknowledgeCallBack, actionListenerCallback);
    }

    private void stopAllConnections() throws JMSException {
        replicationScheduledFuture.cancel(true);
        ackPollerScheduledFuture.cancel(true);

        ActiveMQMessageConsumer consumer = eventConsumer.get();
        ActiveMQSession session = connectionSession.get();
        ActiveMQConnection brokerConnection = connection.get();
        if (messageListener != null) {
            messageListener.close();
        }
        if (consumer != null) {
            consumer.stop();
            consumer.close();
        }
        if (session != null) {
            session.close();
        }
        if (brokerConnection != null && brokerConnection.isStarted()) {
            brokerConnection.stop();
            brokerConnection.close();
        }

        getBrokerService().stopAllConnectors(new ServiceStopper());

        eventConsumer.set(null);
        connectionSession.set(null);
        connection.set(null);
        replicationScheduledFuture = null;
        ackPollerScheduledFuture = null;
    }

    private void removeReplicationQueues() {
        ReplicaSupport.REPLICATION_QUEUE_NAMES.forEach(queueName -> {
            try {
                getBrokerService().removeDestination(new ActiveMQQueue(queueName));
            } catch (Exception e) {
                logger.error("Failed to delete replication queue [{}]", queueName, e);
            }
        });
    }

    private void beginReplicationIdempotent() {
        if (connectionSession.get() == null) {
            logger.debug("Establishing inter-broker replication connection");
            establishConnectionSession();
        }
        if (eventConsumer.get() == null) {
            try {
                logger.debug("Creating replica event consumer");
                consumeReplicationEvents();
            } catch (Exception e) {
                logger.error("Could not establish replication consumer", e);
            }
        }
    }

    private void establishConnectionSession() {
        if (isConnecting.compareAndSet(false, true)) {
            logger.debug("Trying to connect to replica source");
            try {
                establishConnection();
                ActiveMQSession session = (ActiveMQSession) connection.get().createSession(false, ActiveMQSession.CLIENT_ACKNOWLEDGE);
                session.setAsyncDispatch(false); // force the primary broker to block if we are slow
                connectionSession.set(session);
                periodAcknowledgeCallBack.setConnectionSession(session);
            } catch (RuntimeException | JMSException e) {
                logger.warn("Failed to establish connection to replica", e);
            } finally {
                if (connectionSession.get() == null) {
                    logger.info("Closing connection session after unsuccessful connection establishment");
                    connection.getAndUpdate(conn -> {
                        try {
                            if (conn != null) {
                                conn.close();
                            }
                        } catch (JMSException e) {
                            logger.error("Failed to close connection after session establishment failed", e);
                        }
                        return null;
                    });
                }
                isConnecting.weakCompareAndSetPlain(true, false);
            }
        }
    }

    private void establishConnection() throws JMSException {
        ActiveMQConnectionFactory replicaSourceConnectionFactory = replicaPolicy.getOtherBrokerConnectionFactory();
        logger.trace("Replica connection URL {}", replicaSourceConnectionFactory.getBrokerURL());
        ActiveMQConnection newConnection = (ActiveMQConnection) replicaSourceConnectionFactory.createConnection();
        newConnection.start();
        connection.set(newConnection);
        periodAcknowledgeCallBack.setConnection(newConnection);
        logger.debug("Established connection to replica source: {}", replicaSourceConnectionFactory.getBrokerURL());
    }

    private void consumeReplicationEvents() throws Exception {
        if (connectionUnusable() || sessionUnusable()) {
            return;
        }
        ActiveMQQueue replicationSourceQueue = connection.get()
                .getDestinationSource()
                .getQueues()
                .stream()
                .filter(d -> ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME.equals(d.getPhysicalName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        MessageFormat.format("There is no replication queue on the source broker {0}", replicaPolicy.getOtherBrokerConnectionFactory().getBrokerURL())
                ));
        logger.info("Plugin will mirror events from queue {}", replicationSourceQueue.getPhysicalName());
        messageListener.initialize();
        ActiveMQPrefetchPolicy prefetchPolicy = connection.get().getPrefetchPolicy();
        Method getNextConsumerId = ActiveMQSession.class.getDeclaredMethod("getNextConsumerId");
        getNextConsumerId.setAccessible(true);
        eventConsumer.set(new ActiveMQMessageConsumer(connectionSession.get(), (ConsumerId) getNextConsumerId.invoke(connectionSession.get()), replicationSourceQueue, null, null, prefetchPolicy.getQueuePrefetch(),
                prefetchPolicy.getMaximumPendingMessageLimit(), false, false, connectionSession.get().isAsyncDispatch(), messageListener) {
            @Override
            public void dispatch(MessageDispatch md) {
                synchronized (periodAcknowledgeCallBack) {
                    super.dispatch(md);
                    try {
                        periodAcknowledgeCallBack.acknowledge();
                    } catch (Exception e) {
                        logger.error("Failed to acknowledge replication message [{}]", e);
                    }
                }
            }
        });
    }


    private boolean connectionUnusable() {
        if (isConnecting.get()) {
            logger.trace("Will not consume events because we are still connecting");
            return true;
        }
        ActiveMQConnection conn = connection.get();
        if (conn == null) {
            logger.trace("Will not consume events because we don't have a connection");
            return true;
        }
        if (conn.isClosed() || conn.isClosing()) {
            logger.trace("Will not consume events because the connection is not open");
            return true;
        }
        return false;
    }

    private boolean sessionUnusable() {
        ActiveMQSession session = connectionSession.get();
        if (session == null) {
            logger.trace("Will not consume events because we don't have a session");
            return true;
        }
        if (session.isClosed()) {
            logger.trace("Will not consume events because the session is not open");
            return true;
        }
        return false;
    }
}
