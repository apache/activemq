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
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public class ReplicaBroker extends MutativeRoleBroker {

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
    private final ReplicaStatistics replicaStatistics;
    private ReplicaBrokerEventListener messageListener;
    private ScheduledFuture<?> replicationScheduledFuture;
    private ScheduledFuture<?> ackPollerScheduledFuture;

    public ReplicaBroker(Broker broker, ReplicaRoleManagement management, ReplicaReplicationQueueSupplier queueProvider,
            ReplicaPolicy replicaPolicy, ReplicaStatistics replicaStatistics) {
        super(broker, management);
        this.queueProvider = queueProvider;
        this.replicaPolicy = replicaPolicy;
        this.periodAcknowledgeCallBack = new PeriodAcknowledge(replicaPolicy);
        this.replicaStatistics = replicaStatistics;
    }

    @Override
    public void start(ReplicaRole role) throws Exception {
        init(role);

        logger.info("Starting replica broker." +
                (role == ReplicaRole.ack_processed ? " Ack has been processed. Checking the role of the other broker." : ""));
    }

    @Override
    public void brokerServiceStarted(ReplicaRole role) {
        stopAllConnections();
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping broker replication.");
        deinitialize();
        super.stop();
    }

    @Override
    public void stopBeforeRoleChange(boolean force) throws Exception {
        if (!force) {
            return;
        }
        logger.info("Stopping broker replication. Forced: [{}]", force);

        updateBrokerState(ReplicaRole.source);
        completeBeforeRoleChange();
    }

    @Override
    public void startAfterRoleChange() throws Exception {
        logger.info("Starting Replica broker");
        init(ReplicaRole.replica);
    }

    void completeBeforeRoleChange() throws Exception {
        deinitialize();
        removeReplicationQueues();
        onStopSuccess();
    }

    private void init(ReplicaRole role) {
        logger.info("Initializing Replica broker");
        queueProvider.initializeSequenceQueue();
        replicationScheduledFuture = brokerConnectionPoller.scheduleAtFixedRate(() -> beginReplicationIdempotent(role), 5, 5, TimeUnit.SECONDS);
        ackPollerScheduledFuture = periodicAckPoller.scheduleAtFixedRate(() -> {
            synchronized (periodAcknowledgeCallBack) {
                try {
                    periodAcknowledgeCallBack.acknowledge();
                } catch (Exception e) {
                    logger.error("Failed to Acknowledge replication Queue message {}", e.getMessage());
                }
            }
        }, replicaPolicy.getReplicaAckPeriod(), replicaPolicy.getReplicaAckPeriod(), TimeUnit.MILLISECONDS);
        messageListener = new ReplicaBrokerEventListener(this, queueProvider, periodAcknowledgeCallBack, replicaPolicy, replicaStatistics);
    }

    private void deinitialize() throws Exception {
        if (replicationScheduledFuture != null) {
            replicationScheduledFuture.cancel(true);
        }
        if (ackPollerScheduledFuture != null) {
            ackPollerScheduledFuture.cancel(true);
        }

        ActiveMQMessageConsumer consumer = eventConsumer.get();
        ActiveMQSession session = connectionSession.get();
        ActiveMQConnection brokerConnection = connection.get();
        if (consumer != null) {
            consumer.setMessageListener(null);
        }
        if (messageListener != null) {
            messageListener.close();
        }
        if (consumer != null) {
            consumer.stop();
            consumer.close();
        }
        if (messageListener != null) {
            messageListener.deinitialize();
        }
        if (session != null) {
            session.close();
        }
        if (brokerConnection != null && brokerConnection.isStarted()) {
            brokerConnection.stop();
            brokerConnection.close();
        }

        eventConsumer.set(null);
        connectionSession.set(null);
        connection.set(null);
        replicationScheduledFuture = null;
        ackPollerScheduledFuture = null;
        messageListener = null;
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference messageReference, Subscription subscription, Throwable poisonCause) {
        // suppressing actions on the replica side. Expecting them to be replicated
        return false;
    }

    @Override
    public boolean isExpired(MessageReference messageReference) {
        // suppressing actions on the replica side. Expecting them to be replicated
        return false;
    }

    private void beginReplicationIdempotent(ReplicaRole initialRole) {
        if (connectionSession.get() == null) {
            logger.debug("Establishing inter-broker replication connection");
            establishConnectionSession();
        }
        if (eventConsumer.get() == null) {
            try {
                logger.debug("Creating replica event consumer");
                consumeReplicationEvents(initialRole);
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
        newConnection.setSendAcksAsync(false);
        newConnection.start();
        connection.set(newConnection);
        periodAcknowledgeCallBack.setConnection(newConnection);
        logger.debug("Established connection to replica source: {}", replicaSourceConnectionFactory.getBrokerURL());
    }

    private void consumeReplicationEvents(ReplicaRole initialRole) throws Exception {
        if (connectionUnusable() || sessionUnusable()) {
            return;
        }
        if (initialRole == ReplicaRole.ack_processed) {
            if (isReadyToFailover()) {
                updateBrokerState(ReplicaRole.source);
                completeBeforeRoleChange();
                return;
            }
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

    private boolean isReadyToFailover() throws JMSException {
        ActiveMQTopic replicationRoleAdvisoryTopic = connection.get()
                .getDestinationSource()
                .getTopics()
                .stream()
                .filter(d -> ReplicaSupport.REPLICATION_ROLE_ADVISORY_TOPIC_NAME.equals(d.getPhysicalName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        MessageFormat.format("There is no replication role advisory topic on the source broker {0}",
                                replicaPolicy.getOtherBrokerConnectionFactory().getBrokerURL())
                ));
        MessageConsumer advisoryConsumer = connectionSession.get().createConsumer(replicationRoleAdvisoryTopic);
        ActiveMQTextMessage message = (ActiveMQTextMessage) advisoryConsumer.receive(5000);
        if (message == null) {
            throw new IllegalStateException("There is no replication role in the role advisory topic on the source broker {0}" +
                    replicaPolicy.getOtherBrokerConnectionFactory().getBrokerURL());
        }
        advisoryConsumer.close();
        return ReplicaRole.valueOf(message.getText()) == ReplicaRole.replica;
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
