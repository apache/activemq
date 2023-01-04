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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class ReplicaBroker extends BrokerFilter {

    private final static long REPLICA_ACK_PERIOD = 5_000;

    private final Logger logger = LoggerFactory.getLogger(ReplicaBroker.class);
    private final ScheduledExecutorService brokerConnectionPoller = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService periodicAckPoller = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean isConnecting = new AtomicBoolean();
    private final AtomicReference<ActiveMQConnection> connection = new AtomicReference<>();
    private final AtomicReference<ActiveMQSession> connectionSession = new AtomicReference<>();
    private final AtomicReference<ActiveMQMessageConsumer> eventConsumer = new AtomicReference<>();
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final ActiveMQConnectionFactory replicaSourceConnectionFactory;
    private final PeriodAcknowledge periodAcknowledgeCallBack;

    public ReplicaBroker(Broker next, ReplicaReplicationQueueSupplier queueProvider, ActiveMQConnectionFactory replicaSourceConnectionFactory) {
        super(next);
        this.queueProvider = queueProvider;
        this.periodAcknowledgeCallBack = new PeriodAcknowledge(REPLICA_ACK_PERIOD);
        this.replicaSourceConnectionFactory = requireNonNull(replicaSourceConnectionFactory, "Need connection details of replica source for this broker");
        requireNonNull(replicaSourceConnectionFactory.getBrokerURL(), "Need connection URI of replica source for this broker");
        validateUser(replicaSourceConnectionFactory);
    }

    private void validateUser(ActiveMQConnectionFactory replicaSourceConnectionFactory) {
        if (replicaSourceConnectionFactory.getUserName() != null) {
            requireNonNull(replicaSourceConnectionFactory.getPassword(), "Both userName and password or none of them should be configured for replica broker");
        }
        if (replicaSourceConnectionFactory.getPassword() != null) {
            requireNonNull(replicaSourceConnectionFactory.getUserName(), "Both userName and password or none of them should be configured for replica broker");
        }
    }

    @Override
    public void start() throws Exception {
        super.start();
        queueProvider.initializeSequenceQueue();
        brokerConnectionPoller.scheduleAtFixedRate(this::beginReplicationIdempotent, 5, 5, TimeUnit.SECONDS);
        periodicAckPoller.scheduleAtFixedRate(() -> {
            synchronized (periodAcknowledgeCallBack) {
                try {
                    periodAcknowledgeCallBack.acknowledge();
                } catch (Exception e) {
                    logger.error("Failed to Acknowledge replication Queue message {}", e.getMessage());
                }
            }
        }, REPLICA_ACK_PERIOD, REPLICA_ACK_PERIOD, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() throws Exception {
        ActiveMQMessageConsumer consumer = eventConsumer.get();
        ActiveMQSession session = connectionSession.get();
        ActiveMQConnection brokerConnection = connection.get();
        if (consumer != null) {
            consumer.stop();
            consumer.close();
        }
        if (session != null) {
            session.close();
        }
        if (brokerConnection != null) {
            brokerConnection.close();
        }
        super.stop();
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
                        MessageFormat.format("There is no replication queue on the source broker {0}", replicaSourceConnectionFactory.getBrokerURL())
                ));
        logger.info("Plugin will mirror events from queue {}", replicationSourceQueue.getPhysicalName());
        ReplicaBrokerEventListener messageListener = new ReplicaBrokerEventListener(getNext(), queueProvider, periodAcknowledgeCallBack);
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
