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

import org.apache.activemq.Service;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.MutableBrokerFilter;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.storage.ReplicaRoleStorage;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicaRoleManagementBroker extends MutableBrokerFilter implements ReplicaRoleManagement {
    private static final String FAIL_OVER_CONSUMER_CLIENT_ID = "DUMMY_FAIL_OVER_CONSUMER";

    private final Logger logger = LoggerFactory.getLogger(ReplicaRoleManagementBroker.class);
    private final Broker broker;
    private final ReplicaPolicy replicaPolicy;
    private ReplicaRole role;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final WebConsoleAccessController webConsoleAccessController;
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer;

    protected final ProducerId replicationProducerId = new ProducerId();
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();

    ReplicaSourceBroker sourceBroker;
    ReplicaBroker replicaBroker;
    private ReplicaRoleStorage replicaRoleStorage;

    public ReplicaRoleManagementBroker(Broker broker, ReplicaPolicy replicaPolicy, ReplicaRole role) {
        super(broker);
        this.broker = broker;
        this.replicaPolicy = replicaPolicy;
        this.role = role;

        replicationProducerId.setConnectionId(new IdGenerator().generateId());

        queueProvider = new ReplicaReplicationQueueSupplier(broker);
        webConsoleAccessController = new WebConsoleAccessController(broker.getBrokerService(),
                replicaPolicy.isControlWebConsoleAccess());

        replicaInternalMessageProducer = new ReplicaInternalMessageProducer(broker);
        ReplicationMessageProducer replicationMessageProducer =
                new ReplicationMessageProducer(replicaInternalMessageProducer, queueProvider);
        ReplicaSequencer replicaSequencer = new ReplicaSequencer(broker, queueProvider, replicaInternalMessageProducer,
                replicationMessageProducer, replicaPolicy);

        sourceBroker = buildSourceBroker(replicationMessageProducer, replicaSequencer, queueProvider);
        replicaBroker = buildReplicaBroker(queueProvider);

        addInterceptor4CompositeQueues();
    }

    @Override
    public void start() throws Exception {
        initializeTransportConnector();
        super.start();
        initializeRoleStorage();

        MutativeRoleBroker nextByRole = getNextByRole();
        nextByRole.start(role);
        setNext(nextByRole);
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        Subscription answer = super.addConsumer(context, info);

        if (ReplicaSupport.isReplicationRoleAdvisoryTopic(info.getDestination())) {
            sendAdvisory(role);
        }

        return answer;
    }

    public ReplicaRole getRole() {
        return role;
    }

    @Override
    public void brokerServiceStarted() {
        super.brokerServiceStarted();
        getNextByRole().brokerServiceStarted(role);
    }

    public void switchRole(ReplicaRole role, boolean force) throws Exception {
        if (role != ReplicaRole.source && role != ReplicaRole.replica) {
            return;
        }
        if (this.role.getExternalRole() == role) {
            return;
        }
        getNextByRole().stopBeforeRoleChange(force);
    }

    public void onStopSuccess() throws Exception {
        MutativeRoleBroker nextByRole = getNextByRole();
        nextByRole.startAfterRoleChange();
        setNext(nextByRole);
    }

    public Broker getBroker() {
        return broker;
    }

    public void updateBrokerState(ConnectionContext connectionContext, TransactionId tid, ReplicaRole role) throws Exception {
        replicaRoleStorage.enqueue(connectionContext, tid, role.name());
        this.role = role;
    }

    public void stopAllConnections() {
        getBrokerService().stopAllConnectors(new ServiceStopper() {
            @Override
            public void stop(Service service) {
                if (service instanceof TransportConnector &&
                        ((TransportConnector) service).getName().equals(ReplicaSupport.REPLICATION_CONNECTOR_NAME)) {
                    return;
                }
                super.stop(service);
            }
        });
        webConsoleAccessController.stop();
    }

    public void startAllConnections() throws Exception {
        getBrokerService().startAllConnectors();
        webConsoleAccessController.start();
    }

    private void initializeRoleStorage() throws Exception {
        ConnectionContext connectionContext = createConnectionContext();
        connectionContext.setClientId(FAIL_OVER_CONSUMER_CLIENT_ID);
        connectionContext.setConnection(new DummyConnection());
        queueProvider.initializeRoleQueueAndTopic();
        replicaRoleStorage = new ReplicaRoleStorage(broker, queueProvider, replicaInternalMessageProducer);
        ReplicaRole savedRole = replicaRoleStorage.initialize(connectionContext);
        if (savedRole != null) {
            role = savedRole;
        }
    }

    private ReplicaSourceBroker buildSourceBroker(ReplicationMessageProducer replicationMessageProducer,
            ReplicaSequencer replicaSequencer, ReplicaReplicationQueueSupplier queueProvider) {
        return new ReplicaSourceBroker(broker, this, replicationMessageProducer, replicaSequencer,
                queueProvider, replicaPolicy);
    }

    private ReplicaBroker buildReplicaBroker(ReplicaReplicationQueueSupplier queueProvider) {
        return new ReplicaBroker(broker, this, queueProvider, replicaPolicy);
    }

    private void addInterceptor4CompositeQueues() {
        final RegionBroker regionBroker = (RegionBroker) broker.getAdaptor(RegionBroker.class);
        final CompositeDestinationInterceptor compositeInterceptor = (CompositeDestinationInterceptor) regionBroker.getDestinationInterceptor();
        DestinationInterceptor[] interceptors = compositeInterceptor.getInterceptors();
        interceptors = Arrays.copyOf(interceptors, interceptors.length + 1);
        interceptors[interceptors.length - 1] = new ReplicaDestinationInterceptor(sourceBroker, this);
        compositeInterceptor.setInterceptors(interceptors);
    }

    private MutativeRoleBroker getNextByRole() {
        switch (role) {
            case source:
            case await_ack:
                return sourceBroker;
            case replica:
            case ack_processed:
                return replicaBroker;
            default:
                throw new IllegalStateException("Unknown replication role: " + role);
        }
    }

    private void initializeTransportConnector() throws Exception {
        logger.info("Initializing Replication Transport Connector");
        TransportConnector transportConnector = getBrokerService().addConnector(replicaPolicy.getTransportConnectorUri());
        transportConnector.setUri(replicaPolicy.getTransportConnectorUri());
        transportConnector.setName(ReplicaSupport.REPLICATION_CONNECTOR_NAME);
    }

    private void sendAdvisory(ReplicaRole role) throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(role.name());
        message.setTransactionId(null);
        message.setDestination(queueProvider.getRoleAdvisoryTopic());
        message.setMessageId(new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
        message.setProducerId(replicationProducerId);
        message.setPersistent(false);
        message.setResponseRequired(false);

        replicaInternalMessageProducer.sendIgnoringFlowControl(createConnectionContext(), message);
    }

    private ConnectionContext createConnectionContext() {
        ConnectionContext connectionContext = getAdminConnectionContext().copy();
        if (connectionContext.getTransactions() == null) {
            connectionContext.setTransactions(new ConcurrentHashMap<>());
        }

        return connectionContext;
    }
}