package org.apache.activemq.replica.storage;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.apache.activemq.replica.ReplicaRole;
import org.apache.activemq.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ReplicaFailOverStateStorage extends ReplicaBaseStorage {

    private final Logger logger = LoggerFactory.getLogger(ReplicaFailOverStateStorage.class);

    public ReplicaFailOverStateStorage(ReplicaReplicationQueueSupplier queueProvider) {
        super(queueProvider);
        this.replicationProducerId.setConnectionId(new IdGenerator().generateId());
    }

    public void initialize(Broker broker, ConnectionContext connectionContext, ReplicaInternalMessageProducer internalMessageProducer) throws Exception {
        this.broker = requireNonNull(broker);
        this.connectionContext = requireNonNull(connectionContext);
        this.replicaInternalMessageProducer = requireNonNull(internalMessageProducer);

        queueProvider.initializeFailOverQueue();

        initializeBase(queueProvider.getFailOverQueue(), "ReplicationPlugin.ReplicaFailOverStorage", null, connectionContext);
    }

    public ReplicaRole getBrokerState() throws JMSException {
        List<ActiveMQTextMessage> activeMQTextMessages = subscription.getDispatched().stream()
                .map(MessageReference::getMessage)
                .map(ActiveMQTextMessage.class::cast)
                .collect(Collectors.toList());

        List<ReplicaRole> replicaRoles = new ArrayList<>();

        for(ActiveMQTextMessage activeMQTextMessage: activeMQTextMessages) {
            replicaRoles.add(ReplicaRole.valueOf(activeMQTextMessage.getText()));
        }

        return replicaRoles.stream().reduce((first, second) -> second)
                .orElse(null);
    }

    public void updateBrokerState(ConnectionContext connectionContext, TransactionId tid, String message) throws Exception {
        acknowledgeAll(connectionContext, tid);

        ActiveMQTextMessage activeMQTextMessage = new ActiveMQTextMessage();
        activeMQTextMessage.setText(message);
        activeMQTextMessage.setTransactionId(tid);
        activeMQTextMessage.setDestination(queueProvider.getFailOverQueue());
        activeMQTextMessage.setMessageId(new MessageId(replicationProducerId, messageIdGenerator.getNextSequenceId()));
        activeMQTextMessage.setProducerId(replicationProducerId);
        activeMQTextMessage.setPersistent(true);
        activeMQTextMessage.setResponseRequired(false);

        replicaInternalMessageProducer.sendIgnoringFlowControl(connectionContext, activeMQTextMessage);
    }

    public ConnectionContext getContext() {
        return connectionContext;
    }
}
