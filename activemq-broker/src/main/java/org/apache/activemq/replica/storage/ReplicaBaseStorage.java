package org.apache.activemq.replica.storage;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.DestinationExtractor;
import org.apache.activemq.replica.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.apache.activemq.replica.ReplicaSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;

import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class ReplicaBaseStorage {

    protected final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();
    protected final ProducerId replicationProducerId = new ProducerId();

    protected Broker broker;
    protected ConnectionContext connectionContext;
    protected ReplicaInternalMessageProducer replicaInternalMessageProducer;
    protected Queue queue;
    protected PrefetchSubscription subscription;
    protected ReplicaReplicationQueueSupplier queueProvider;
    private ActiveMQQueue activeMQQueue;

    public ReplicaBaseStorage(ReplicaReplicationQueueSupplier queueProvider) {
        this.queueProvider = requireNonNull(queueProvider);
    }

    public ReplicaBaseStorage(Broker broker, ReplicaReplicationQueueSupplier queueProvider, ReplicaInternalMessageProducer replicaInternalMessageProducer) {
        this(queueProvider);
        this.broker = requireNonNull(broker);
        this.replicaInternalMessageProducer = requireNonNull(replicaInternalMessageProducer);

        replicationProducerId.setConnectionId(new IdGenerator().generateId());
    }

    protected void initializeBase(ActiveMQQueue activeMQQueue, String idGeneratorPrefix, String selector, ConnectionContext connectionContext) throws Exception {
        queue = broker.getDestinations(activeMQQueue).stream().findFirst()
                .map(DestinationExtractor::extractQueue).orElseThrow();
        this.activeMQQueue = activeMQQueue;

        ConnectionId connectionId = new ConnectionId(new IdGenerator(idGeneratorPrefix).generateId());
        SessionId sessionId = new SessionId(connectionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerId consumerId = new ConsumerId(sessionId, new LongSequenceGenerator().getNextSequenceId());
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setPrefetchSize(ReplicaSupport.INTERMEDIATE_QUEUE_PREFETCH_SIZE);
        consumerInfo.setDestination(activeMQQueue);
        if (selector != null) {
            consumerInfo.setSelector(selector);
        }
        subscription = (PrefetchSubscription) broker.addConsumer(connectionContext, consumerInfo);
        queue.iterate();
    }

    protected void acknowledgeAll(ConnectionContext connectionContext, TransactionId tid) throws Exception {
        List<MessageReference> dispatched = subscription.getDispatched();

        if (!dispatched.isEmpty()) {
            MessageAck ack = new MessageAck(dispatched.get(dispatched.size() - 1).getMessage(), MessageAck.STANDARD_ACK_TYPE, dispatched.size());
            ack.setFirstMessageId(dispatched.get(0).getMessageId());
            ack.setDestination(activeMQQueue);
            ack.setTransactionId(tid);
            acknowledge(connectionContext, ack);
        }
    }

    protected void acknowledge(ConnectionContext connectionContext, MessageAck ack) throws Exception {
        ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
        consumerExchange.setConnectionContext(connectionContext);
        consumerExchange.setSubscription(subscription);

        broker.acknowledge(consumerExchange, ack);
    }


}
