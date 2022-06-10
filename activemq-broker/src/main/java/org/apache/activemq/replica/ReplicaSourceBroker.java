package org.apache.activemq.replica;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicaSourceBroker extends BrokerFilter {

    private final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);
    private static final DestinationMapEntry<Boolean> IS_REPLICATED = new DestinationMapEntry<>() {}; // used in destination map to indicate mirrored status
    static final String REPLICATION_CONNECTOR_NAME = "replication";

    final DestinationMap destinationsToReplicate = new DestinationMap();

    private final IdGenerator idGenerator = new IdGenerator();
    private final ProducerId replicationProducerId = new ProducerId();
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();

    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    final ReplicaReplicationQueueSupplier queueProvider;
    private final URI transportConnectorUri;
    private ReplicaInternalMessageProducer replicaInternalMessageProducer;

    private final Object sendingMutex = new Object();

    private final AtomicBoolean initialized = new AtomicBoolean();

    public ReplicaSourceBroker(Broker next, URI transportConnectorUri) {
        super(next);
        this.transportConnectorUri = Objects.requireNonNull(transportConnectorUri, "Need replication transport connection URI for this broker");
        replicationProducerId.setConnectionId(idGenerator.generateId());
        queueProvider = new ReplicaReplicationQueueSupplier(next);
    }

    @Override
    public void start() throws Exception {
        TransportConnector transportConnector = next.getBrokerService().addConnector(transportConnectorUri);
        transportConnector.setName(REPLICATION_CONNECTOR_NAME);

        queueProvider.initialize();
        logger.info("Replica plugin initialized with queue {}", queueProvider.get());
        initialized.compareAndSet(false, true);

        replicaInternalMessageProducer = new ReplicaInternalMessageProducer(next, getAdminConnectionContext());

        super.start();
    }

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemporary)
            throws Exception {
        Destination newDestination = super.addDestination(context, destination, createIfTemporary);
        if (shouldReplicateDestination(destination)) {
            replicateDestinationCreation(context, destination);
        }
        return newDestination;
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
        replicateDestinationRemoval(context, destination);
    }

    private boolean shouldReplicateDestination(ActiveMQDestination destination) {
        boolean isReplicationQueue = isReplicationQueue(destination);
        boolean isAdvisoryDestination = isAdvisoryDestination(destination);
        boolean isTemporaryDestination = destination.isTemporary();
        boolean shouldReplicate = !isReplicationQueue && !isAdvisoryDestination && !isTemporaryDestination;
        String reason = shouldReplicate ? "" : " because ";
        if (isReplicationQueue) reason += "it is a replication queue";
        if (isAdvisoryDestination) reason += "it is an advisory destination";
        if (isTemporaryDestination) reason += "it is a temporary destination";
        logger.debug("Will {}replicate destination {}{}", shouldReplicate ? "": "not ", destination, reason);
        return shouldReplicate;
    }

    private boolean isReplicationQueue(ActiveMQDestination destination) {
        return ReplicaSupport.REPLICATION_QUEUE_NAME.equals(destination.getPhysicalName());
    }

    private boolean isAdvisoryDestination(ActiveMQDestination destination) {
        return destination.getPhysicalName().startsWith(AdvisorySupport.ADVISORY_TOPIC_PREFIX);
    }

    private boolean isReplicaContext(ConnectionContext initialContext) {
        return initialContext != null && ReplicaSupport.REPLICATION_PLUGIN_USER_NAME.equals(initialContext.getUserName());
    }

    private void replicateDestinationCreation(ConnectionContext context, ActiveMQDestination destination) throws Exception {
        enqueueReplicaEvent(
                context,
                new ReplicaEvent()
                        .setEventType(ReplicaEventType.DESTINATION_UPSERT)
                        .setEventData(eventSerializer.serializeReplicationData(destination))
        );
        if (destinationsToReplicate.chooseValue(destination) == null) {
            destinationsToReplicate.put(destination, IS_REPLICATED);
        }
    }

    private void replicateDestinationRemoval(ConnectionContext context, ActiveMQDestination destination) {
        if (!isReplicatedDestination(destination)) {
            return;
        }
        try {
            enqueueReplicaEvent(
                    context,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.DESTINATION_DELETE)
                            .setEventData(eventSerializer.serializeReplicationData(destination))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate remove of destination {}", destination.getPhysicalName(), e);
        }
    }

    private boolean isReplicatedDestination(ActiveMQDestination destination) {
        if (destinationsToReplicate.chooseValue(destination) == null) {
            logger.debug("{} is not a replicated destination", destination.getPhysicalName());
            return false;
        }
        return true;
    }

    private void enqueueReplicaEvent(ConnectionContext initialContext, ReplicaEvent event) throws Exception {
        if (isReplicaContext(initialContext)) {
            return;
        }
        if (!initialized.get()) {
            return;
        }

        synchronized (sendingMutex) {
            logger.debug("Replicating {} event", event.getEventType());
            logger.trace("Replicating {} event: data:\n{}", event.getEventType(), new Object() {
                @Override
                public String toString() {
                    try {
                        return eventSerializer.deserializeMessageData(event.getEventData()).toString();
                    } catch (IOException e) {
                        return "<some event data>";
                    }
                }
            }); // FIXME: remove
            ActiveMQMessage eventMessage = new ActiveMQMessage();
            eventMessage.setPersistent(true);
            eventMessage.setType("ReplicaEvent");
            eventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
            eventMessage.setMessageId(new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
            eventMessage.setDestination(queueProvider.get());
            eventMessage.setProducerId(replicationProducerId);
            eventMessage.setResponseRequired(false);
            eventMessage.setContent(event.getEventData());
            eventMessage.setProperties(event.getReplicationProperties());
            replicaInternalMessageProducer.produceToReplicaQueue(eventMessage);
        }
    }
}
