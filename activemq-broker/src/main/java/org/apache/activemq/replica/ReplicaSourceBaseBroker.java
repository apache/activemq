package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ReplicaSourceBaseBroker extends BrokerFilter {
    final ReplicaReplicationQueueSupplier queueProvider;
    private ReplicationMessageProducer replicationMessageProducer;
    protected final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();

    private final AtomicBoolean initialized = new AtomicBoolean();

    ReplicaSourceBaseBroker(Broker next) {
        super(next);
        queueProvider = new ReplicaReplicationQueueSupplier(next);
    }

    @Override
    public void start() throws Exception {
        queueProvider.initialize();
        initialized.compareAndSet(false, true);

        ReplicaInternalMessageProducer replicaInternalMessageProducer = new ReplicaInternalMessageProducer(next, getAdminConnectionContext());
        replicationMessageProducer = new ReplicationMessageProducer(replicaInternalMessageProducer, queueProvider);
        super.start();
    }


    protected void enqueueReplicaEvent(ConnectionContext initialContext, ReplicaEvent event) throws Exception {
        if (isReplicaContext(initialContext)) {
            return;
        }
        if (!initialized.get()) {
            return;
        }
        replicationMessageProducer.enqueueReplicaEvent(event);
    }

    protected boolean isReplicaContext(ConnectionContext initialContext) {
        return initialContext != null && ReplicaSupport.REPLICATION_PLUGIN_USER_NAME.equals(initialContext.getUserName());
    }

}
