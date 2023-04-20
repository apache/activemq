package org.apache.activemq.replica;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.virtual.CompositeDestinationFilter;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.TransactionId;

public class ReplicaDestinationFilter extends DestinationFilter {

    private final boolean nextIsComposite;
    private final ReplicaSourceBroker sourceBroker;
    private final ReplicaRoleManagementBroker roleManagementBroker;

    public ReplicaDestinationFilter(Destination next, ReplicaSourceBroker sourceBroker, ReplicaRoleManagementBroker roleManagementBroker) {
        super(next);
        this.nextIsComposite = this.next != null && this.next instanceof CompositeDestinationFilter;
        this.sourceBroker = sourceBroker;
        this.roleManagementBroker = roleManagementBroker;
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        if(ReplicaRole.source == roleManagementBroker.getRole()) {
            super.send(producerExchange, messageSend);
            if(!nextIsComposite) {
                // don't replicate composite destination
                replicateSend(producerExchange, messageSend);
            }
        } else {
            if(nextIsComposite) {
                // we jump over CompositeDestinationFilter as we don't want to fan out composite destinations on the replica side
                ((CompositeDestinationFilter) getNext()).getNext().send(producerExchange, messageSend);
            } else {
                super.send(producerExchange, messageSend);
            }
        }
    }

    private void replicateSend(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        final ConnectionContext connectionContext = producerExchange.getConnectionContext();
        if (!sourceBroker.needToReplicateSend(connectionContext, messageSend)) {
            return;
        }

        TransactionId transactionId = null;
        if (messageSend.getTransactionId() != null && !messageSend.getTransactionId().isXATransaction()) {
            transactionId = messageSend.getTransactionId();
        }

        sourceBroker.replicateSend(connectionContext, messageSend, transactionId);
    }

}