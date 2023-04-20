package org.apache.activemq.replica.storage;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.replica.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.apache.activemq.replica.ReplicaRole;
import org.apache.activemq.replica.ReplicaSupport;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaFailOverStateStorageTest {
    private static final MessageId MESSAGE_ID = new MessageId("1:0:0:1");

    private final ReplicaReplicationQueueSupplier queueProvider = mock(ReplicaReplicationQueueSupplier.class);
    private final Broker broker = mock(Broker.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);
    private final ReplicaInternalMessageProducer internalMessageProducer = mock(ReplicaInternalMessageProducer.class);
    private final PrefetchSubscription subscription = mock(PrefetchSubscription.class);
    private final Queue failOverQueue = mock(Queue.class);
    private final ConnectionContext adminConnectionContext = mock(ConnectionContext.class);

    private ReplicaFailOverStateStorage replicaFailOverStateStorage;

    @Before
    public void setUp() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setMessageId(MESSAGE_ID);
        message.setText(ReplicaRole.source.name());

        when(adminConnectionContext.copy()).thenReturn(connectionContext);
        when(broker.getAdminConnectionContext()).thenReturn(adminConnectionContext);
        when(subscription.getDispatched()).thenReturn(List.of(message));
        when(broker.getDestinations(any())).thenReturn(Set.of(failOverQueue));
        when(broker.addConsumer(any(), any())).thenReturn(subscription);

        this.replicaFailOverStateStorage = new ReplicaFailOverStateStorage(queueProvider);
        replicaFailOverStateStorage.initialize(broker, connectionContext, internalMessageProducer);

    }

    @Test
    public void shouldReturnNullWhenNoBrokerStateStored() throws Exception {
        when(subscription.getDispatched()).thenReturn(new ArrayList<>());

        ReplicaRole replicaRole = replicaFailOverStateStorage.getBrokerState();

        verify(subscription).getDispatched();
        assertThat(replicaRole).isNull();
    }


    @Test
    public void shouldReturnBrokerStateStored() throws Exception {
        ReplicaRole replicaRole = replicaFailOverStateStorage.getBrokerState();

        verify(subscription).getDispatched();
        assertThat(replicaRole).isEqualTo(ReplicaRole.source);
    }

    @Test
    public void shouldUpdateBrokerStateStored() throws Exception {
        LocalTransactionId tid = new LocalTransactionId(
                new ConnectionId(ReplicaSupport.REPLICATION_PLUGIN_CONNECTION_ID),
                ReplicaSupport.LOCAL_TRANSACTION_ID_GENERATOR.getNextSequenceId());

        ArgumentCaptor<MessageAck> messageAckCaptor = ArgumentCaptor.forClass(MessageAck.class);
        replicaFailOverStateStorage.updateBrokerState(connectionContext, tid, ReplicaRole.replica.name());

        verify(subscription).getDispatched();
        verify(broker).acknowledge(any(), messageAckCaptor.capture());
        verify(internalMessageProducer).sendIgnoringFlowControl(any(), any());

        MessageAck messageAck = messageAckCaptor.getValue();
        assertThat(messageAck.getFirstMessageId()).isEqualTo(MESSAGE_ID);
    }
}
