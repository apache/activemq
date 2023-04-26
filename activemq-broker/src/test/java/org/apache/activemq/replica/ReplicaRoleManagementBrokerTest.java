package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaRoleManagementBrokerTest {

    private ReplicaRoleManagementBroker replicaRoleManagementBroker;
    private final Broker broker = mock(Broker.class);
    private final ReplicaBroker replicaBroker = mock(ReplicaBroker.class);
    private final ReplicaSourceBroker sourceBroker = mock(ReplicaSourceBroker.class);
    private final BrokerService brokerService = mock(BrokerService.class);
    private final PrefetchSubscription subscription = mock(PrefetchSubscription.class);

    @Before
    public void setUp() throws Exception {
        when(broker.getAdminConnectionContext()).thenReturn(new ConnectionContext());
        when(broker.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getBroker()).thenReturn(broker);
        when(broker.getDurableDestinations()).thenReturn(Set.of(new ActiveMQQueue(ReplicaSupport.REPLICATION_ROLE_QUEUE_NAME)));
        when(broker.getDestinations(any())).thenReturn(Set.of(mock(Queue.class)));
        when(broker.addConsumer(any(), any())).thenReturn(subscription);
        when(brokerService.addConnector(any(URI.class))).thenReturn(mock(TransportConnector.class));
        ReplicaPolicy replicaPolicy = new ReplicaPolicy();
        replicaPolicy.setControlWebConsoleAccess(false);
        replicaPolicy.setTransportConnectorUri(new URI("tcp://localhost:61617"));

        RegionBroker regionBroker = mock(RegionBroker.class);
        when(broker.getAdaptor(RegionBroker.class)).thenReturn(regionBroker);
        CompositeDestinationInterceptor cdi = mock(CompositeDestinationInterceptor.class);
        when(regionBroker.getDestinationInterceptor()).thenReturn(cdi);
        when(cdi.getInterceptors()).thenReturn(new DestinationInterceptor[]{});

        replicaRoleManagementBroker = new ReplicaRoleManagementBroker(broker, replicaPolicy, ReplicaRole.replica);
        replicaRoleManagementBroker.replicaBroker = replicaBroker;
        replicaRoleManagementBroker.sourceBroker = sourceBroker;
    }

    @Test
    public void startAsSourceWhenBrokerFailOverStateIsSource() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.source.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        verify(sourceBroker).start();
        verify(replicaBroker, never()).start();
    }

    @Test
    public void startAsReplicaWhenBrokerFailOverStateIsSource() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.replica.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        verify(replicaBroker).start();
        verify(sourceBroker, never()).start();
    }

    @Test
    public void startAsSourceWhenBrokerFailOverStateIsAwaitAck() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.await_ack.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        verify(((MutativeRoleBroker) sourceBroker)).stopBeforeRoleChange(false);
        verify(sourceBroker).start();
        verify(replicaBroker, never()).start();
    }

    @Test
    public void startAsSourceWhenBrokerFailOverStateIsAckReceived() throws Exception {
        when(replicaFailOverStateStorage.getBrokerState()).thenReturn(ReplicaRole.source);
        replicaRoleManagementBroker.start();

        verify(sourceBroker).start();
        verify(replicaBroker, never()).start();
    }

    @Test
    public void switchToSourceWhenHardFailOverInvoked() throws Exception {
        when(sourceBroker.isStopped()).thenReturn(true);
        replicaRoleManagementBroker.switchRole(ReplicaRole.source, true);

        verify((MutativeRoleBroker) replicaBroker).stopBeforeRoleChange(true);
    }

    @Test
    public void switchToReplicaWhenHardFailOverInvoked() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.source.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.switchRole(ReplicaRole.replica, true);

        verify((MutativeRoleBroker) sourceBroker).stopBeforeRoleChange(true);
    }

    @Test
    public void invokeSwitchToReplicaWhenSoftFailOverInvoked() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.source.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.switchRole(ReplicaRole.replica, false);

        verify((MutativeRoleBroker) sourceBroker).stopBeforeRoleChange(false);
    }

    @Test
    public void completeSwitchToReplicaWhenSoftFailOverInvoked() throws Exception {
        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.onStopSuccess();

        verify((MutativeRoleBroker) replicaBroker).startAfterRoleChange();
    }

    @Test
    public void switchToSourceWhenSoftFailOverInvoked() throws Exception {
        when(sourceBroker.isStopped()).thenReturn(false);
        replicaRoleManagementBroker.onFailOverAck();

        verify((MutativeRoleBroker) replicaBroker).stopBeforeRoleChange(true);
        verify((MutativeRoleBroker) sourceBroker).startAfterRoleChange();
    }
}
