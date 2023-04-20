package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.storage.ReplicaFailOverStateStorage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaRoleManagementBrokerTest {

    private ReplicaRoleManagementBroker replicaRoleManagementBroker;
    private final Broker broker = mock(Broker.class);
    private final MutativeRoleBroker replicaBroker = mock(ReplicaBroker.class);
    private final MutativeRoleBroker sourceBroker = mock(ReplicaSourceBroker.class);
    private final ReplicaFailOverStateStorage replicaFailOverStateStorage = mock(ReplicaFailOverStateStorage.class);

    @Before
    public void setUp() throws Exception {
        when(broker.getAdminConnectionContext()).thenReturn(new ConnectionContext());

        replicaRoleManagementBroker = new ReplicaRoleManagementBroker(broker, sourceBroker, replicaBroker, replicaFailOverStateStorage, ReplicaRole.replica);
    }

    @Test
    public void startAsSourceWhenBrokerFailOverStateIsSource() throws Exception {
        when(replicaFailOverStateStorage.getBrokerState()).thenReturn(ReplicaRole.source);

        replicaRoleManagementBroker.start();

        verify(sourceBroker).start();
        verify(replicaBroker, never()).start();
    }

    @Test
    public void startAsReplicaWhenBrokerFailOverStateIsSource() throws Exception {
        when(replicaFailOverStateStorage.getBrokerState()).thenReturn(ReplicaRole.replica);
        replicaRoleManagementBroker.start();

        verify(replicaBroker).start();
        verify(sourceBroker, never()).start();
    }

    @Test
    public void startAsSourceWhenBrokerFailOverStateIsAwaitAck() throws Exception {
        when(replicaFailOverStateStorage.getBrokerState()).thenReturn(ReplicaRole.await_ack);
        replicaRoleManagementBroker.start();

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
        verify(sourceBroker).start();
        verify(replicaFailOverStateStorage).updateBrokerState(any(), any(TransactionId.class), eq(ReplicaRole.source.name()));
    }

    @Test
    public void switchToReplicaWhenHardFailOverInvoked() throws Exception {
        replicaRoleManagementBroker = new ReplicaRoleManagementBroker(broker, sourceBroker, replicaBroker, replicaFailOverStateStorage, ReplicaRole.source);
        when(replicaBroker.isStopped()).thenReturn(false);
        replicaRoleManagementBroker.switchRole(ReplicaRole.replica, true);

        verify((MutativeRoleBroker) sourceBroker).stopBeforeRoleChange(true);
        verify((MutativeRoleBroker) replicaBroker).startAfterRoleChange();
        verify(replicaFailOverStateStorage).updateBrokerState(any(), any(TransactionId.class), eq(ReplicaRole.replica.name()));
    }

    @Test
    public void invokeSwitchToReplicaWhenSoftFailOverInvoked() throws Exception {
        replicaRoleManagementBroker = new ReplicaRoleManagementBroker(broker, sourceBroker, replicaBroker, replicaFailOverStateStorage, ReplicaRole.source);
        replicaRoleManagementBroker.switchRole(ReplicaRole.replica, false);

        verify((MutativeRoleBroker) sourceBroker).stopBeforeRoleChange(false);
        verify(replicaFailOverStateStorage, never()).updateBrokerState(any(), any(TransactionId.class), anyString());
    }

    @Test
    public void completeSwitchToReplicaWhenSoftFailOverInvoked() throws Exception {
        replicaRoleManagementBroker = new ReplicaRoleManagementBroker(broker, sourceBroker, replicaBroker, replicaFailOverStateStorage, ReplicaRole.source);
        when(replicaBroker.isStopped()).thenReturn(false);
        replicaRoleManagementBroker.onDeinitializationSuccess();

        verify((MutativeRoleBroker) replicaBroker).startAfterRoleChange();
        verify(replicaFailOverStateStorage).updateBrokerState(any(), any(TransactionId.class), eq(ReplicaRole.replica.name()));
    }

    @Test
    public void switchToSourceWhenSoftFailOverInvoked() throws Exception {
        when(sourceBroker.isStopped()).thenReturn(false);
        replicaRoleManagementBroker.onFailOverAck();

        verify((MutativeRoleBroker) replicaBroker).stopBeforeRoleChange(true);
        verify((MutativeRoleBroker) sourceBroker).startAfterRoleChange();
        verify(replicaFailOverStateStorage).updateBrokerState(any(), any(TransactionId.class), eq(ReplicaRole.source.name()));
    }
}
