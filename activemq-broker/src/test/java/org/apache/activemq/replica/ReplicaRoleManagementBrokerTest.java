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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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
        SystemUsage systemUsage = mock(SystemUsage.class);
        when(brokerService.getSystemUsage()).thenReturn(systemUsage);
        MemoryUsage memoryUsage = mock(MemoryUsage.class);
        when(systemUsage.getMemoryUsage()).thenReturn(memoryUsage);

        RegionBroker regionBroker = mock(RegionBroker.class);
        when(broker.getAdaptor(RegionBroker.class)).thenReturn(regionBroker);
        CompositeDestinationInterceptor cdi = mock(CompositeDestinationInterceptor.class);
        when(regionBroker.getDestinationInterceptor()).thenReturn(cdi);
        when(cdi.getInterceptors()).thenReturn(new DestinationInterceptor[]{});

        replicaRoleManagementBroker = new ReplicaRoleManagementBroker(broker, replicaPolicy, ReplicaRole.replica, new ReplicaStatistics());
        replicaRoleManagementBroker.replicaBroker = replicaBroker;
        replicaRoleManagementBroker.sourceBroker = sourceBroker;
    }

    @Test
    public void startAsSourceWhenBrokerFailOverStateIsSource() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.source.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        verify(sourceBroker).start(any());
        verify(replicaBroker, never()).start();
    }

    @Test
    public void startAsReplicaWhenBrokerFailOverStateIsReplica() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.replica.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        verify(replicaBroker).start(any());
        verify(sourceBroker, never()).start();
    }

    @Test
    public void startAsSourceWhenBrokerFailOverStateIsAwaitAck() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.await_ack.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        verify(sourceBroker).start(any());
        verify(replicaBroker, never()).start();
    }

    @Test
    public void startAsReplicaWhenBrokerFailOverStateIsAckProcessed() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.ack_processed.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        verify(replicaBroker).start(any());
        verify(sourceBroker, never()).start();
    }

    @Test
    public void switchToSourceWhenHardFailOverInvoked() throws Exception {
        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.switchRole(ReplicaRole.source, true);

        verify(replicaBroker).stopBeforeRoleChange(true);
    }

    @Test
    public void switchToReplicaWhenHardFailOverInvoked() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.source.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.switchRole(ReplicaRole.replica, true);

        verify(sourceBroker).stopBeforeRoleChange(true);
    }

    @Test
    public void invokeSwitchToReplicaWhenSoftFailOverInvoked() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.source.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.switchRole(ReplicaRole.replica, false);

        verify(sourceBroker).stopBeforeRoleChange(false);
    }

    @Test
    public void doNotInvokeSwitchToReplicaWhenAwaitAck() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.await_ack.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.switchRole(ReplicaRole.replica, false);

        verify(sourceBroker, never()).stopBeforeRoleChange(anyBoolean());
    }

    @Test
    public void doNotInvokeSwitchToReplicaWhenAckProcessed() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.ack_processed.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.switchRole(ReplicaRole.source, false);

        verify(replicaBroker, never()).stopBeforeRoleChange(anyBoolean());
    }

    @Test
    public void invokeSwitchToReplicaWhenAwaitAckAndForce() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.await_ack.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.switchRole(ReplicaRole.replica, true);

        verify(sourceBroker).stopBeforeRoleChange(true);
    }

    @Test
    public void invokeSwitchToReplicaWhenAckProcessedAndForce() throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setText(ReplicaRole.ack_processed.name());
        when(subscription.getDispatched()).thenReturn(List.of(message));

        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.switchRole(ReplicaRole.source, true);

        verify(replicaBroker).stopBeforeRoleChange(true);
    }

    @Test
    public void completeSwitchToReplicaWhenSoftFailOverInvoked() throws Exception {
        replicaRoleManagementBroker.start();

        replicaRoleManagementBroker.onStopSuccess();

        verify(replicaBroker).startAfterRoleChange();
    }
}
