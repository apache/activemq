package org.apache.activemq.replica;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaReplicationQueueSupplierTest {

    private final Broker broker = mock(Broker.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);
    private final BrokerService brokerService = mock(BrokerService.class);

    private final ReplicaReplicationQueueSupplier supplier = new ReplicaReplicationQueueSupplier(broker);

    @Before
    public void setUp() throws Exception {
        when(broker.getAdminConnectionContext()).thenReturn(connectionContext);
        when(broker.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getBroker()).thenReturn(broker);
    }

    @Test
    public void canCreateQueue() throws Exception {
        supplier.initialize();

        ActiveMQQueue activeMQQueue = supplier.get();
        assertThat(activeMQQueue.getPhysicalName()).isEqualTo(ReplicaSupport.REPLICATION_QUEUE_NAME);

        verify(broker).addDestination(eq(connectionContext), eq(activeMQQueue), eq(false));
    }

    @Test
    public void notCreateQueueIfExists() throws Exception {
        ActiveMQQueue replicationQueue = new ActiveMQQueue(ReplicaSupport.REPLICATION_QUEUE_NAME);

        when(broker.getDurableDestinations()).thenReturn(Collections.singleton(replicationQueue));

        supplier.initialize();

        ActiveMQQueue activeMQQueue = supplier.get();
        assertThat(activeMQQueue).isEqualTo(replicationQueue);

        verify(broker, never()).addDestination(any(), any(), anyBoolean());
    }

}
