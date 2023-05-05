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
package org.apache.activemq.replica.storage;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.replica.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.apache.activemq.replica.ReplicaSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaRecoverySequenceStorageTest {

    private final static String SEQUENCE_NAME = "testSeq";
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);
    private final Broker broker = mock(Broker.class);
    private final ReplicaReplicationQueueSupplier queueProvider = mock(ReplicaReplicationQueueSupplier.class);
    private final Queue sequenceQueue = mock(Queue.class);
    private final PrefetchSubscription subscription = mock(PrefetchSubscription.class);
    private final ActiveMQQueue sequenceQueueDestination = new ActiveMQQueue(ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
    private final ReplicaInternalMessageProducer replicaProducer = mock(ReplicaInternalMessageProducer.class);

    private ReplicaRecoverySequenceStorage replicaSequenceStorage;

    @Before
    public void setUp() throws Exception {
        when(broker.getDestinations(any())).thenReturn(Set.of(sequenceQueue));
        ConnectionContext adminConnectionContext = mock(ConnectionContext.class);
        when(adminConnectionContext.copy()).thenReturn(connectionContext);
        when(broker.getAdminConnectionContext()).thenReturn(adminConnectionContext);
        when(broker.addConsumer(any(), any())).thenReturn(subscription);
        when(queueProvider.getSequenceQueue()).thenReturn(sequenceQueueDestination);

        this.replicaSequenceStorage = new ReplicaRecoverySequenceStorage(broker, queueProvider, replicaProducer, SEQUENCE_NAME);
    }

    @Test
    public void shouldInitializeWhenNoMessagesExist() throws Exception {
        when(subscription.getDispatched()).thenReturn(new ArrayList<>());

        List<String> initialize = replicaSequenceStorage.initialize(connectionContext);
        assertThat(initialize).isEmpty();
        verify(sequenceQueue, never()).removeMessage(any());
    }

    @Test
    public void shouldInitializeWhenMoreThanOneExist() throws Exception {
        ActiveMQTextMessage message1 = new ActiveMQTextMessage();
        message1.setMessageId(new MessageId("1:0:0:1"));
        message1.setText("1");
        message1.setStringProperty(ReplicaBaseSequenceStorage.SEQUENCE_NAME_PROPERTY, SEQUENCE_NAME);
        ActiveMQTextMessage message2 = new ActiveMQTextMessage();
        message2.setMessageId(new MessageId("1:0:0:2"));
        message2.setText("2");
        message2.setStringProperty(ReplicaBaseSequenceStorage.SEQUENCE_NAME_PROPERTY, SEQUENCE_NAME);

        when(subscription.getDispatched())
                .thenReturn(List.of(new IndirectMessageReference(message1), new IndirectMessageReference(message2)));

        List<String> initialize = replicaSequenceStorage.initialize(connectionContext);
        assertThat(initialize).containsExactly(message1.getText(), message2.getText());
    }
}
