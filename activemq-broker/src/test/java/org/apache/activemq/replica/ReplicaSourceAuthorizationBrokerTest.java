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

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.CompositeDestinationInterceptor;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicaSourceAuthorizationBrokerTest {

    private final Broker broker = mock(Broker.class);
    private final ConnectionContext connectionContext = mock(ConnectionContext.class);

    ReplicaSourceAuthorizationBroker source;
    private final TransportConnector transportConnector = mock(TransportConnector.class);

    private final ActiveMQQueue testDestination = new ActiveMQQueue("TEST.QUEUE");


    @Before
    public void setUp() throws Exception {
        RegionBroker regionBroker = mock(RegionBroker.class);
        when(broker.getAdaptor(RegionBroker.class)).thenReturn(regionBroker);
        CompositeDestinationInterceptor cdi = mock(CompositeDestinationInterceptor.class);
        when(regionBroker.getDestinationInterceptor()).thenReturn(cdi);
        when(cdi.getInterceptors()).thenReturn(new DestinationInterceptor[]{});
        when(connectionContext.getConnector()).thenReturn(transportConnector);

        source = new ReplicaSourceAuthorizationBroker(broker);
    }

    @Test
    public void letsCreateConsumerForReplicaQueueFromReplicaConnection() throws Exception {
        when(transportConnector.getName()).thenReturn(ReplicaSupport.REPLICATION_CONNECTOR_NAME);

        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(new ActiveMQQueue(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME));
        source.addConsumer(connectionContext, consumerInfo);

        verify(broker).addConsumer(eq(connectionContext), eq(consumerInfo));
    }

    @Test(expected = ActiveMQReplicaException.class)
    public void doesNotLetCreateConsumerForReplicaQueueFromNonReplicaConnection() throws Exception {
        when(transportConnector.getName()).thenReturn("test");

        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(new ActiveMQQueue(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME));
        source.addConsumer(connectionContext, consumerInfo);
    }

    @Test
    public void letsCreateConsumerForNonReplicaAdvisoryTopicFromReplicaConnection() throws Exception {
        when(transportConnector.getName()).thenReturn(ReplicaSupport.REPLICATION_CONNECTOR_NAME);

        ActiveMQTopic advisoryTopic = new ActiveMQTopic(AdvisorySupport.ADVISORY_TOPIC_PREFIX + "TEST");
        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(advisoryTopic);
        source.addConsumer(connectionContext, consumerInfo);

        verify(broker).addConsumer(eq(connectionContext), eq(consumerInfo));
    }

    @Test
    public void letsCreateConsumerForNonReplicaQueueFromNonReplicaConnection() throws Exception {
        when(transportConnector.getName()).thenReturn("test");

        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(testDestination);
        source.addConsumer(connectionContext, consumerInfo);

        verify(broker).addConsumer(eq(connectionContext), eq(consumerInfo));
    }

    @Test(expected = ActiveMQReplicaException.class)
    public void doesNoLetCreateConsumerForNonReplicaQueueFromReplicaConnection() throws Exception {
        when(transportConnector.getName()).thenReturn(ReplicaSupport.REPLICATION_CONNECTOR_NAME);

        ConsumerInfo consumerInfo = new ConsumerInfo();
        consumerInfo.setDestination(testDestination);
        source.addConsumer(connectionContext, consumerInfo);
    }

    @Test(expected = ActiveMQReplicaException.class)
    public void doesNotLetCreateProducerForReplicaQueueFromNonReplicaConnection() throws Exception {
        when(transportConnector.getName()).thenReturn("test");

        ProducerInfo producerInfo = new ProducerInfo();
        producerInfo.setDestination(new ActiveMQQueue(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME));
        source.addProducer(connectionContext, producerInfo);
    }

    @Test
    public void letsCreateProducerForReplicaQueueFromReplicaConnection() throws Exception {
        when(transportConnector.getName()).thenReturn(ReplicaSupport.REPLICATION_CONNECTOR_NAME);

        ProducerInfo producerInfo = new ProducerInfo();
        producerInfo.setDestination(new ActiveMQQueue(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME));
        source.addProducer(connectionContext, producerInfo);

        verify(broker).addProducer(eq(connectionContext), eq(producerInfo));
    }

    @Test
    public void letsCreateProducerForNonReplicaQueueFromNonReplicaConnection() throws Exception {
        when(transportConnector.getName()).thenReturn("test");

        ProducerInfo producerInfo = new ProducerInfo();
        producerInfo.setDestination(testDestination);
        source.addProducer(connectionContext, producerInfo);

        verify(broker).addProducer(eq(connectionContext), eq(producerInfo));
    }

    @Test(expected = ActiveMQReplicaException.class)
    public void doesNotLetCreateProducerForNonReplicaQueueFromReplicaConnection() throws Exception {
        when(transportConnector.getName()).thenReturn(ReplicaSupport.REPLICATION_CONNECTOR_NAME);

        ProducerInfo producerInfo = new ProducerInfo();
        producerInfo.setDestination(testDestination);
        source.addProducer(connectionContext, producerInfo);
    }
}
