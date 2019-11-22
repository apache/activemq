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
package org.apache.activemq.broker.advisory;

import junit.framework.Test;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.SessionInfo;

public class AdvisoryBrokerTest extends BrokerTestSupport {
     
    public void testConnectionAdvisories() throws Exception {
        
        ActiveMQDestination destination = AdvisorySupport.getConnectionAdvisoryTopic();
        
        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(consumerInfo1);

        // We should get an advisory of our own connection.
        Message m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        assertEquals(((ConnectionInfo)m1.getDataStructure()).getConnectionId(), connectionInfo1.getConnectionId());

        // Setup a second connection 
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        connection2.send(connectionInfo2);
        
        // We should get an advisory of the second connection.
        m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        assertEquals(((ConnectionInfo)m1.getDataStructure()).getConnectionId(), connectionInfo2.getConnectionId());

        // Close the second connection.
        connection2.send(closeConnectionInfo(connectionInfo2));
        connection2.stop();

        // We should get an advisory of the second connection closing
        m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        RemoveInfo r = (RemoveInfo) m1.getDataStructure();
        assertEquals(r.getObjectId(), connectionInfo2.getConnectionId());
        
        assertNoMessagesLeft(connection1);
    }

    public void testConsumerAdvisories() throws Exception {

        ActiveMQDestination queue = new ActiveMQQueue("test");
        ActiveMQDestination destination = AdvisorySupport.getConsumerAdvisoryTopic(queue);
        
        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(consumerInfo1);

        // We should not see and advisory for the advisory consumer.
        assertNoMessagesLeft(connection1);

        // Setup a second consumer.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, queue);
        consumerInfo1.setPrefetchSize(100);
        
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(consumerInfo2);
        
        // We should get an advisory of the new consumer.
        Message m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        assertEquals(((ConsumerInfo)m1.getDataStructure()).getConsumerId(), consumerInfo2.getConsumerId());

        // Close the second connection.
        connection2.request(closeConnectionInfo(connectionInfo2));
        connection2.stop();

        // We should get an advisory of the consumer closing
        m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        RemoveInfo r = (RemoveInfo) m1.getDataStructure();
        assertEquals(r.getObjectId(), consumerInfo2.getConsumerId());
        
        assertNoMessagesLeft(connection2);
    }

    public void testConsumerAdvisoriesReplayed() throws Exception {

        ActiveMQDestination queue = new ActiveMQQueue("test");
        ActiveMQDestination destination = AdvisorySupport.getConsumerAdvisoryTopic(queue);
        
        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);

        // Setup a second consumer.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, queue);
        consumerInfo2.setPrefetchSize(100);        
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(consumerInfo2);
        
        // We should get an advisory of the previous consumer.        
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        Message m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        assertEquals(((ConsumerInfo)m1.getDataStructure()).getConsumerId(), consumerInfo2.getConsumerId());

        // Close the second connection.
        connection2.request(closeConnectionInfo(connectionInfo2));
        connection2.stop();

        // We should get an advisory of the consumer closing
        m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        RemoveInfo r = (RemoveInfo) m1.getDataStructure();
        assertEquals(r.getObjectId(), consumerInfo2.getConsumerId());
        
        assertNoMessagesLeft(connection2);
    }

    public void testProducerAdvisories() throws Exception {

        ActiveMQDestination queue = new ActiveMQQueue("test");
        ActiveMQDestination destination = AdvisorySupport.getProducerAdvisoryTopic(queue);
        
        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(consumerInfo1);

        assertNoMessagesLeft(connection1);

        // Setup a producer.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
        producerInfo2.setDestination(queue);
        
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);
        
        // We should get an advisory of the new produver.
        Message m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        assertEquals(((ProducerInfo)m1.getDataStructure()).getProducerId(), producerInfo2.getProducerId());

        // Close the second connection.
        connection2.request(closeConnectionInfo(connectionInfo2));
        connection2.stop();

        // We should get an advisory of the producer closing
        m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        RemoveInfo r = (RemoveInfo) m1.getDataStructure();
        assertEquals(r.getObjectId(), producerInfo2.getProducerId());
        
        assertNoMessagesLeft(connection2);
    }
    
    public void testProducerAdvisoriesReplayed() throws Exception {

        ActiveMQDestination queue = new ActiveMQQueue("test");
        ActiveMQDestination destination = AdvisorySupport.getProducerAdvisoryTopic(queue);
        
        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);

        // Setup a producer.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
        producerInfo2.setDestination(queue);
        
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);
        
        // Create the advisory consumer.. it should see the previous producer        
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        Message m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        assertEquals(((ProducerInfo)m1.getDataStructure()).getProducerId(), producerInfo2.getProducerId());

        // Close the second connection.
        connection2.request(closeConnectionInfo(connectionInfo2));
        connection2.stop();

        // We should get an advisory of the producer closing
        m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        RemoveInfo r = (RemoveInfo) m1.getDataStructure();
        assertEquals(r.getObjectId(), producerInfo2.getProducerId());
        
        assertNoMessagesLeft(connection2);
    }

    public void testProducerAdvisoriesReplayedOnlyTargetNewConsumer() throws Exception {

        ActiveMQDestination queue = new ActiveMQQueue("test");
        ActiveMQDestination destination = AdvisorySupport.getProducerAdvisoryTopic(queue);
        
        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        // Create the first consumer..         
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Setup a producer.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
        producerInfo2.setDestination(queue);        
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);
        
        Message m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        assertEquals(((ProducerInfo)m1.getDataStructure()).getProducerId(), producerInfo2.getProducerId());
        
        // Create the 2nd consumer..         
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(100);
        connection2.send(consumerInfo2);

        // The second consumer should se a replay
        m1 = receiveMessage(connection2);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        assertEquals(((ProducerInfo)m1.getDataStructure()).getProducerId(), producerInfo2.getProducerId());

        // But the first consumer should not see the replay.
        assertNoMessagesLeft(connection1);
    }

    public void testAnonymousProducerAdvisoriesTrue() throws Exception {
        //turn on support for anonymous producers
        broker.setAnonymousProducerAdvisorySupport(true);

        ActiveMQDestination destination = AdvisorySupport.getProducerAdvisoryTopic(null);
        assertEquals(AdvisorySupport.ANONYMOUS_PRODUCER_ADVISORY_TOPIC_PREFIX, destination.getPhysicalName());

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);

        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(consumerInfo1);

        assertNoMessagesLeft(connection1);

        // Setup a producer.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
        //don't set a destination

        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);

        // We should get an advisory of the new produver.
        Message m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        assertEquals(((ProducerInfo)m1.getDataStructure()).getProducerId(), producerInfo2.getProducerId());
        assertEquals(AdvisorySupport.ANONYMOUS_PRODUCER_ADVISORY_TOPIC_PREFIX, m1.getDestination().getPhysicalName());

        // Close the second connection.
        connection2.request(closeConnectionInfo(connectionInfo2));
        connection2.stop();

        // We should get an advisory of the producer closing
        m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNotNull(m1.getDataStructure());
        RemoveInfo r = (RemoveInfo) m1.getDataStructure();
        assertEquals(r.getObjectId(), producerInfo2.getProducerId());
        assertEquals(AdvisorySupport.ANONYMOUS_PRODUCER_ADVISORY_TOPIC_PREFIX, m1.getDestination().getPhysicalName());

        assertNoMessagesLeft(connection2);
    }

    public void testAnonymousProducerAdvisoriesFalse() throws Exception {
        broker.setAnonymousProducerAdvisorySupport(false);

        assertAnonymousProducerAdvisoriesOff();
    }

    public void testAnonymousProducerAdvisoriesDefault() throws Exception {
        //Default for now is to have anonymous producer advisories turned off
        assertAnonymousProducerAdvisoriesOff();
    }

    private void assertAnonymousProducerAdvisoriesOff() throws Exception {
        ActiveMQDestination destination = AdvisorySupport.getProducerAdvisoryTopic(null);
        assertEquals(AdvisorySupport.ANONYMOUS_PRODUCER_ADVISORY_TOPIC_PREFIX, destination.getPhysicalName());

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);

        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(consumerInfo1);

        assertNoMessagesLeft(connection1);

        // Setup a producer.
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
        //don't set a destination

        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);

        // We should get an advisory of the new produver.
        Message m1 = receiveMessage(connection1, 1000);
        assertNull(m1);

        assertNoMessagesLeft(connection2);
    }

    public static Test suite() {
        return suite(AdvisoryBrokerTest.class);
    }
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
