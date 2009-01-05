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
package org.apache.activemq.store.kahadb;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;

import junit.framework.Test;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.RecoveryBrokerTest;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;


/**
 * Used to verify that recovery works correctly against 
 * 
 * @version $Revision: 712224 $
 */
public class KahaDBStoreRecoveryBrokerTest extends RecoveryBrokerTest {

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File("target/activemq-data/kahadb"));
        kaha.deleteAllMessages();
        broker.setPersistenceAdapter(kaha);
        return broker;
    }
    
    protected BrokerService createRestartedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File("target/activemq-data/kahadb"));
        broker.setPersistenceAdapter(kaha);
        return broker;
    }
    
    public static Test suite() {
        return suite(KahaDBStoreRecoveryBrokerTest.class);
    }
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    
    public void testLargeQueuePersistentMessagesNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);
        
        ArrayList<String> expected = new ArrayList<String>();
        
        int MESSAGE_COUNT = 10000;
        for(int i=0; i < MESSAGE_COUNT; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
            expected.add(message.getMessageId().toString());
        }
        connection.request(closeConnectionInfo(connectionInfo));

        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);
        producerInfo = createProducerInfo(sessionInfo);
        connection.send(producerInfo);

        for(int i=0; i < MESSAGE_COUNT/2; i++) {
            Message m = receiveMessage(connection);
            assertNotNull("Should have received message "+expected.get(0)+" by now!", m);
            assertEquals(expected.remove(0), m.getMessageId().toString());
            MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
            connection.send(ack);
        }
        
        connection.request(closeConnectionInfo(connectionInfo));
        
        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        for(int i=0; i < MESSAGE_COUNT/2; i++) {
            Message m = receiveMessage(connection);
            assertNotNull("Should have received message "+expected.get(i)+" by now!", m);
            assertEquals(expected.get(i), m.getMessageId().toString());
            MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
            connection.send(ack);
            
            
        }
        
        connection.request(closeConnectionInfo(connectionInfo));
    }
}
