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
package org.apache.activemq.usecases;

import static org.junit.Assert.assertNotEquals;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ConnectionControl;
import org.apache.log4j.Logger;
import org.springframework.core.io.ClassPathResource;

public class ClientRebalanceTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = Logger.getLogger(ClientRebalanceTest.class);
    private static final String QUEUE_NAME = "Test.ClientRebalanceTest";

    protected void setUp() throws Exception {
        setAutoFail(true);
        super.setUp();
    }


    public void testRebalance() throws Exception {
        createBroker(new ClassPathResource("org/apache/activemq/usecases/rebalance-broker1.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/rebalance-broker2.xml"));

        startAllBrokers();

        brokers.get("b1").broker.waitUntilStarted();

        LOG.info("Starting connection");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("failover:(tcp://localhost:61616,tcp://localhost:61617)?randomize=false");
        Connection conn = factory.createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue theQueue = session.createQueue(QUEUE_NAME);
        MessageProducer producer = session.createProducer(theQueue);
        MessageConsumer consumer = session.createConsumer(theQueue);
        Message message = session.createTextMessage("Test message");
        producer.send(message);
        Message msg = consumer.receive(2000);
        assertNotNull(msg);

        // introduce third broker
        createBroker(new ClassPathResource("org/apache/activemq/usecases/rebalance-broker3.xml"));
        brokers.get("b3").broker.waitUntilStarted();
        
        Thread.sleep(3000);

        LOG.info("Stopping broker 1");

        brokers.get("b1").broker.stop();
        brokers.get("b1").broker.waitUntilStopped();
        
        Thread.sleep(3000);
        // should reconnect to some of the remaining brokers
        producer.send(message);
        msg = consumer.receive(2000);
        assertNotNull(msg);

        LOG.info("Stopping broker 2");

        brokers.get("b2").broker.stop();
        brokers.get("b2").broker.waitUntilStopped();

        // should reconnect to broker3
        producer.send(message);
        msg = consumer.receive(2000);
        assertNotNull(msg);
    }
    
    public void testReconnect() throws Exception {
        createBroker(new ClassPathResource("org/apache/activemq/usecases/rebalance-broker1.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/rebalance-broker2.xml"));

        startAllBrokers();

        brokers.get("b1").broker.waitUntilStarted();

        LOG.info("Starting connection");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("failover:(tcp://localhost:61616)?randomize=false");
        Connection conn = factory.createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue theQueue = session.createQueue("Test.ClientReconnectTest");
        MessageProducer producer = session.createProducer(theQueue);
        MessageConsumer consumer = session.createConsumer(theQueue);
        Message message = session.createTextMessage("Test message");
        producer.send(message);
        Message msg = consumer.receive(2000);
        assertNotNull(msg);

        TransportConnector transportConnector = brokers.get("b1").broker.getTransportConnectorByName("openwire");
        assertNotNull(transportConnector);

        TransportConnection startFailoverConnection = findClientFailoverTransportConnection(transportConnector);

        assertNotNull(startFailoverConnection);
        String startConnectionId = startFailoverConnection.getConnectionId();
        String startRemoteAddress = startFailoverConnection.getRemoteAddress();
        ConnectionControl simulateRebalance = new ConnectionControl();
        simulateRebalance.setReconnectTo("tcp://localhost:61616");
        startFailoverConnection.dispatchSync(simulateRebalance);

        Thread.sleep(2000l);

        TransportConnection afterFailoverConnection = findClientFailoverTransportConnection(transportConnector);
        assertNotNull(afterFailoverConnection);
        assertEquals(startConnectionId, afterFailoverConnection.getConnectionId());
        assertNotEquals(startRemoteAddress, afterFailoverConnection.getRemoteAddress());
        // should still be able to send without exception to broker3
        producer.send(message);
        msg = consumer.receive(2000);
        assertNotNull(msg);
        conn.close();
    }
    
    protected TransportConnection findClientFailoverTransportConnection(TransportConnector transportConnector) {
        TransportConnection failoverConnection = null;
        for(TransportConnection tmpConnection : transportConnector.getConnections()) {
            if(tmpConnection.isNetworkConnection()) {
                continue;
            }
            if(tmpConnection.isFaultTolerantConnection()) {
                failoverConnection = tmpConnection;
            }
        }
        return failoverConnection;
    }
}
