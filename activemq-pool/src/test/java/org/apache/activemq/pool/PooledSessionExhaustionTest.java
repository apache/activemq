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
package org.apache.activemq.pool;

import org.apache.activemq.*;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;

import junit.framework.*;
import javax.jms.*;
import javax.jms.Message;

import org.apache.log4j.Logger;

public class PooledSessionExhaustionTest extends TestCase {
    private static final String QUEUE = "FOO";
    private static final int NUM_MESSAGES = 700;

    private Logger logger = Logger.getLogger(getClass());

    private BrokerService broker;
    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;
    private String connectionUri;
    private int numReceived = 0;

    @Override
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        TransportConnector connector = broker.addConnector("tcp://localhost:0");
        broker.start();
        connectionUri = connector.getPublishableConnectString();
        factory = new ActiveMQConnectionFactory(connectionUri);
        pooledFactory = new PooledConnectionFactory(factory);
        pooledFactory.setMaxConnections(1);
        pooledFactory.setBlockIfSessionPoolIsFull(false);
    }

    @Override
    protected void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        broker = null;
    }

    public void sendMessages(ConnectionFactory connectionFactory) throws Exception {
        for (int i = 0; i < NUM_MESSAGES; i++) {
            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(QUEUE);
            MessageProducer producer = session.createProducer(destination);

            String msgTo = "hello";
            TextMessage message = session.createTextMessage(msgTo);
            producer.send(message);
            connection.close();
            logger.debug("sent " + i + " messages using " + connectionFactory.getClass());
        }
    }

    public void testCanExhaustSessions() throws Exception {
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connectionUri);
                    Connection connection = connectionFactory.createConnection();
                    connection.start();

                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Destination destination = session.createQueue(QUEUE);
                    MessageConsumer consumer = session.createConsumer(destination);
                    for (int i = 0; i < NUM_MESSAGES; ++i) {
                        Message msg = consumer.receive(5000);
                        if (msg == null) {
                            return;
                        }
                        numReceived++;
                        if (numReceived % 20 == 0) {
                            logger.debug("received " + numReceived + " messages ");
                            System.runFinalization();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();

        sendMessages(pooledFactory);
        thread.join();

        assertEquals(NUM_MESSAGES, numReceived);
    }
}
