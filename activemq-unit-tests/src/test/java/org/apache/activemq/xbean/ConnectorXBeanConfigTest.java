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
package org.apache.activemq.xbean;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.jms.*;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.MessageIdList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ConnectorXBeanConfigTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorXBeanConfigTest.class);
    protected BrokerService brokerService;

    public void testConnectorConfiguredCorrectly() throws Exception {

        TransportConnector connector = brokerService.getTransportConnectors().get(0);

        assertEquals(new URI("tcp://localhost:61636"), connector.getUri());
        assertTrue(connector.getTaskRunnerFactory() == brokerService.getTaskRunnerFactory());

        NetworkConnector netConnector = brokerService.getNetworkConnectors().get(0);
        List<ActiveMQDestination> excludedDestinations = netConnector.getExcludedDestinations();
        assertEquals(new ActiveMQQueue("exclude.test.foo"), excludedDestinations.get(0));
        assertEquals(new ActiveMQTopic("exclude.test.bar"), excludedDestinations.get(1));

        List<ActiveMQDestination> dynamicallyIncludedDestinations = netConnector.getDynamicallyIncludedDestinations();
        assertEquals(new ActiveMQQueue("include.test.foo"), dynamicallyIncludedDestinations.get(0));
        assertEquals(new ActiveMQTopic("include.test.bar"), dynamicallyIncludedDestinations.get(1));
    }

    public void testBrokerRestartIsAllowed() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();

        // redundant start is now ignored
        brokerService.start();

        assertTrue("mapped address in published address", brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString().contains("Mapped"));
    }

    public void testForceBrokerRestart() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();

        brokerService.start(true); // force restart
        brokerService.waitUntilStarted();

        LOG.info("try and connect to restarted broker");
        //send and receive a message from a restarted broker
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61636");
        Connection conn = factory.createConnection();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();
        Destination dest = new ActiveMQQueue("test");
        MessageConsumer consumer = sess.createConsumer(dest);
        MessageProducer producer = sess.createProducer(dest);
        producer.send(sess.createTextMessage("test"));
        TextMessage msg = (TextMessage)consumer.receive(1000);
        assertEquals("test", msg.getText());
    }


    public void testBrokerWontStop() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?async=false");
        factory.setDispatchAsync(false);
        factory.setAlwaysSessionAsync(false);
        Connection conn = factory.createConnection();
        final Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        conn.start();
        final Destination dest = new ActiveMQQueue("TEST");
        final CountDownLatch stop = new CountDownLatch(1);
        final CountDownLatch sendSecond = new CountDownLatch(1);
        final CountDownLatch shutdown = new CountDownLatch(1);
        final CountDownLatch test = new CountDownLatch(1);

        ActiveMQConnectionFactory testFactory = new ActiveMQConnectionFactory("vm://localhost?async=false");
        Connection testConn = testFactory.createConnection();
        testConn.start();
        Destination testDestination = sess.createQueue("NEW");
        Session testSess = testConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer testProducer = testSess.createProducer(testDestination);

        final Thread consumerThread = new Thread() {
            @Override
            public void run() {
                try {
                MessageProducer producer = sess.createProducer(dest);
                producer.send(sess.createTextMessage("msg1"));
                MessageConsumer consumer = sess.createConsumer(dest);
                consumer.setMessageListener(new MessageListener() {
                    @Override
                    public void onMessage(Message message) {
                        try {
                            // send a message that will block
                            Thread.sleep(2000);
                            sendSecond.countDown();
                            // try to stop the broker
                            Thread.sleep(5000);
                            stop.countDown();
                            // run the test
                            Thread.sleep(5000);
                            test.countDown();
                            shutdown.await();
                        } catch (InterruptedException ie) {
                        }
                    }
                });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        consumerThread.start();

        final Thread producerThread = new Thread() {
            @Override
            public void run() {
                try {
                    sendSecond.await();
                    MessageProducer producer = sess.createProducer(dest);
                    producer.send(sess.createTextMessage("msg2"));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        producerThread.start();

        final Thread stopThread = new Thread() {
            @Override
            public void run() {
                try {
                    stop.await();
                    brokerService.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        stopThread.start();

        test.await();
        try {
            testSess.createConsumer(testDestination);
            fail("Should have failed creating a consumer!");
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            testProducer.send(testSess.createTextMessage("msg3"));
            fail("Should have failed sending a message!");
        } catch (Exception e) {
            e.printStackTrace();
        }

        shutdown.countDown();


    }

    @Override
    protected void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();
    }

    @Override
    protected void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    protected BrokerService createBroker() throws Exception {
        String uri = "org/apache/activemq/xbean/connector-test.xml";
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }
}
