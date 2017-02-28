/*
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
package org.apache.activemq.transport.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSSecurityException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.net.ServerSocketFactory;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.LinkStealingTest;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.transport.TransportListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test state tracking operations during failover
 */
public class FailoverStateTrackingTest {

    private static final Logger LOG = LoggerFactory.getLogger(LinkStealingTest.class);

    private BrokerService brokerService;

    private int serverPort;
    private ActiveMQConnectionFactory cf;
    private String connectionURI;
    private ActiveMQConnection connection;

    private final AtomicLong consumerCounter = new AtomicLong();
    private final AtomicLong producerCounter = new AtomicLong();

    @Before
    public void setUp() throws Exception {
        serverPort = getProxyPort();
        createAuthenticatingBroker();

        connectionURI = "failover:(tcp://0.0.0.0:" + serverPort + ")?jms.watchTopicAdvisories=false";
        cf = new ActiveMQConnectionFactory(connectionURI);

        brokerService.start();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {}
            connection = null;
        }

        if (brokerService != null) {
            brokerService.stop();
            brokerService = null;
        }
    }

    @Test
    public void testUnauthorizedConsumerIsNotRecreated() throws Exception {
        final CountDownLatch connectionDropped = new CountDownLatch(1);
        final CountDownLatch connectionRestored = new CountDownLatch(1);

        connection = (ActiveMQConnection) cf.createConnection();
        connection.addTransportListener(new TransportListener() {

            @Override
            public void transportResumed() {
                if (connectionDropped.getCount() == 0) {
                    connectionRestored.countDown();
                }
            }

            @Override
            public void transportInterupted() {
                connectionDropped.countDown();
            }

            @Override
            public void onException(IOException error) {
            }

            @Override
            public void onCommand(Object command) {
            }
        });

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("testQueue");
        try {
            session.createConsumer(queue);
            fail("Should have failed to create this consumer");
        } catch (JMSSecurityException ex) {}

        brokerService.stop();
        brokerService.waitUntilStopped();

        assertTrue("Connection should be interrupted", connectionDropped.await(10, TimeUnit.SECONDS));

        createTrackingBroker();
        brokerService.start();

        assertTrue("Connection should be reconnected", connectionRestored.await(10, TimeUnit.SECONDS));

        try {
            session.createConsumer(queue);
        } catch (JMSSecurityException ex) {
            fail("Should have been able to create this consumer");
        }

        assertEquals(1, consumerCounter.get());
    }

    @Test
    public void testUnauthorizedProducerIsNotRecreated() throws Exception {
        final CountDownLatch connectionDropped = new CountDownLatch(1);
        final CountDownLatch connectionRestored = new CountDownLatch(1);

        connection = (ActiveMQConnection) cf.createConnection();
        connection.addTransportListener(new TransportListener() {

            @Override
            public void transportResumed() {
                LOG.debug("Connection restored");
                if (connectionDropped.getCount() == 0) {
                    connectionRestored.countDown();
                }
            }

            @Override
            public void transportInterupted() {
                LOG.debug("Connection interrupted");
                connectionDropped.countDown();
            }

            @Override
            public void onException(IOException error) {
            }

            @Override
            public void onCommand(Object command) {
            }
        });

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("testQueue");
        try {
            session.createProducer(queue);
            fail("Should have failed to create this producer");
        } catch (JMSSecurityException ex) {}

        brokerService.stop();
        brokerService.waitUntilStopped();

        assertTrue("Connection should be interrupted", connectionDropped.await(10, TimeUnit.SECONDS));

        createTrackingBroker();
        brokerService.start();

        assertTrue("Connection should be reconnected", connectionRestored.await(10, TimeUnit.SECONDS));

        try {
            session.createProducer(queue);
        } catch (JMSSecurityException ex) {
            fail("Should have been able to create this producer");
        }

        assertEquals(1, producerCounter.get());
    }

    private void createAuthenticatingBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setPlugins(new BrokerPlugin[] { new BrokerPluginSupport() {

            @Override
            public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
                throw new SecurityException();
            }

            @Override
            public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
                throw new SecurityException();
            }
        }});

        brokerService.addConnector("tcp://0.0.0.0:" + serverPort);
    }

    private void createTrackingBroker() throws Exception {
        consumerCounter.set(0);
        producerCounter.set(0);

        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setPlugins(new BrokerPlugin[] { new BrokerPluginSupport() {

            @Override
            public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
                consumerCounter.incrementAndGet();
                return getNext().addConsumer(context, info);
            }

            @Override
            public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
                producerCounter.incrementAndGet();
                getNext().addProducer(context, info);
            }

        }});

        brokerService.addConnector("tcp://0.0.0.0:" + serverPort);
    }

    protected int getProxyPort() {
        int proxyPort = 61616;

        try (ServerSocket ss = ServerSocketFactory.getDefault().createServerSocket(0)) {
            proxyPort = ss.getLocalPort();
        } catch (IOException e) {
        }

        return proxyPort;
    }
}
