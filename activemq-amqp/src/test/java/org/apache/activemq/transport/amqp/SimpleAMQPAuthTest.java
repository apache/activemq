package org.apache.activemq.transport.amqp;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.qpid.amqp_1_0.client.ConnectionClosedException;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleAMQPAuthTest {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleAMQPAuthTest.class);

    private final String SIMPLE_AUTH_AMQP_BROKER_XML =
        "org/apache/activemq/transport/amqp/simple-auth-amqp-broker.xml";
    private BrokerService brokerService;
    private int port;

    @Before
    public void setUp() throws Exception {
        startBroker();
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService = null;
        }
    }

    @Test(timeout = 10000)
    public void testNoUserOrPassword() throws Exception {
        try {
            ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", port, "", "");
            Connection connection = factory.createConnection();
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    LOG.error("Unexpected exception ", exception);
                    exception.printStackTrace();
                }
            });
            connection.start();
            Thread.sleep(1000);
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Expected JMSException");
        } catch (JMSException e) {
            Exception linkedException = e.getLinkedException();
            if (linkedException != null && linkedException instanceof ConnectionClosedException) {
                ConnectionClosedException cce = (ConnectionClosedException) linkedException;
                assertEquals("Error{condition=unauthorized-access,description=User name [null] or password is invalid.}", cce.getRemoteError().toString());
            } else {
                LOG.error("Unexpected Exception", e);
                fail("Unexpected exception: " + e.getMessage());
            }
        }
    }

    @Test(timeout = 10000)
    public void testUnknownUser() throws Exception {
        try {
            ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", port, "admin", "password");
            Connection connection = factory.createConnection("nosuchuser", "blah");
            connection.start();
            Thread.sleep(500);
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Expected JMSException");
        } catch (JMSException e)  {
            Exception linkedException = e.getLinkedException();
            if (linkedException != null && linkedException instanceof ConnectionClosedException) {
                ConnectionClosedException cce = (ConnectionClosedException) linkedException;
                assertEquals("Error{condition=unauthorized-access,description=User name [nosuchuser] or password is invalid.}", cce.getRemoteError().toString());
            } else {
                LOG.error("Unexpected Exception", e);
                fail("Unexpected exception: " + e.getMessage());
            }
        }
    }

    @Test(timeout = 10000)
    public void testKnownUserWrongPassword() throws Exception {
        try {
            ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", port, "admin", "password");
            Connection connection = factory.createConnection("user", "wrongPassword");
            connection.start();
            Thread.sleep(500);
            connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Expected JMSException");
        } catch (JMSException e) {
            Exception linkedException = e.getLinkedException();
            if (linkedException != null && linkedException instanceof ConnectionClosedException) {
                ConnectionClosedException cce = (ConnectionClosedException) linkedException;
                assertEquals("Error{condition=unauthorized-access,description=User name [user] or password is invalid.}", cce.getRemoteError().toString());
            } else {
                LOG.error("Unexpected Exception", e);
                fail("Unexpected exception: " + e.getMessage());
            }
        }
    }

    @Test(timeout = 30000)
    public void testSendReceive() throws Exception {
        ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", port, "admin", "password");
        Connection connection = factory.createConnection("user", "userPassword");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        QueueImpl queue = new QueueImpl("queue://txqueue");
        MessageProducer p = session.createProducer(queue);
        TextMessage message = null;
        message = session.createTextMessage();
        String messageText = "hello  sent at " + new java.util.Date().toString();
        message.setText(messageText);
        p.send(message);

        // Get the message we just sent
        MessageConsumer consumer = session.createConsumer(queue);
        connection.start();
        Message msg = consumer.receive(5000);
        assertNotNull(msg);
        assertTrue(msg instanceof TextMessage);
        TextMessage textMessage = (TextMessage) msg;
        assertEquals(messageText, textMessage.getText());
        connection.close();
    }

    protected BrokerService createBroker() throws Exception {
        return createBroker(SIMPLE_AUTH_AMQP_BROKER_XML);
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.debug(">>>>> Loading broker configuration from the classpath with URI: {}", uri);
        return BrokerFactory.createBroker(new URI("xbean:" +  uri));
    }

    public void startBroker() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        port = brokerService.getTransportConnectorByName("amqp").getPublishableConnectURI().getPort();
        brokerService.waitUntilStarted();
    }
}

