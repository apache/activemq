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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMSClientSimpleAuthTest {

    @Rule public TestName name = new TestName();

    private static final Logger LOG = LoggerFactory.getLogger(JMSClientSimpleAuthTest.class);

    private final String SIMPLE_AUTH_AMQP_BROKER_XML =
        "org/apache/activemq/transport/amqp/simple-auth-amqp-broker.xml";
    private BrokerService brokerService;
    private Connection connection;
    private URI amqpURI;

    @Before
    public void setUp() throws Exception {
        LOG.info("========== starting: " + getTestName() + " ==========");
        startBroker();
    }

    @After
    public void stopBroker() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ex) {}
            connection = null;
        }

        if (brokerService != null) {
            brokerService.stop();
            brokerService = null;
        }

        LOG.info("========== finished: " + getTestName() + " ==========");
    }

    public String getTestName() {
        return name.getMethodName();
    }

    @Test(timeout = 10000)
    public void testNoUserOrPassword() throws Exception {
        try {
            connection = JMSClientContext.INSTANCE.createConnection(amqpURI, "", "");
            connection.start();
            fail("Expected JMSException");
        } catch (JMSSecurityException ex) {
            LOG.debug("Failed to authenticate connection with no user / password.");
        }
    }

    @Test(timeout = 10000)
    public void testUnknownUser() throws Exception {
        try {
            connection = JMSClientContext.INSTANCE.createConnection(amqpURI, "nosuchuser", "blah");
            connection.start();
            fail("Expected JMSException");
        } catch (JMSSecurityException ex) {
            LOG.debug("Failed to authenticate connection with unknown user ID");
        }
    }

    @Test(timeout = 10000)
    public void testKnownUserWrongPassword() throws Exception {
        try {
            connection = JMSClientContext.INSTANCE.createConnection(amqpURI, "user", "wrongPassword");
            connection.start();
            fail("Expected JMSException");
        } catch (JMSSecurityException ex) {
            LOG.debug("Failed to authenticate connection with incorrect password.");
        }
    }

    @Test(timeout = 30000)
    public void testRepeatedWrongPasswordAttempts() throws Exception {
        for (int i = 0; i < 25; ++i) {
            Connection connection = null;
            try {
                connection = JMSClientContext.INSTANCE.createConnection(amqpURI, "user", "wrongPassword");
                connection.start();
                fail("Expected JMSException");
            } catch (JMSSecurityException ex) {
                LOG.debug("Failed to authenticate connection with incorrect password.");
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    @Test(timeout = 30000)
    public void testSendReceive() throws Exception {
        connection = JMSClientContext.INSTANCE.createConnection(amqpURI, "user", "userPassword");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("USERS.txQueue");
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

    @Test(timeout = 30000)
    public void testProducerNotAuthorized() throws Exception {
        connection = JMSClientContext.INSTANCE.createConnection(amqpURI, "guest", "guestPassword");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("USERS.txQueue");
        try {
            session.createProducer(queue);
            fail("Should not be able to produce here.");
        } catch (JMSSecurityException jmsSE) {
            LOG.info("Caught expected exception");
        }
    }

    @Test(timeout = 30000)
    public void testAnonymousProducerNotAuthorized() throws Exception {
        connection = JMSClientContext.INSTANCE.createConnection(amqpURI, "guest", "guestPassword");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("USERS.txQueue");
        MessageProducer producer = session.createProducer(null);

        try {
            producer.send(queue, session.createTextMessage());
            fail("Should not be able to produce here.");
        } catch (JMSSecurityException jmsSE) {
            LOG.info("Caught expected exception");
        }
    }

    @Test(timeout = 30000)
    public void testCreateTemporaryQueueNotAuthorized() throws JMSException {
        connection = JMSClientContext.INSTANCE.createConnection(amqpURI, "user", "userPassword");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        try {
            session.createTemporaryQueue();
        } catch (JMSSecurityException jmsse) {
        } catch (JMSException jmse) {
            LOG.info("Client should have thrown a JMSSecurityException but only threw JMSException");
        }

        // Should not be fatal
        assertNotNull(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
    }

    @Test(timeout = 30000)
    public void testCreateTemporaryTopicNotAuthorized() throws JMSException {
        connection = JMSClientContext.INSTANCE.createConnection(amqpURI, "user", "userPassword");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        try {
            session.createTemporaryTopic();
        } catch (JMSSecurityException jmsse) {
        } catch (JMSException jmse) {
            LOG.info("Client should have thrown a JMSSecurityException but only threw JMSException");
        }

        // Should not be fatal
        assertNotNull(connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
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
        amqpURI = brokerService.getTransportConnectorByName("amqp").getPublishableConnectURI();
        brokerService.waitUntilStarted();
    }
}
