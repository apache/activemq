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
package org.apache.activemq.security;

import java.net.URI;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XBeanSecurityWithGuestTest extends JmsTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(XBeanSecurityWithGuestTest.class);
    public ActiveMQDestination destination;
    
    public static Test suite() {
        return suite(XBeanSecurityWithGuestTest.class);
    }
    
    public void testUserSendGoodPassword() throws JMSException {
        Message m = doSend(false);
        assertEquals("system", ((ActiveMQMessage)m).getUserID());
        assertEquals("system", m.getStringProperty("JMSXUserID"));
    }
    
    public void testUserSendWrongPassword() throws JMSException {
        Message m = doSend(false);
        // note brokerService.useAuthenticatedPrincipalForJMXUserID=true for this
        assertEquals("guest", ((ActiveMQMessage)m).getUserID());
        assertEquals("guest", m.getStringProperty("JMSXUserID"));
    }

    protected BrokerService createBroker() throws Exception {
        return createBroker("org/apache/activemq/security/jaas-broker-guest.xml");
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }

    public Message doSend(boolean fail) throws JMSException {

        Connection adminConnection = factory.createConnection("system", "manager");
        connections.add(adminConnection);

        adminConnection.start();
        Session adminSession = adminConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = adminSession.createConsumer(destination);

        connections.remove(connection);
        connection = (ActiveMQConnection)factory.createConnection(userName, password);
        connections.add(connection);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try {
            sendMessages(session, destination, 1);
        } catch (JMSException e) {
            // If test is expected to fail, the cause must only be a
            // SecurityException
            // otherwise rethrow the exception
            if (!fail || !(e.getCause() instanceof SecurityException)) {
                throw e;
            }
        }

        Message m = consumer.receive(1000);
        if (fail) {
            assertNull(m);
        } else {
            assertNotNull(m);
            assertEquals("0", ((TextMessage)m).getText());
            assertNull(consumer.receiveNoWait());
        }
        return m;
    }
    
    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestUserSendGoodPassword() {
        addCombinationValues("userName", new Object[] {"system"});
        addCombinationValues("password", new Object[] {"manager"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("test"), new ActiveMQTopic("test")});
    }
    
    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestUserSendWrongPassword() {
        addCombinationValues("userName", new Object[] {"system"});
        addCombinationValues("password", new Object[] {"wrongpassword"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("GuestQueue")});
    }
}
