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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

/**
 * @version $Revision$
 */
public class SecurityTestSupport extends JmsTestSupport {

    public ActiveMQDestination destination;

    /**
     * Overrides to set the JMSXUserID flag to true.
     */
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.setPopulateJMSXUserID(true);
        return broker;
    }

    public void testUserReceiveFails() throws JMSException {
        doReceive(true);
    }

    public void testInvalidAuthentication() throws JMSException {
        try {
            // No user id
            Connection c = factory.createConnection();
            connections.add(c);
            c.start();
            fail("Expected exception.");
        } catch (JMSException e) {
        }

        try {
            // Bad password
            Connection c = factory.createConnection("user", "krap");
            connections.add(c);
            c.start();
            fail("Expected exception.");
        } catch (JMSException e) {
        }

        try {
            // Bad userid
            Connection c = factory.createConnection("userkrap", null);
            connections.add(c);
            c.start();
            fail("Expected exception.");
        } catch (JMSException e) {
        }
    }

    public void testUserReceiveSucceeds() throws JMSException {
        Message m = doReceive(false);
        assertEquals("system", ((ActiveMQMessage)m).getUserID());
        assertEquals("system", m.getStringProperty("JMSXUserID"));
    }

    public void testGuestReceiveSucceeds() throws JMSException {
        doReceive(false);
    }

    public void testGuestReceiveFails() throws JMSException {
        doReceive(true);
    }

    public void testUserSendSucceeds() throws JMSException {
        Message m = doSend(false);
        assertEquals("user", ((ActiveMQMessage)m).getUserID());
        assertEquals("user", m.getStringProperty("JMSXUserID"));
    }

    public void testUserSendFails() throws JMSException {
        doSend(true);
    }

    public void testGuestSendFails() throws JMSException {
        doSend(true);
    }

    public void testGuestSendSucceeds() throws JMSException {
        doSend(false);
    }

    /**
     * @throws JMSException
     */
    public Message doSend(boolean fail) throws JMSException {

        Connection adminConnection = factory.createConnection("system", "manager");
        connections.add(adminConnection);

        adminConnection.start();
        Session adminSession = adminConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = adminSession.createConsumer(destination);

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
     * @throws JMSException
     */
    public Message doReceive(boolean fail) throws JMSException {

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = null;
        try {
            consumer = session.createConsumer(destination);
            if (fail) {
                fail("Expected failure due to security constraint.");
            }
        } catch (JMSException e) {
            if (fail && e.getCause() instanceof SecurityException) {
                return null;
            }
            throw e;
        }

        Connection adminConnection = factory.createConnection("system", "manager");
        connections.add(adminConnection);
        Session adminSession = adminConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        sendMessages(adminSession, destination, 1);

        Message m = consumer.receive(1000);
        assertNotNull(m);
        assertEquals("0", ((TextMessage)m).getText());
        assertNull(consumer.receiveNoWait());
        return m;

    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestUserReceiveFails() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST"), new ActiveMQQueue("GUEST.BAR"), new ActiveMQTopic("GUEST.BAR"),});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestInvalidAuthentication() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestUserReceiveSucceeds() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("USERS.FOO"), new ActiveMQTopic("USERS.FOO"),});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestGuestReceiveSucceeds() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("GUEST.BAR"), new ActiveMQTopic("GUEST.BAR"),});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestGuestReceiveFails() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST"), new ActiveMQQueue("USERS.FOO"), new ActiveMQTopic("USERS.FOO"),});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestUserSendSucceeds() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("USERS.FOO"), new ActiveMQQueue("GUEST.BAR"), new ActiveMQTopic("USERS.FOO"),
                                                          new ActiveMQTopic("GUEST.BAR"),});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestUserSendFails() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST"),});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestGuestSendFails() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST"), new ActiveMQQueue("USERS.FOO"), new ActiveMQTopic("USERS.FOO")});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestGuestSendSucceeds() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("GUEST.BAR"), new ActiveMQTopic("GUEST.BAR"),});
    }
}
