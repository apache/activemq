/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.security;

import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.command.ActiveMQDestination;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * 
 * @version $Revision$
 */
public class SecurityTestSupport extends JmsTestSupport {

    public ActiveMQDestination destination;

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
        }
        catch (JMSException e) {
        }

        try {
            // Bad password
            Connection c = factory.createConnection("user", "krap");
            connections.add(c);
            c.start();
            fail("Expected exception.");
        }
        catch (JMSException e) {
        }

        try {
            // Bad userid
            Connection c = factory.createConnection("userkrap", null);
            connections.add(c);
            c.start();
            fail("Expected exception.");
        }
        catch (JMSException e) {
        }
    }

    public void testUserReceiveSucceeds() throws JMSException {
        doReceive(false);
    }

    public void testGuestReceiveSucceeds() throws JMSException {
        doReceive(false);
    }

    public void testGuestReceiveFails() throws JMSException {
        doReceive(true);
    }

    public void testUserSendSucceeds() throws JMSException {
        doSend(false);
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
    public void doSend(boolean fail) throws JMSException {

        Connection adminConnection = factory.createConnection("system", "manager");
        connections.add(adminConnection);

        adminConnection.start();
        Session adminSession = adminConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = adminSession.createConsumer(destination);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try {
            sendMessages(session, destination, 1);
        }
        catch (JMSException e) {
            // If test is expected to fail, the cause must only be a
            // SecurityException
            // otherwise rethrow the exception
            if (!fail || !(e.getCause() instanceof SecurityException)) {
                throw e;
            }
        }

        Message m = consumer.receive(1000);
        if (fail)
            assertNull(m);
        else {
            assertNotNull(m);
            assertEquals("0", ((TextMessage) m).getText());
            assertNull(consumer.receiveNoWait());
        }

    }

    /**
     * @throws JMSException
     */
    public void doReceive(boolean fail) throws JMSException {

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = null;
        try {
            consumer = session.createConsumer(destination);
            if (fail)
                fail("Expected failure due to security constraint.");
        }
        catch (JMSException e) {
            if (fail && e.getCause() instanceof SecurityException)
                return;
            throw e;
        }

        Connection adminConnection = factory.createConnection("system", "manager");
        connections.add(adminConnection);
        Session adminSession = adminConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        sendMessages(adminSession, destination, 1);

        Message m = consumer.receive(1000);
        assertNotNull(m);
        assertEquals("0", ((TextMessage) m).getText());
        assertNull(consumer.receiveNoWait());

    }

}
