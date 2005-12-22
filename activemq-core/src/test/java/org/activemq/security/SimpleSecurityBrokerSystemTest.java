/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activemq.security;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Test;

import org.activemq.JmsTestSupport;
import org.activemq.broker.Broker;
import org.activemq.broker.BrokerService;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ActiveMQQueue;
import org.activemq.command.ActiveMQTopic;
import org.activemq.filter.DestinationMap;
import org.activemq.jaas.GroupPrincipal;

/**
 * Tests that the broker allows/fails access to destinations based on the
 * security policy installed on the broker.
 *
 * @version $Revision$
 */
public class SimpleSecurityBrokerSystemTest extends JmsTestSupport {

    static final GroupPrincipal guests = new GroupPrincipal("guests");
    static final GroupPrincipal users = new GroupPrincipal("users");
    static final GroupPrincipal admins = new GroupPrincipal("admins");

    public ActiveMQDestination destination;
    public SecurityFactory authorizationFactory;
    public SecurityFactory authenticationFactory;

    interface SecurityFactory {
        public  Broker create(Broker broker);
    }

    class SimpleAuthorizationFactory implements SecurityFactory {
        public  Broker create(Broker broker) {

            DestinationMap readAccess = new DestinationMap();
            readAccess.put(new ActiveMQQueue(">"), admins);
            readAccess.put(new ActiveMQQueue("USERS.>"), users);
            readAccess.put(new ActiveMQQueue("GUEST.>"), guests);
            readAccess.put(new ActiveMQTopic(">"), admins);
            readAccess.put(new ActiveMQTopic("USERS.>"), users);
            readAccess.put(new ActiveMQTopic("GUEST.>"), guests);

            DestinationMap writeAccess = new DestinationMap();
            writeAccess.put(new ActiveMQQueue(">"), admins);
            writeAccess.put(new ActiveMQQueue("USERS.>"), users);
            writeAccess.put(new ActiveMQQueue("GUEST.>"), users);
            writeAccess.put(new ActiveMQQueue("GUEST.>"), guests);
            writeAccess.put(new ActiveMQTopic(">"), admins);
            writeAccess.put(new ActiveMQTopic("USERS.>"), users);
            writeAccess.put(new ActiveMQTopic("GUEST.>"), users);
            writeAccess.put(new ActiveMQTopic("GUEST.>"), guests);

            readAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), guests);
            readAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), users);
            writeAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), guests);
            writeAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), users);

            DestinationMap adminAccess = new DestinationMap();
            adminAccess.put(new ActiveMQQueue(">"), admins);
            adminAccess.put(new ActiveMQQueue(">"), users);
            adminAccess.put(new ActiveMQQueue(">"), guests);

            return new SimpleAuthorizationBroker(broker, writeAccess, readAccess, adminAccess);
        }
        public String toString() {
            return "SimpleAuthorizationBroker";
        }
    }

    class SimpleAuthenticationFactory implements SecurityFactory {
        public  Broker create(Broker broker) {

            HashMap u = new HashMap();
            u.put("system", "manager");
            u.put("user", "password");
            u.put("guest", "password");

            HashMap groups = new HashMap();
            groups.put("system", new HashSet(Arrays.asList(new Object[]{admins, users})));
            groups.put("user", new HashSet(Arrays.asList(new Object[]{users})));
            groups.put("guest", new HashSet(Arrays.asList(new Object[]{guests})));

            return new SimpleAuthenticationBroker(broker, u, groups);
        }
        public String toString() {
            return "SimpleAuthenticationBroker";
        }
    }

    static {
        String path = System.getProperty("java.security.auth.login.config");
        if( path == null ) {
            URL resource = SimpleSecurityBrokerSystemTest.class.getResource("login.config");
            if( resource!=null ) {
                path = resource.getFile();
                System.setProperty("java.security.auth.login.config", path);
            }
        }
        System.out.println("Path to login config: "+path);
    }

    class JaasAuthenticationFactory implements SecurityFactory {
        public  Broker create(Broker broker) {
            return new JaasAuthenticationBroker(broker, "activemq-test-domain");
        }
        public String toString() {
            return "JassAuthenticationBroker";
        }
    }

    public static Test suite() {
        return suite(SimpleSecurityBrokerSystemTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombos() {
        addCombinationValues("authorizationFactory", new Object[] {
                new SimpleAuthorizationFactory(),
        });
        addCombinationValues("authenticationFactory", new Object[] {
                new SimpleAuthenticationFactory(),
                new JaasAuthenticationFactory(),
        });
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService() {
            protected Broker addInterceptors(Broker broker) throws IOException {
                broker = super.addInterceptors(broker);
                broker = authorizationFactory.create(broker);
                broker = authenticationFactory.create(broker);
                return broker;
            }
        };
        broker.setPersistent(false);
        return broker;
    }

    public void initCombosForTestUserReceiveFails() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {
                new ActiveMQQueue("TEST"),
                new ActiveMQTopic("TEST"),
                new ActiveMQQueue("GUEST.BAR"),
                new ActiveMQTopic("GUEST.BAR"),
        });
    }
    public void testUserReceiveFails() throws JMSException {
        doReceive(true);
    }


    public void initCombosForTestInvalidAuthentication() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
    }
    public void testInvalidAuthentication() throws JMSException {
        try {
            // No user id
            Connection c= factory.createConnection();
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

    public void initCombosForTestUserReceiveSucceeds() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {
                new ActiveMQQueue("USERS.FOO"),
                new ActiveMQTopic("USERS.FOO"),
        });
    }
    public void testUserReceiveSucceeds() throws JMSException {
        doReceive(false);
    }


    public void initCombosForTestGuestReceiveSucceeds() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {
                new ActiveMQQueue("GUEST.BAR"),
                new ActiveMQTopic("GUEST.BAR"),
        });
    }
    public void testGuestReceiveSucceeds() throws JMSException {
        doReceive(false);
    }

    public void initCombosForTestGuestReceiveFails() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {
                new ActiveMQQueue("TEST"),
                new ActiveMQTopic("TEST"),
                new ActiveMQQueue("USERS.FOO"),
                new ActiveMQTopic("USERS.FOO"),
        });
    }
    public void testGuestReceiveFails() throws JMSException {
        doReceive(true);
    }


    public void initCombosForTestUserSendSucceeds() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {
                new ActiveMQQueue("USERS.FOO"),
                new ActiveMQQueue("GUEST.BAR"),
                new ActiveMQTopic("USERS.FOO"),
                new ActiveMQTopic("GUEST.BAR"),
        });
    }
    public void testUserSendSucceeds() throws JMSException {
        doSend(false);
    }

    public void initCombosForTestUserSendFails() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {
                new ActiveMQQueue("TEST"),
                new ActiveMQTopic("TEST"),
        });
    }
    public void testUserSendFails() throws JMSException {
        doSend(true);
    }


    public void initCombosForTestGuestSendFails() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {
                new ActiveMQQueue("TEST"),
                new ActiveMQTopic("TEST"),
                new ActiveMQQueue("USERS.FOO"),
                new ActiveMQTopic("USERS.FOO"),
        });
    }
    public void testGuestSendFails() throws JMSException {
        doSend(true);
    }

    public void initCombosForTestGuestSendSucceeds() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {
                new ActiveMQQueue("GUEST.BAR"),
                new ActiveMQTopic("GUEST.BAR"),
        });
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
        } catch (JMSException e) {
            // If test is expected to fail, the cause must only be a SecurityException
            // otherwise rethrow the exception
            if (!fail || !(e.getCause() instanceof SecurityException)) {
            	throw e;
            }
        }

        Message m = consumer.receive(1000);
        if(fail)
            assertNull(m);
        else {
            assertNotNull(m);
            assertEquals("0", ((TextMessage)m).getText());
            assertNull(consumer.receiveNoWait());
        }

    }

    /**
     * @throws JMSException
     */
    public void doReceive(boolean fail) throws JMSException {

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer=null;
        try {
            consumer = session.createConsumer(destination);
            if( fail )
                fail("Expected failure due to security constraint.");
        } catch (JMSException e) {
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
        assertEquals("0", ((TextMessage)m).getText());
        assertNull(consumer.receiveNoWait());

    }
}
