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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.net.URL;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Test;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQMessageTransformation;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Response;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.jaas.GroupPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.*;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

/**
 * Tests that the broker allows/fails access to destinations based on the
 * security policy installed on the broker.
 *
 *
 */
@Category(ParallelTest.class)
public class SimpleSecurityBrokerSystemTest extends SecurityTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleSecurityBrokerSystemTest.class);

    public static final GroupPrincipal GUESTS = new GroupPrincipal("guests");
    public static final GroupPrincipal USERS = new GroupPrincipal("users");
    public static final GroupPrincipal ADMINS = new GroupPrincipal("admins");
    public static Principal WILDCARD;
    static {
        try {
         WILDCARD = (Principal) DefaultAuthorizationMap.createGroupPrincipal(
                 SecurityContext.WILDCARD, GroupPrincipal.class.getName());
        } catch (Exception e) {
            LOG.error("Failed to make wildcard principal", e);
        }
    }

    public BrokerPlugin authorizationPlugin;
    public BrokerPlugin authenticationPlugin;

    static {
        String path = System.getProperty("java.security.auth.login.config");
        if (path == null) {
            URL resource = SimpleSecurityBrokerSystemTest.class.getClassLoader().getResource("login.config");
            if (resource != null) {
                path = resource.getFile();
                System.setProperty("java.security.auth.login.config", path);
            }
        }
        LOG.info("Path to login config: " + path);
    }

    public static Test suite() {
        return suite(SimpleSecurityBrokerSystemTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    /**
     * @throws jakarta.jms.JMSException
     */
    public void testPopulateJMSXUserID() throws Exception {
        destination = new ActiveMQQueue("TEST");
        Connection connection = factory.createConnection("system", "manager");
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        sendMessages(session, destination, 1);

        // make sure that the JMSXUserID is exposed over JMX
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        CompositeData[] browse = (CompositeData[]) mbs.invoke(new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST"), "browse", null, null);
        assertEquals("system", browse[0].get("JMSXUserID"));

        // And also via JMS.
        MessageConsumer consumer = session.createConsumer(destination);
        Message m = consumer.receive(1000);
        assertTrue(m.propertyExists("JMSXUserID"));
        assertEquals("system",  m.getStringProperty("JMSXUserID"));
    }

    public void testComposite() throws Exception {
        // user is authorized for both
        destination = new ActiveMQQueue("USERS.queue.1,USERS.queue.2");
        Connection connection = factory.createConnection("user", "password");
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        sendMessages(session, destination, 1);
        MessageConsumer consumer = session.createConsumer(destination);
        // there should be 2 messages because a copy gets sent to each dest in the composite list
        assertNotNull(consumer.receive(100));
        assertNotNull(consumer.receive(100));
    }

    public void testCompositeFail() throws Exception {
        destination = new ActiveMQQueue("USERS.queue.1,GUEST.queue.2");
        Connection connection = factory.createConnection("guest", "password");
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertThrows(JMSSecurityException.class, () -> sendMessages(session, destination, 1));
    }

    public void testCompositeFailMulti() throws Exception {
        destination = new ActiveMQQueue("GUEST.queue.1,GUEST.queue.2");
        // Add child composite that is not authorized
        Arrays.stream(destination.getCompositeDestinations()).findFirst().orElseThrow().setCompositeDestinations(
                new ActiveMQDestination[]{new ActiveMQQueue("USERS.queue.1")});
        Connection connection = factory.createConnection("guest", "password");
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertThrows(JMSSecurityException.class, () -> sendMessages(session, destination, 1));
    }

    public void testCompositeTempWriteDestsExist() throws Exception {
        // pre-create dests
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("USERS.queue.1"), false);
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("GUEST.queue.1"), false);

        // Test that authorization works when dests already exist
        testCompositeTempWrite();
    }

    public void testCompositeTempWrite() throws Exception {
        // Test that authorization works on composite temp
        destination = new ActiveMQTempQueue("queue://USERS.queue.1,queue://GUEST.queue.1");
        Connection connection = factory.createConnection("guest", "password");
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        JMSSecurityException e = assertThrows(JMSSecurityException.class, () -> sendMessages(session, destination, 1));
        assertTrue(e.getMessage().contains("User guest is not authorized to write to:"));
    }

    public void testCompositeTempMultiLevel() throws Exception {
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("GUEST.queue.1"), false);
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("GUEST.queue.2"), false);
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("USERS.queue.1"), false);

        // Test that authorization works on composite temp
        // The top level composite list would be authorized, verify it fails because one of the
        // composite dests itself contains a dest that isn't authorized
        destination = new ActiveMQTempQueue("queue://GUEST.queue.1,queue://GUEST.queue.2");
        Arrays.stream(destination.getCompositeDestinations()).findFirst().orElseThrow().setCompositeDestinations(
                new ActiveMQDestination[]{new ActiveMQQueue("USERS.queue.1")});
        Connection connection = factory.createConnection("guest", "password");
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        JMSSecurityException e = assertThrows(JMSSecurityException.class, () -> sendMessages(session, destination, 1));
        assertTrue(e.getMessage().contains("User guest is not authorized to write to:"));
    }

    public void testCompositeTempRead() throws Exception {
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("USERS.queue.1"), false);
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("GUEST.queue.1"), false);

        // Test that authorization works on composite temp
        testCompositeTempRead(new ActiveMQTempQueue("queue://USERS.queue.1,queue://GUEST.queue.1"));
    }

    public void testCompositeTempReadMultiLevel() throws Exception {
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("GUEST.queue.1"), false);
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("GUEST.queue.2"), false);
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("USERS.queue.1"), false);

        // test unauthorized dest that is a child of another composite fails
        ActiveMQDestination dest = new ActiveMQTempQueue("queue://GUEST.queue.1,queue://GUEST.queue.2");
                Arrays.stream(dest.getCompositeDestinations()).findFirst().orElseThrow().setCompositeDestinations(
                        new ActiveMQDestination[]{new ActiveMQQueue("USERS.queue.1")});

        // Test that authorization works on composite temp
        testCompositeTempRead(dest);
    }

    private void testCompositeTempRead(ActiveMQDestination dest) throws Exception {
        destination = dest;

        // Test that authorization works on composite temp
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection("guest", "password");
        connections.add(connection);
        connection.start();

        // Consumers should be blocked
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Method nextId = ActiveMQSession.class.getDeclaredMethod("getNextConsumerId");
        nextId.setAccessible(true);

        // the client normally blocks this so bypass the check to test the broker auth
        ConsumerInfo info = new ConsumerInfo((ConsumerId) nextId.invoke(session));
        info.setDestination(destination);
        Object result = connection.getTransport().request(info, 500);
        assertTrue(result instanceof ExceptionResponse);
        assertTrue(((Response) result).isException());
        assertTrue(((ExceptionResponse) result).getException() instanceof SecurityException);
        assertTrue(((ExceptionResponse) result).getException().getMessage()
                .contains("User guest is not authorized to read from"));
    }

    public void testCompositeTempQueueWithTopics() throws Exception {
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQTopic("USERS.topic.1"), false);
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQTopic("GUEST.topic.1"), false);

        // Test that authorization works with contained topic instead of queues
        destination = new ActiveMQTempQueue("topic://USERS.topic.1,topic://GUEST.topic.1");
        Connection connection = factory.createConnection("guest", "password");
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        JMSSecurityException e = assertThrows(JMSSecurityException.class, () -> sendMessages(session, destination, 1));
        assertTrue(e.getMessage().contains("User guest is not authorized to write to:"));
    }

    public void testCompositeTempTopicWithQueues() throws Exception {
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("USERS.queue.1"), false);
        broker.getBroker().addDestination(broker.getAdminConnectionContext(),
                new ActiveMQQueue("GUEST.queue.1"), false);

        // Test that authorization works with contained queues when temp is of type topic
        destination = new ActiveMQTempTopic("queue://USERS.queue.1,queue://GUEST.queue.1");
        Connection connection = factory.createConnection("guest", "password");
        connections.add(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        JMSSecurityException e = assertThrows(JMSSecurityException.class, () -> sendMessages(session, destination, 1));
        assertTrue(e.getMessage().contains("User guest is not authorized to write to:"));
    }

    public static AuthorizationMap createAuthorizationMap() {
        DestinationMap readAccess = new DefaultAuthorizationMap();
        readAccess.put(new ActiveMQQueue(">"), ADMINS);
        readAccess.put(new ActiveMQQueue("USERS.>"), USERS);
        readAccess.put(new ActiveMQQueue("GUEST.>"), GUESTS);
        readAccess.put(new ActiveMQTopic(">"), ADMINS);
        readAccess.put(new ActiveMQTopic("USERS.>"), USERS);
        readAccess.put(new ActiveMQTopic("GUEST.>"), GUESTS);

        DestinationMap writeAccess = new DefaultAuthorizationMap();
        writeAccess.put(new ActiveMQQueue(">"), ADMINS);
        writeAccess.put(new ActiveMQQueue("USERS.>"), USERS);
        writeAccess.put(new ActiveMQQueue("GUEST.>"), USERS);
        writeAccess.put(new ActiveMQQueue("GUEST.>"), GUESTS);
        writeAccess.put(new ActiveMQTopic(">"), ADMINS);
        writeAccess.put(new ActiveMQTopic("USERS.>"), USERS);
        writeAccess.put(new ActiveMQTopic("GUEST.>"), USERS);
        writeAccess.put(new ActiveMQTopic("GUEST.>"), GUESTS);

        readAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.TempQueue"), WILDCARD);
        readAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.TempTopic"), WILDCARD);

        DestinationMap adminAccess = new DefaultAuthorizationMap();
        adminAccess.put(new ActiveMQTopic(">"), ADMINS);
        adminAccess.put(new ActiveMQTopic(">"), USERS);
        adminAccess.put(new ActiveMQTopic(">"), GUESTS);
        adminAccess.put(new ActiveMQQueue(">"), ADMINS);
        adminAccess.put(new ActiveMQQueue(">"), USERS);
        adminAccess.put(new ActiveMQQueue(">"), GUESTS);

        var authorizationMap = new SimpleAuthorizationMap(writeAccess, readAccess, adminAccess);
        var tempDestinationAuthorizationEntry = new TempDestinationAuthorizationEntry();
        try {
            tempDestinationAuthorizationEntry.setAdmin("admins, users, guests");
            tempDestinationAuthorizationEntry.setRead("admins, users, guests");
            tempDestinationAuthorizationEntry.setWrite("admins, users, guests");
        } catch (Exception e) {
            fail(e.getMessage());
        }
        authorizationMap.setTempDestinationAuthorizationEntry(tempDestinationAuthorizationEntry);

        return authorizationMap;

    }

    public static class SimpleAuthenticationFactory implements BrokerPlugin {
        public Broker installPlugin(Broker broker) {

            HashMap<String, String> u = new HashMap<String, String>();
            u.put("system", "manager");
            u.put("user", "password");
            u.put("guest", "password");

            Map<String, Set<Principal>> groups = new HashMap<String, Set<Principal>>();
            groups.put("system", new HashSet<Principal>(Arrays.asList(new Principal[] {ADMINS, USERS})));
            groups.put("user", new HashSet<Principal>(Arrays.asList(new Principal[] {USERS})));
            groups.put("guest", new HashSet<Principal>(Arrays.asList(new Principal[] {GUESTS})));

            return new SimpleAuthenticationBroker(broker, u, groups);
        }

        public String toString() {
            return "SimpleAuthenticationBroker";
        }
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombos() {
        addCombinationValues("authorizationPlugin",
                             new Object[] {new AuthorizationPlugin(createAuthorizationMap())});
        addCombinationValues("authenticationPlugin", new Object[] {new SimpleAuthenticationFactory(),
                                                                   new JaasAuthenticationPlugin()});
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.setPopulateJMSXUserID(true);
        broker.setUseAuthenticatedPrincipalForJMSXUserID(true);
        broker.setPlugins(new BrokerPlugin[] {authorizationPlugin, authenticationPlugin});
        broker.setPersistent(false);
        return broker;
    }

}
