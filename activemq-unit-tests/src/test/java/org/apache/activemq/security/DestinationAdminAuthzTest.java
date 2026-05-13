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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@RunWith(value = Parameterized.class)
public class DestinationAdminAuthzTest {

    private static final Logger LOG = LoggerFactory.getLogger(DestinationAdminAuthzTest.class);

    public static final String APP1_USER_NAME = "app1";
    public static final String APP2_USER_NAME = "app2";
    public static final String APP3_USER_NAME = "app3";

    public static final String APP1_GROUP_NAME = "app1Group";
    public static final String APP2_GROUP_NAME = "app2Group";
    public static final String APP3_GROUP_NAME = "app3Group";

    public static final GroupPrincipal ADMINS = new GroupPrincipal("admins");
    public static final GroupPrincipal APP1GROUP = new GroupPrincipal(APP1_GROUP_NAME);
    public static final GroupPrincipal APP2GROUP = new GroupPrincipal(APP2_GROUP_NAME);
    public static final GroupPrincipal APP3GROUP = new GroupPrincipal(APP3_GROUP_NAME);

    public static Principal WILDCARD;
    static {
        try {
            WILDCARD = (Principal) DefaultAuthorizationMap.createGroupPrincipal(
                    "*", GroupPrincipal.class.getName());
        } catch (Exception e) {
            LOG.error("Failed to make wildcard principal", e);
        }
    }

    private BrokerService brokerService = null;
    private String destinationType = null;
    private String userName = null;
    private boolean errorExpected = true;

    @Parameterized.Parameters(name="type={0}, user={1}, error={2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"queue", APP1_USER_NAME, false},
                {"topic", APP1_USER_NAME, false},
                {"temp-queue", APP1_USER_NAME, false},
                {"temp-topic", APP1_USER_NAME, false},
                {"queue", APP2_USER_NAME, false},
                {"topic", APP2_USER_NAME, false},
                {"temp-queue", APP2_USER_NAME, false},
                {"temp-topic", APP2_USER_NAME, false},
                {"queue", APP3_USER_NAME, true},
                {"topic", APP3_USER_NAME, true},
                {"temp-queue", APP3_USER_NAME, true},
                {"temp-topic", APP3_USER_NAME, true}
        });
    }

    public DestinationAdminAuthzTest(String destinationType, String userName, boolean errorExpected) {
        this.destinationType = destinationType;
        this.userName = userName;
        this.errorExpected = errorExpected;
    }

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted(5_000L);
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null && brokerService.isStarted()) {
            LOG.info("Shutting down broker:{}", brokerService.getBrokerName());
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Test
    public void testDestinationPermissions() throws Exception {
        boolean exceptionOccured = false;

        var connectionFactory = new ActiveMQConnectionFactory(brokerService.getVmConnectorURI());
        connectionFactory.setWatchTopicAdvisories(false);

        final String destName = userName + ".in";;
        ActiveMQDestination tmpActiveMQDestination = null;
        switch (destinationType) {
            case "temp-queue": tmpActiveMQDestination = new ActiveMQTempQueue(); break;
            case "temp-topic": tmpActiveMQDestination = new ActiveMQTempTopic(); break;
            case "queue": tmpActiveMQDestination = new ActiveMQQueue(destName); break;
            case "topic": tmpActiveMQDestination = new ActiveMQTopic(destName); break;
            default: throw new IllegalArgumentException("Unsupported destinationType: " + destinationType);
        };
        var activemqDestination = tmpActiveMQDestination;

        try (final var connection = connectionFactory.createConnection(userName, "password");
             final var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            connection.start();

            Destination tmpDestination = null;
            switch (destinationType) {
                case "temp-queue": tmpDestination = session.createTemporaryQueue(); break;
                case "temp-topic": tmpDestination = session.createTemporaryTopic(); break;
                case "queue": tmpDestination = session.createQueue(destName); break;
                case "topic": tmpDestination = session.createTopic(destName); break;
                default: throw new IllegalArgumentException("Unsupported destinationType: " + destinationType);
            };
            final var destination = tmpDestination;

            if(activemqDestination.isTemporary()) {
                switch (destinationType) {
                    case "temp-queue": activemqDestination.setPhysicalName(destination.toString().substring("temp-queue://".length())); break;
                    case "temp-topic": activemqDestination.setPhysicalName(destination.toString().substring("temp-topic://".length())); break;
                }
            }

            var brokerDestination = brokerService.getDestination(activemqDestination);

            try (var messageProducer = session.createProducer(destination);
                 var messageConsumer = session.createConsumer(destination)) {

                messageProducer.send(session.createTextMessage("Test message"));

                Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        try {
                            return brokerService.getDestination(activemqDestination) != null;
                        } catch (Exception e) {
                            return false;
                        }
                    }
                }, 5_000L, 50L);

                var message = messageConsumer.receive(2_000L);

                Wait.waitFor(new Wait.Condition() {
                    @Override
                    public boolean isSatisified() throws Exception {
                        return 1L == brokerDestination.getDestinationStatistics().getDequeues().getCount();
                    }
                }, 5_000L, 50L);

                assertNotNull(message);
                assertTrue(message.propertyExists("JMSXUserID"));
                assertEquals(userName, message.getStringProperty("JMSXUserID"));
                assertEquals("Test message", message.getBody(String.class));
            }

            Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return 0L == brokerDestination.getDestinationStatistics().getConsumers().getCount();
                }
            }, 5_000L, 50L);

            if (!activemqDestination.isTemporary()) {
                ((ActiveMQConnection) connection).destroyDestination(activemqDestination);
            }

            // Check removing a destination that already exists, but does not have authz
            boolean removeExceptionOccured = false;
            try {
                for (var otherActiveMQDestination : Set.of(new ActiveMQQueue("other.in"), new ActiveMQTopic("other.in"))) {
                    ((ActiveMQConnection) connection).destroyDestination(otherActiveMQDestination);
                }
            } catch (JMSException e) {
                removeExceptionOccured = true;
            }
            assertTrue(removeExceptionOccured);

            // Check removing a destination that does not exist does not throw an error
            boolean notExistExceptionOccured = false;
            try {
                for (var noexistActiveMQDestination : Set.of(new ActiveMQQueue("noexist.in"), new ActiveMQTopic("noexist.in"))) {
                    ((ActiveMQConnection) connection).destroyDestination(noexistActiveMQDestination);
                }
            } catch (JMSException e) {
                notExistExceptionOccured = true;
            }
            assertFalse(notExistExceptionOccured);

        } catch (JMSException e) {
            exceptionOccured = true;
        }

        assertEquals(errorExpected, exceptionOccured);
    }

    public static AuthorizationMap createAuthorizationMap() {
        DestinationMap readAccess = new DefaultAuthorizationMap();
        readAccess.put(new ActiveMQQueue(">"), ADMINS);
        readAccess.put(new ActiveMQQueue("app1.>"), APP1GROUP);
        readAccess.put(new ActiveMQQueue("app2.>"), APP2GROUP);
        readAccess.put(new ActiveMQTopic(">"), ADMINS);
        readAccess.put(new ActiveMQTopic("app1.>"), APP1GROUP);
        readAccess.put(new ActiveMQTopic("app2.>"), APP2GROUP);

        // Note: APP3 intentionally has no acccess
        // readAccess.put(new ActiveMQQueue("app3.>"), APP3);
        // readAccess.put(new ActiveMQTopic("app3.>"), APP3);

        DestinationMap writeAccess = new DefaultAuthorizationMap();
        writeAccess.put(new ActiveMQQueue(">"), ADMINS);
        writeAccess.put(new ActiveMQQueue("app1.>"), APP1GROUP);
        writeAccess.put(new ActiveMQQueue("app2.>"), APP2GROUP);
        writeAccess.put(new ActiveMQTopic(">"), ADMINS);
        writeAccess.put(new ActiveMQTopic("app1.>"), APP1GROUP);
        writeAccess.put(new ActiveMQTopic("app2.>"), APP2GROUP);

        DestinationMap adminAccess = new DefaultAuthorizationMap();
        adminAccess.put(new ActiveMQTopic(">"), ADMINS);
        adminAccess.put(new ActiveMQTopic("app1.>"), APP1GROUP);
        adminAccess.put(new ActiveMQTopic("app2.>"), APP2GROUP);
        adminAccess.put(new ActiveMQQueue(">"), ADMINS);
        adminAccess.put(new ActiveMQQueue("app1.>"), APP1GROUP);
        adminAccess.put(new ActiveMQQueue("app2.>"), APP2GROUP);

        readAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), WILDCARD);
        writeAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), WILDCARD);
        adminAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), WILDCARD);

        var authorizationMap = new SimpleAuthorizationMap(writeAccess, readAccess, adminAccess);
        var tempDestinationAuthorizationEntry = new TempDestinationAuthorizationEntry();

        try {
            tempDestinationAuthorizationEntry.setAdmin("admins, app1Group, app2Group");
            tempDestinationAuthorizationEntry.setRead("admins, app1Group, app2Group");
            tempDestinationAuthorizationEntry.setWrite("admins, app1Group, app2Group");
        } catch (Exception e) {
            fail(e.getMessage());
        }

        authorizationMap.setTempDestinationAuthorizationEntry(tempDestinationAuthorizationEntry);
        return authorizationMap;
    }

    public static class SimpleAuthenticationBrokerPlugin implements BrokerPlugin {

        private final Map<String, String> userPasswords;
        private final Map<String, Set<Principal>> userGroups;

        public SimpleAuthenticationBrokerPlugin(final Map<String, String> userPasswords, final Map<String, Set<Principal>> userGroups) {
            this.userPasswords = userPasswords;
            this.userGroups = userGroups;
        }

        public Broker installPlugin(Broker broker) {
            return new SimpleAuthenticationBroker(broker, userPasswords, userGroups);
        }

        public String toString() {
            return "SimpleAuthenticationBroker";
        }
    }

    protected BrokerService createBroker() throws Exception {
        var brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setDestinations(new ActiveMQDestination[] { new ActiveMQQueue("other.in"), new ActiveMQTopic("other.in") });
        brokerService.setPersistent(false);
        brokerService.setPlugins(new BrokerPlugin[] {new AuthorizationPlugin(createAuthorizationMap()), new SimpleAuthenticationBrokerPlugin(createUserPasswords(), createUserGroups())});
        brokerService.setPopulateJMSXUserID(true);
        brokerService.setSchedulerSupport(false);
        brokerService.setUseAuthenticatedPrincipalForJMSXUserID(true);
        brokerService.setUseJmx(true);
        return brokerService;
    }

    private static Map<String, String> createUserPasswords() {
        var users = new HashMap<String, String>();
        users.put("admin", "password");
        users.put(APP1_USER_NAME, "password");
        users.put(APP2_USER_NAME, "password");
        users.put(APP3_USER_NAME, "password");
        return users;
    }

    private static Map<String, Set<Principal>> createUserGroups() {
        var groups = new HashMap<String, Set<Principal>>();
        groups.put("admin", new HashSet<Principal>(Arrays.asList(new Principal[] {new GroupPrincipal("admins")})));
        groups.put(APP1_USER_NAME, new HashSet<Principal>(Arrays.asList(new Principal[] { new GroupPrincipal(APP1_GROUP_NAME) })));
        groups.put(APP2_USER_NAME, new HashSet<Principal>(Arrays.asList(new Principal[] { new GroupPrincipal(APP2_GROUP_NAME) })));
        groups.put(APP3_USER_NAME, new HashSet<Principal>(Arrays.asList(new Principal[] { new GroupPrincipal(APP3_GROUP_NAME) })));
        return groups;
    }
}

