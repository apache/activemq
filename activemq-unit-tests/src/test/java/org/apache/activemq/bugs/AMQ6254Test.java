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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.leveldb.LevelDBStoreFactory;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class AMQ6254Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ6254Test.class);

    private static final String KAHADB = "KahaDB";
    private static final String LEVELDB = "LevelDB";

    private BrokerService brokerService;

    private String topicA = "alphabet.a";
    private String topicB = "alphabet.b";

    private String persistenceAdapterName;
    private boolean pluginsEnabled;

    @Parameters(name="{0} -> plugins = {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {KAHADB, true },
                {KAHADB, false },
                {LEVELDB, true },
                {LEVELDB, false },
            });
    }

    public AMQ6254Test(String persistenceAdapterName, boolean pluginsEnabled) {
        this.persistenceAdapterName = persistenceAdapterName;
        this.pluginsEnabled = pluginsEnabled;
    }

    @Test(timeout = 60000)
    public void testReactivateKeepaliveSubscription() throws Exception {
        // Create wild card durable subscription
        Connection connection = createConnection();
        connection.setClientID("cliID");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber subscriber = session.createDurableSubscriber(session.createTopic("alphabet.>"), "alphabet.>");

        // Send message on Topic A
        connection = createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session.createTopic(topicA));
        producer.send(session.createTextMessage("Hello A"));

        // Verify that message is received
        TextMessage message = (TextMessage) subscriber.receive(2000);
        assertNotNull("Message not received.", message);
        assertEquals("Hello A", message.getText());

        assertTrue("Should have only one consumer", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToTopic(topicA).getConsumerCount() == 1;
            }
        }));

        subscriber.close();

        assertTrue("Should have one message consumed", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getProxyToTopic(topicA).getDequeueCount() == 1;
            }
        }));

        connection.close();

        assertTrue("Should have only one destination", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                Destination destA = getDestination(topicA);
                return destA.getDestinationStatistics().getConsumers().getCount() == 1;
            }
        }));

        assertTrue("Should have only one inactive subscription", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
            }
        }));

        // Restart broker
        brokerService.stop();
        brokerService.waitUntilStopped();
        LOG.info("Broker stopped");

        brokerService = createBroker(false);
        brokerService.start();
        brokerService.waitUntilStarted();
        LOG.info("Broker restarted");

        // Recreate wild card durable subscription
        connection = createConnection();
        connection.setClientID("cliID");
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        subscriber = session.createDurableSubscriber(session.createTopic("alphabet.>"), "alphabet.>");

        // Send message on Topic B
        connection = createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(session.createTopic(topicA));
        producer.send(session.createTextMessage("Hello Again A"));

        // Verify both messages are received
        message = (TextMessage) subscriber.receive(2000);
        assertNotNull("Message not received.", message);
        assertEquals("Hello Again A", message.getText());

        // Verify that we still have a single subscription
        assertTrue("Should have only one destination", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                Destination destA = getDestination(topicA);
                return destA.getDestinationStatistics().getConsumers().getCount() == 1;
            }
        }));

        subscriber.close();
        connection.close();
    }

    private Destination getDestination(String topicName) {
        RegionBroker regionBroker = (RegionBroker) brokerService.getRegionBroker();
        TopicRegion topicRegion = (TopicRegion) regionBroker.getTopicRegion();

        Set<Destination> destinations = topicRegion.getDestinations(new ActiveMQTopic(topicName));
        assertEquals(1, destinations.size());

        return destinations.iterator().next();
    }

    private Connection createConnection() throws Exception {
        String connectionURI = brokerService.getTransportConnectors().get(0).getPublishableConnectString();
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(connectionURI);
        return cf.createConnection("system", "manager");
    }

    @Before
    public void setUp() throws Exception {
        brokerService = createBroker(true);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
    }

    protected BrokerService createBroker(boolean deleteAllMessages) throws Exception {
        BrokerService answer = new BrokerService();

        answer.setKeepDurableSubsActive(true);
        answer.setUseJmx(true);
        answer.setPersistent(true);
        answer.setDeleteAllMessagesOnStartup(deleteAllMessages);
        answer.setAdvisorySupport(false);

        switch (persistenceAdapterName) {
            case KAHADB:
                answer.setPersistenceAdapter(new KahaDBPersistenceAdapter());
                break;
            case LEVELDB:
                answer.setPersistenceFactory(new LevelDBStoreFactory());
                break;
        }

        answer.addConnector("tcp://localhost:0");

        if (pluginsEnabled) {
            ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();

            BrokerPlugin authenticationPlugin = configureAuthentication();
            if (authenticationPlugin != null) {
                plugins.add(configureAuthorization());
            }

            BrokerPlugin authorizationPlugin = configureAuthorization();
            if (authorizationPlugin != null) {
                plugins.add(configureAuthentication());
            }

            if (!plugins.isEmpty()) {
                BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
                answer.setPlugins(plugins.toArray(array));
            }
        }

        ActiveMQDestination[] destinations = { new ActiveMQTopic(topicA), new ActiveMQTopic(topicB) };
        answer.setDestinations(destinations);
        return answer;
    }

    protected BrokerPlugin configureAuthentication() throws Exception {
        List<AuthenticationUser> users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser("system", "manager", "users,admins"));
        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);

        return authenticationPlugin;
    }

    protected BrokerPlugin configureAuthorization() throws Exception {

        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> authorizationEntries = new ArrayList<DestinationMapEntry>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setQueue(">");
        entry.setRead("admins");
        entry.setWrite("admins");
        entry.setAdmin("admins");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("USERS.>");
        entry.setRead("users");
        entry.setWrite("users");
        entry.setAdmin("users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("GUEST.>");
        entry.setRead("guests");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic(">");
        entry.setRead("admins");
        entry.setWrite("admins");
        entry.setAdmin("admins");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("USERS.>");
        entry.setRead("users");
        entry.setWrite("users");
        entry.setAdmin("users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("GUEST.>");
        entry.setRead("guests");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("ActiveMQ.Advisory.>");
        entry.setRead("guests,users");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);

        TempDestinationAuthorizationEntry tempEntry = new TempDestinationAuthorizationEntry();
        tempEntry.setRead("admins");
        tempEntry.setWrite("admins");
        tempEntry.setAdmin("admins");

        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap(authorizationEntries);
        authorizationMap.setTempDestinationAuthorizationEntry(tempEntry);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(authorizationMap);

        return authorizationPlugin;
    }

    protected TopicViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Topic,destinationName="+name);
        TopicViewMBean proxy = (TopicViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, TopicViewMBean.class, true);
        return proxy;
    }
}
