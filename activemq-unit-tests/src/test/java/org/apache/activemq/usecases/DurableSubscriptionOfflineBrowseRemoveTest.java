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
package org.apache.activemq.usecases;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.*;


@RunWith(value = Parameterized.class)
public class DurableSubscriptionOfflineBrowseRemoveTest extends DurableSubscriptionOfflineTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionOfflineBrowseRemoveTest.class);

    public static final String IDENTITY = "milly";
    public boolean keepDurableSubsActive;

    @Parameterized.Parameters(name = "PA-{0}.KeepSubsActive-{1}")
    public static Collection<Object[]> getTestParameters() {
        List<Object[]> testParameters = new ArrayList<Object[]>();
        testParameters.add(new Object[]{TestSupport.PersistenceAdapterChoice.KahaDB, Boolean.TRUE});
        testParameters.add(new Object[]{TestSupport.PersistenceAdapterChoice.KahaDB, Boolean.FALSE});

        testParameters.add(new Object[]{TestSupport.PersistenceAdapterChoice.JDBC, Boolean.TRUE});
        testParameters.add(new Object[]{TestSupport.PersistenceAdapterChoice.JDBC, Boolean.FALSE});

        // leveldb needs some work on finding index from green messageId
        return testParameters;
    }

    public DurableSubscriptionOfflineBrowseRemoveTest(TestSupport.PersistenceAdapterChoice adapter, boolean keepDurableSubsActive) {
        this.defaultPersistenceAdapter = adapter;
        this.usePrioritySupport = true;
        this.keepDurableSubsActive = keepDurableSubsActive;
    }

    @Override
    public void configurePlugins(BrokerService brokerService) throws Exception {
        List<DestinationMapEntry> authorizationEntries = new ArrayList<>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setTopic(">");
        entry.setRead(IDENTITY);
        entry.setWrite(IDENTITY);
        entry.setAdmin(IDENTITY);
        authorizationEntries.add(entry);

        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap(authorizationEntries);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(authorizationMap);

        List<AuthenticationUser> users = new ArrayList<>();
        users.add(new AuthenticationUser(IDENTITY, IDENTITY, IDENTITY));

        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);


        broker.setPlugins(new BrokerPlugin[]{authenticationPlugin, authorizationPlugin});

    }
    @Override
    public PersistenceAdapter setDefaultPersistenceAdapter(BrokerService broker) throws IOException {
        broker.setKeepDurableSubsActive(keepDurableSubsActive);
        return super.setPersistenceAdapter(broker, defaultPersistenceAdapter);
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://" + getName(true));
        connectionFactory.setUserName(IDENTITY);
        connectionFactory.setPassword(IDENTITY);
        connectionFactory.setWatchTopicAdvisories(false);
        return connectionFactory;
    }

    @Test(timeout = 60 * 1000)
    public void testBrowseRemoveBrowseOfflineSub() throws Exception {
        // create durable subscription
        Connection con = createConnection();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId");
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        for (int i = 0; i < 10; i++) {
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        session.close();
        con.close();

        // browse the durable sub
        ObjectName[] subs = broker.getAdminView().getInactiveDurableTopicSubscribers();
        assertEquals(1, subs.length);
        ObjectName subName = subs[0];
        DurableSubscriptionViewMBean sub = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(subName, DurableSubscriptionViewMBean.class, true);
        CompositeData[] data  = sub.browse();
        assertNotNull(data);
        assertEquals(10, data.length);

        LinkedList<String> idToRemove = new LinkedList<>();
        idToRemove.add((String)data[5].get("JMSMessageID"));
        idToRemove.add((String)data[9].get("JMSMessageID"));
        idToRemove.add((String)data[0].get("JMSMessageID"));

        LOG.info("Removing: " + idToRemove);
        for (String id: idToRemove) {
            sub.removeMessage(id);
        }

        if (defaultPersistenceAdapter.compareTo(TestSupport.PersistenceAdapterChoice.JDBC) == 0) {
            for (int i=0; i<10; i++) {
                // each iteration does one priority
                ((JDBCPersistenceAdapter)broker.getPersistenceAdapter()).cleanup();
            }
        }

        data  = sub.browse();
        assertNotNull(data);
        assertEquals(7, data.length);

        for (CompositeData c: data) {
            String id = (String)c.get("JMSMessageID");
            for (String removedId : idToRemove) {
                assertNotEquals(id, removedId);
            }
        }

        // remove non existent
        LOG.info("Repeat remove: " + idToRemove.getFirst());
        sub.removeMessage(idToRemove.getFirst());

    }
}
