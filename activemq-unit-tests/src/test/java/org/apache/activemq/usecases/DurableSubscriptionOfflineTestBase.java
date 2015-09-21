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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport.PersistenceAdapterChoice;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.apache.activemq.store.leveldb.LevelDBPersistenceAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public abstract class DurableSubscriptionOfflineTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionOfflineTestBase.class);
    public boolean usePrioritySupport = Boolean.TRUE;
    public int journalMaxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    public boolean keepDurableSubsActive = true;
    protected BrokerService broker;
    protected ActiveMQTopic topic;
    protected final List<Throwable> exceptions = new ArrayList<Throwable>();
    protected ActiveMQConnectionFactory connectionFactory;
    protected boolean isTopic = true;
    public PersistenceAdapterChoice defaultPersistenceAdapter = PersistenceAdapterChoice.KahaDB;

    @Rule
    public TestName testName = new TestName();

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://" + getName(true));
        connectionFactory.setWatchTopicAdvisories(false);
        return connectionFactory;
    }

    protected Connection createConnection() throws Exception {
        return createConnection("cliName");
    }

    protected Connection createConnection(String name) throws Exception {
        ConnectionFactory connectionFactory1 = createConnectionFactory();
        Connection connection = connectionFactory1.createConnection();
        connection.setClientID(name);
        connection.start();
        return connection;
    }

    public ActiveMQConnectionFactory getConnectionFactory() throws Exception {
        if (connectionFactory == null) {
            connectionFactory = createConnectionFactory();
            assertTrue("Should have created a connection factory!", connectionFactory != null);
        }
        return connectionFactory;
    }

    @Before
    public void setUp() throws Exception {
        exceptions.clear();
        topic = (ActiveMQTopic) createDestination();
        createBroker();
    }

    @After
    public void tearDown() throws Exception {
        destroyBroker();
    }

    protected void createBroker() throws Exception {
        createBroker(true);
    }

    protected void createBroker(boolean deleteAllMessages) throws Exception {
        String currentTestName = getName(true);
        broker = BrokerFactory.createBroker("broker:(vm://" + currentTestName +")");
        broker.setBrokerName(currentTestName);
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        broker.getManagementContext().setCreateConnector(false);
        broker.setAdvisorySupport(false);
        broker.setKeepDurableSubsActive(keepDurableSubsActive);
        broker.addConnector("tcp://0.0.0.0:0");

        if (usePrioritySupport) {
            PolicyEntry policy = new PolicyEntry();
            policy.setPrioritizedMessages(true);
            PolicyMap policyMap = new PolicyMap();
            policyMap.setDefaultEntry(policy);
            broker.setDestinationPolicy(policyMap);
        }

        setDefaultPersistenceAdapter(broker);
        if (broker.getPersistenceAdapter() instanceof JDBCPersistenceAdapter) {
            // ensure it kicks in during tests
            ((JDBCPersistenceAdapter)broker.getPersistenceAdapter()).setCleanupPeriod(2*1000);
        } else if (broker.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter) {
            // have lots of journal files
            ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).setJournalMaxFileLength(journalMaxFileLength);
        }

        configurePlugins(broker);
        broker.start();
        broker.waitUntilStarted();
    }

    public void configurePlugins(BrokerService broker) throws Exception {
    }

    protected void destroyBroker() throws Exception {
        if (broker != null)
            broker.stop();
    }

    protected Destination createDestination(String subject) {
        if (isTopic) {
            return new ActiveMQTopic(subject);
        } else {
            return new ActiveMQQueue(subject);
        }
    }

    protected Destination createDestination() {
        return createDestination(getDestinationString());
    }

    /**
     * Returns the name of the destination used in this test case
     */
    protected String getDestinationString() {
        return getClass().getName() + "." + getName(true);
    }


    public String getName() {
        return getName(false);
    }

    protected String getName(boolean original) {
        String currentTestName = testName.getMethodName();
        currentTestName = currentTestName.replace("[","");
        currentTestName = currentTestName.replace("]","");
        return currentTestName;
    }

    public PersistenceAdapter setDefaultPersistenceAdapter(BrokerService broker) throws IOException {
        return setPersistenceAdapter(broker, defaultPersistenceAdapter);
    }

    public PersistenceAdapter setPersistenceAdapter(BrokerService broker, PersistenceAdapterChoice choice) throws IOException {
        PersistenceAdapter adapter = null;
        switch (choice) {
            case JDBC:
                LOG.debug(">>>> setPersistenceAdapter to JDBC ");
                adapter = new JDBCPersistenceAdapter();
                break;
            case KahaDB:
                LOG.debug(">>>> setPersistenceAdapter to KahaDB ");
                adapter = new KahaDBPersistenceAdapter();
                break;
            case LevelDB:
                LOG.debug(">>>> setPersistenceAdapter to LevelDB ");
                adapter = new LevelDBPersistenceAdapter();
                break;
            case MEM:
                LOG.debug(">>>> setPersistenceAdapter to MEM ");
                adapter = new MemoryPersistenceAdapter();
                break;
        }
        broker.setPersistenceAdapter(adapter);
        return adapter;
    }
}

class DurableSubscriptionOfflineTestListener implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionOfflineTestListener.class);
    int count = 0;
    String id = null;

    DurableSubscriptionOfflineTestListener() {}

    DurableSubscriptionOfflineTestListener(String id) {
        this.id = id;
    }
    @Override
    public void onMessage(javax.jms.Message message) {
        count++;
        if (id != null) {
            try {
                LOG.info(id + ", " + message.getJMSMessageID());
            } catch (Exception ignored) {}
        }
    }
}