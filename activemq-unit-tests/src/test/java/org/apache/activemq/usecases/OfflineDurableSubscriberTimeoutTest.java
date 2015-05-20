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

import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Topic;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.Wait;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OfflineDurableSubscriberTimeoutTest extends org.apache.activemq.TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(OfflineDurableSubscriberTimeoutTest.class);
    private BrokerService broker;

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://" + getName(true));
        connectionFactory.setWatchTopicAdvisories(false);
        return connectionFactory;
    }

    @Override
    protected Connection createConnection() throws Exception {
        return createConnection("id");
    }

    protected Connection createConnection(String name) throws Exception {
        Connection con = getConnectionFactory().createConnection();
        con.setClientID(name);
        con.start();
        return con;
    }

    public static Test suite() {
        return suite(OfflineDurableSubscriberTimeoutTest.class);
    }

    @Override
    protected void setUp() throws Exception {
        createBroker();
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        destroyBroker();
    }

    private void createBroker() throws Exception {
        createBroker(true);
    }

    private void createBroker(boolean deleteAllMessages) throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://" + getName(true) + ")");
        broker.setBrokerName(getName(true));
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        broker.getManagementContext().setCreateConnector(false);
        broker.setAdvisorySupport(false);

        setDefaultPersistenceAdapter(broker);

        ((KahaDBPersistenceAdapter) broker.getPersistenceAdapter()).getStore().getPageFile().setPageSize(1024);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setExpireMessagesPeriod(1000);

        policyEntry.setProducerFlowControl(true);
        policyMap.put(new ActiveMQTopic(">"), policyEntry);
        broker.setDestinationPolicy(policyMap);

        broker.setOfflineDurableSubscriberTaskSchedule(1000);
        broker.setOfflineDurableSubscriberTimeout(2004);

        broker.setDestinations(new ActiveMQDestination[]{
                new ActiveMQTopic("topic1")
        });

        broker.start();
    }

    private void destroyBroker() throws Exception {
        if (broker != null)
            broker.stop();
    }

    public void testOfflineDurableSubscriberTimeout() throws Exception {

        final AtomicBoolean foundLogMessage = new AtomicBoolean(false);
        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().isGreaterOrEqual(Level.WARN)) {
                    LOG.info("received unexpected log message: " + event.getMessage());
                    foundLogMessage.set(true);
                }
            }
        };

        org.apache.log4j.Logger log4jLoggerMRB =
                org.apache.log4j.Logger.getLogger(ManagedRegionBroker.class);
        org.apache.log4j.Logger log4jLoggerT =
                org.apache.log4j.Logger.getLogger(org.apache.activemq.broker.region.Topic.class);

        log4jLoggerMRB.addAppender(appender);
        log4jLoggerT.addAppender(appender);

        try {

            createOfflineDurableSubscribers("topic_new");

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    LOG.info("broker.getAdminView().getInactiveDurableTopicSubscribers():" + broker.getAdminView().getInactiveDurableTopicSubscribers().length);
                    return broker.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
                }
            }));
            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return broker.getAdminView().getInactiveDurableTopicSubscribers().length == 0;
                }
            }));


            broker.stop();
            broker.waitUntilStopped();

            createBroker(false);
            broker.waitUntilStarted();

            createOfflineDurableSubscribers("topic_new");

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return broker.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
                }
            }));

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return broker.getAdminView().getInactiveDurableTopicSubscribers().length == 0;
                }
            }));

            LOG.info("Create Consumer for topic1");
            //create connection to topic that is experiencing warning
            createOfflineDurableSubscribers("topic1");


            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return broker.getAdminView().getInactiveDurableTopicSubscribers().length == 1;
                }
            }));

            assertTrue(Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return broker.getAdminView().getInactiveDurableTopicSubscribers().length == 0;
                }
            }));

            assertFalse("have not found any log warn/error", foundLogMessage.get());
        } finally {
            log4jLoggerMRB.removeAppender(appender);
            log4jLoggerT.removeAppender(appender);
        }
    }

    private void createOfflineDurableSubscribers(String topic) throws Exception {
        Connection con = createConnection();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber((Topic) createDestination(topic), "sub1", null, true);
        session.close();
        con.close();
    }
}