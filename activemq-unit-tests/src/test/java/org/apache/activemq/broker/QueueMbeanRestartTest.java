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
package org.apache.activemq.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;

import org.apache.activemq.TestSupport;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.JMXSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(value = Parameterized.class)
public class QueueMbeanRestartTest extends TestSupport {
    private static final transient Logger LOG = LoggerFactory.getLogger(QueueMbeanRestartTest.class);

    BrokerService broker;

    private final TestSupport.PersistenceAdapterChoice persistenceAdapterChoice;

    @Parameterized.Parameters
    public static Collection<TestSupport.PersistenceAdapterChoice[]> getTestParameters() {
        TestSupport.PersistenceAdapterChoice[] kahaDb = {TestSupport.PersistenceAdapterChoice.KahaDB};
        TestSupport.PersistenceAdapterChoice[] levelDb = {TestSupport.PersistenceAdapterChoice.LevelDB};
        TestSupport.PersistenceAdapterChoice[] jdbc = {TestSupport.PersistenceAdapterChoice.JDBC};
        List<TestSupport.PersistenceAdapterChoice[]> choices = new ArrayList<TestSupport.PersistenceAdapterChoice[]>();
        choices.add(kahaDb);
        choices.add(levelDb);
        choices.add(jdbc);

        return choices;
    }

    public QueueMbeanRestartTest(TestSupport.PersistenceAdapterChoice choice) {
        this.persistenceAdapterChoice = choice;
    }

    @Before
    public void setUp() throws Exception {
        topic = false;
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        broker.stop();
    }

    @Test(timeout = 60000)
    public void testMBeanPresenceOnRestart() throws Exception {
        createBroker(true);

        sendMessages();
        verifyPresenceOfQueueMbean();
        LOG.info("restart....");

        restartBroker();
        verifyPresenceOfQueueMbean();
    }

    private void restartBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        Thread.sleep(5 * 1000);
        createBroker(false);
        broker.waitUntilStarted();
    }

    private void verifyPresenceOfQueueMbean() throws Exception {
        for (ObjectName name : broker.getManagementContext().queryNames(null, null)) {
            LOG.info("candidate :" + name);
            String type = name.getKeyProperty("destinationType");
            if (type != null && type.equals("Queue")) {
                assertEquals(
                        JMXSupport.encodeObjectNamePart(((ActiveMQQueue) createDestination()).getPhysicalName()),
                        name.getKeyProperty("destinationName"));
                LOG.info("found mbbean " + name);
                return;
            }
        }
        fail("expected to find matching queue mbean for: " + createDestination());
    }

    private void sendMessages() throws Exception {
        Session session = createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(createDestination());
        producer.send(session.createTextMessage());
    }

    private void createBroker(boolean deleteAll) throws Exception {
        broker = new BrokerService();
        setPersistenceAdapter(broker, persistenceAdapterChoice);

        broker.setDeleteAllMessagesOnStartup(deleteAll);
        broker.start();
    }
}