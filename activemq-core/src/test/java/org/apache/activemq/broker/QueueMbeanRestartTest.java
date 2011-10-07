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

import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import junit.framework.Test;
import org.apache.activemq.TestSupport;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.JMXSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueMbeanRestartTest extends TestSupport {
    private static final transient Logger LOG = LoggerFactory.getLogger(QueueMbeanRestartTest.class);

    BrokerService broker;

    public static Test suite() {
        return suite(QueueMbeanRestartTest.class);
    }

    public void setUp() throws Exception {
        topic = false;
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        broker.stop();
    }

    public void initCombosForTestMBeanPresenceOnRestart() {
        addCombinationValues("defaultPersistenceAdapter",
                new Object[]{PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.JDBC});
    }

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
        createBroker(false);
    }

    private void verifyPresenceOfQueueMbean() throws Exception {
        for (ObjectName name : broker.getManagementContext().queryNames(null, null)) {
            LOG.info("candidate :" + name);
            String type = name.getKeyProperty("Type");
            if (type != null && type.equals("Queue")) {
                assertEquals(
                        JMXSupport.encodeObjectNamePart(((ActiveMQQueue) createDestination()).getPhysicalName()),
                        name.getKeyProperty("Destination"));
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
        setDefaultPersistenceAdapter(broker);

        broker.setDeleteAllMessagesOnStartup(deleteAll);
        broker.start();
    }
}