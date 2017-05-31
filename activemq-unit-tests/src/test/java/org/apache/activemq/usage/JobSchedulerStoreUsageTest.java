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

package org.apache.activemq.usage;

import java.io.File;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.ProducerThread;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotEquals;

public class JobSchedulerStoreUsageTest extends EmbeddedBrokerTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JobSchedulerStoreUsageTest.class);

    final int WAIT_TIME_MILLS = 20*1000;

    @Override
    protected BrokerService createBroker() throws Exception {
        File schedulerDirectory = new File("target/scheduler");

        IOHelper.mkdirs(schedulerDirectory);
        IOHelper.deleteChildren(schedulerDirectory);

        BrokerService broker = super.createBroker();
        broker.setSchedulerSupport(true);
        broker.setSchedulerDirectoryFile(schedulerDirectory);
        broker.getSystemUsage().getJobSchedulerUsage().setLimit(7 * 1024);
        broker.deleteAllMessages();
        return broker;
    }

    @Override
    protected boolean isPersistent() {
        return true;
    }

    public void testBlockAndChangeViaJmxReleases() throws Exception {

        LOG.info("Initial scheduler usage: {}", broker.getAdminView().getJobSchedulerStorePercentUsage());

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection conn = factory.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = sess.createQueue(this.getClass().getName());
        final ProducerThread producer = new ProducerThread(sess, dest) {
            @Override
            protected Message createMessage(int i) throws Exception {
                Message message = super.createMessage(i);
                message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, WAIT_TIME_MILLS / 2);
                return message;
            }
        };
        producer.setMessageCount(100);
        producer.start();

        assertEquals(7 * 1024, broker.getAdminView().getJobSchedulerStoreLimit());

        assertTrue("Usage exhausted", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("scheduler store usage %" + broker.getAdminView().getJobSchedulerStorePercentUsage() + " producerSent count:" +  producer.getSentCount());
                return broker.getAdminView().getJobSchedulerStorePercentUsage() > 100;
            }
        }));

        LOG.info("scheduler store usage %" + broker.getAdminView().getJobSchedulerStorePercentUsage() + " producerSent count:" +  producer.getSentCount());

        assertNotEquals("Producer has not sent all messages", producer.getMessageCount(), producer.getSentCount());

        broker.getAdminView().setJobSchedulerStoreLimit(1024 * 1024 * 33);

        LOG.info("scheduler store usage %" + broker.getAdminView().getJobSchedulerStorePercentUsage() + " producerSent count:" +  producer.getSentCount());

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return producer.getSentCount() == producer.getMessageCount();
            }
        });

        assertEquals("Producer sent all messages", producer.getMessageCount(), producer.getSentCount());

        assertTrue(broker.getAdminView().getJobSchedulerStorePercentUsage() < 100);
    }
}