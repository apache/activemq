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
package org.apache.activemq.broker.scheduler;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(BlockJUnit4ClassRunner.class)
public class JmsCronSchedulerTest extends EmbeddedBrokerTestSupport {

    @Rule
    public TestName testName = new TestName();

    private static final Logger LOG = LoggerFactory.getLogger(JmsCronSchedulerTest.class);

    @Test
    public void testSimulatenousCron() throws Exception {

        final int COUNT = 10;
        final AtomicInteger count = new AtomicInteger();
        Connection connection = createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(destination);

        final CountDownLatch latch = new CountDownLatch(COUNT);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                count.incrementAndGet();
                latch.countDown();
                assertTrue(message instanceof TextMessage);
                TextMessage tm = (TextMessage) message;
                try {
                    LOG.info("Received [{}] count: {} ", tm.getText(), count.get());
                } catch (JMSException e) {
                    LOG.error("Unexpected exception in onMessage", e);
                    fail("Unexpected exception in onMessage: " + e.getMessage());
                }
            }
        });

        connection.start();
        for (int i = 0; i < COUNT; i++) {
            MessageProducer producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage("test msg "+ i);
            message.setStringProperty(ScheduledMessage.AMQ_SCHEDULED_CRON, "* * * * *");
            producer.send(message);
            LOG.info("Message {} sent at {}", i, new Date().toString());
            producer.close();
            //wait a couple sec so cron start time is different for next message
            Thread.sleep(2000);
        }
        SchedulerBroker sb = (SchedulerBroker) this.broker.getBroker().getAdaptor(SchedulerBroker.class);
        JobScheduler js = sb.getJobScheduler();
        List<Job> list = js.getAllJobs();
        assertEquals(COUNT, list.size());
        latch.await(2, TimeUnit.MINUTES);
        //All should messages should have been received by now
        assertEquals(COUNT, count.get());
    }

    @Test
    public void testCronScheduleWithTtlSet() throws Exception {

        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(destination);
        connection.start();

        MessageProducer producer = session.createProducer(destination);
        producer.setTimeToLive(TimeUnit.MINUTES.toMillis(1));
        TextMessage message = session.createTextMessage("test msg ");
        message.setStringProperty(ScheduledMessage.AMQ_SCHEDULED_CRON, "* * * * *");

        producer.send(message);
        producer.close();

        Thread.sleep(TimeUnit.MINUTES.toMillis(2));

        assertNotNull(consumer.receiveNoWait());
        assertNull(consumer.receiveNoWait());
    }

    @Before
    public void setUp() throws Exception {
        LOG.info("Starting test {}", testName.getMethodName());
        bindAddress = "vm://localhost";
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        return createBroker(true);
    }

    protected BrokerService createBroker(boolean delete) throws Exception {
        File schedulerDirectory = new File("target/scheduler");
        if (delete) {
            IOHelper.mkdirs(schedulerDirectory);
            IOHelper.deleteChildren(schedulerDirectory);
        }
        BrokerService answer = new BrokerService();
        answer.setPersistent(true);
        answer.getManagementContext().setCreateConnector(false);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.setDataDirectory("target");
        answer.setSchedulerDirectoryFile(schedulerDirectory);
        answer.setSchedulerSupport(true);
        answer.setUseJmx(false);
        answer.addConnector(bindAddress);
        return answer;
    }
}
