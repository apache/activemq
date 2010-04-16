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

public class JmsSchedulerTest extends EmbeddedBrokerTestSupport {

    public void testCron() throws Exception {
        final int COUNT = 10;
        final AtomicInteger count = new AtomicInteger();
        Connection connection = createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(destination);

        final CountDownLatch latch = new CountDownLatch(COUNT);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                latch.countDown();
                count.incrementAndGet();
            }
        });

        connection.start();
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("test msg");
        long time = 1000;
        message.setStringProperty(ScheduledMessage.AMQ_SCHEDULED_CRON, "* * * * *");
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 500);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, COUNT - 1);

        producer.send(message);
        producer.close();

        Thread.sleep(500);
        SchedulerBroker sb = (SchedulerBroker) this.broker.getBroker().getAdaptor(SchedulerBroker.class);
        JobScheduler js = sb.getJobScheduler();
        List<Job> list = js.getAllJobs();
        assertEquals(1, list.size());
        latch.await(240, TimeUnit.SECONDS);
        assertEquals(COUNT, count.get());
    }

    public void testSchedule() throws Exception {
        final int COUNT = 1;
        Connection connection = createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(destination);

        final CountDownLatch latch = new CountDownLatch(COUNT);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                latch.countDown();
            }
        });

        connection.start();
        long time = 5000;
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("test msg");

        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);

        producer.send(message);
        producer.close();
        // make sure the message isn't delivered early
        Thread.sleep(2000);
        assertEquals(latch.getCount(), COUNT);
        latch.await(5, TimeUnit.SECONDS);
        assertEquals(latch.getCount(), 0);
    }
    
    public void testTransactedSchedule() throws Exception {
        final int COUNT = 1;
        Connection connection = createConnection();

        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        MessageConsumer consumer = session.createConsumer(destination);

        final CountDownLatch latch = new CountDownLatch(COUNT);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                latch.countDown();
                try {
                    session.commit();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        connection.start();
        long time = 5000;
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("test msg");

        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);

        producer.send(message);
        session.commit();
        producer.close();
        // make sure the message isn't delivered early
        Thread.sleep(2000);
        assertEquals(latch.getCount(), COUNT);
        latch.await(5, TimeUnit.SECONDS);
        assertEquals(latch.getCount(), 0);
    }


    public void testScheduleRepeated() throws Exception {
        final int NUMBER = 10;
        final AtomicInteger count = new AtomicInteger();
        Connection connection = createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(destination);

        final CountDownLatch latch = new CountDownLatch(NUMBER);
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                latch.countDown();
                count.incrementAndGet();
            }
        });

        connection.start();
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("test msg");
        long time = 1000;
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 500);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, NUMBER - 1);
        producer.send(message);
        producer.close();
        assertEquals(latch.getCount(), NUMBER);
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
        // wait a little longer - make sure we only get NUMBER of replays
        Thread.sleep(1000);
        assertEquals(NUMBER, count.get());
    }

    @Override
    protected void setUp() throws Exception {
        bindAddress = "vm://localhost";
        super.setUp();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        File schedulerDirectory = new File("target/scheduler");
        IOHelper.mkdirs(schedulerDirectory);
        IOHelper.deleteChildren(schedulerDirectory);
        BrokerService answer = new BrokerService();
        answer.setPersistent(isPersistent());
        answer.setDeleteAllMessagesOnStartup(true);
        answer.setDataDirectory("target");
        answer.setSchedulerDirectoryFile(schedulerDirectory);
        answer.setUseJmx(false);
        answer.addConnector(bindAddress);
        return answer;
    }
}
