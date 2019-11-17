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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.ProducerThread;
import org.apache.activemq.util.Wait;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsSchedulerTest extends JobSchedulerTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsSchedulerTest.class);

    @Test
    public void testCron() throws Exception {
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
                LOG.info("Received scheduled message, waiting for {} more", latch.getCount());
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
        connection.close();
    }

    @Test
    public void testSchedule() throws Exception {
        final int COUNT = 1;
        Connection connection = createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(destination);

        final CountDownLatch latch = new CountDownLatch(COUNT);
        consumer.setMessageListener(new MessageListener() {
            @Override
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
        connection.close();
    }

    @Test
    public void testTransactedSchedule() throws Exception {
        final int COUNT = 1;
        Connection connection = createConnection();

        final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        MessageConsumer consumer = session.createConsumer(destination);

        final CountDownLatch latch = new CountDownLatch(COUNT);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    session.commit();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
                latch.countDown();
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
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(latch.getCount(), 0);
        connection.close();
    }

    @Test
    public void testScheduleRepeated() throws Exception {
        final int NUMBER = 10;
        final AtomicInteger count = new AtomicInteger();
        Connection connection = createConnection();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(destination);

        final CountDownLatch latch = new CountDownLatch(NUMBER);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                count.incrementAndGet();
                latch.countDown();
                LOG.info("Received scheduled message, waiting for {} more", latch.getCount());
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
        connection.close();
    }

    @Test
    public void testScheduleRestart() throws Exception {
        testScheduleRestart(RestartType.NORMAL);
    }

    @Test
    public void testScheduleFullRecoveryRestart() throws Exception {
        testScheduleRestart(RestartType.FULL_RECOVERY);
    }

    @Test
    public void testUpdatesAppliedToIndexBeforeJournalShouldBeDiscarded() throws Exception {
        final int NUMBER_OF_MESSAGES = 1000;
        final AtomicInteger numberOfDiscardedJobs = new AtomicInteger();
        final JobSchedulerStoreImpl jobSchedulerStore = (JobSchedulerStoreImpl) broker.getJobSchedulerStore();
        Location middleLocation = null;

        Appender appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getMessage().toString().contains("Removed Job past last appened in the journal")) {
                    numberOfDiscardedJobs.incrementAndGet();
                }
            }
        };

        registerLogAppender(appender);

        // send a messages
        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();
        MessageProducer producer = session.createProducer(destination);

        for (int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            TextMessage message = session.createTextMessage("test msg");
            long time = 5000;
            message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
            producer.send(message);

            if (NUMBER_OF_MESSAGES / 2 == i) {
                middleLocation = jobSchedulerStore.getJournal().getLastAppendLocation();
            }
        }

        producer.close();

        broker.stop();
        broker.waitUntilStopped();

        // Simulating the case here updates got applied on the index before the journal updates
        jobSchedulerStore.getJournal().setLastAppendLocation(middleLocation);
        jobSchedulerStore.load();

        assertEquals(numberOfDiscardedJobs.get(), NUMBER_OF_MESSAGES / 2);
    }

    private void registerLogAppender(final Appender appender) {
        org.apache.log4j.Logger log4jLogger =
                org.apache.log4j.Logger.getLogger(JobSchedulerStoreImpl.class);
        log4jLogger.addAppender(appender);
        log4jLogger.setLevel(Level.TRACE);
    }

    private void testScheduleRestart(final RestartType restartType) throws Exception {
        // send a message
        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("test msg");
        long time = 5000;
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
        producer.send(message);
        producer.close();

        //restart broker
        restartBroker(restartType);

        // consume the message
        connection = createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(destination);
        Message msg = consumer.receive(10000);
        assertNotNull("Didn't receive the message", msg);

        //send another message
        producer = session.createProducer(destination);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
        producer.send(message);
        producer.close();
        connection.close();
    }

    @Test
    public void testJobSchedulerStoreUsage() throws Exception {

        // Shrink the store limit down so we get the producer to block
        broker.getSystemUsage().getJobSchedulerUsage().setLimit(10 * 1024);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = factory.createConnection();
        connection.start();
        Session sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final long time = 5000;
        final ProducerThread producer = new ProducerThread(sess, destination) {
            @Override
            protected Message createMessage(int i) throws Exception {
                Message message = super.createMessage(i);
                message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time);
                return message;
            }
        };
        producer.setMessageCount(100);
        producer.start();

        MessageConsumer consumer = sess.createConsumer(destination);
        final CountDownLatch latch = new CountDownLatch(100);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                latch.countDown();
            }
        });

        // wait for the producer to block, which should happen immediately, and also wait long
        // enough for the delay to elapse.  We should see no deliveries as the send should block
        // on the first message.
        Thread.sleep(10000l);

        assertEquals(100, latch.getCount());

        // Increase the store limit so the producer unblocks.  Everything should enqueue at this point.
        broker.getSystemUsage().getJobSchedulerUsage().setLimit(1024 * 1024 * 33);

        // Wait long enough that the messages are enqueued and the delivery delay has elapsed.
        Thread.sleep(10000l);

        // Make sure we sent all the messages we expected to send
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return producer.getSentCount() == producer.getMessageCount();
            }
        }, 20000l);

        assertEquals("Producer didn't send all messages", producer.getMessageCount(), producer.getSentCount());

        // Make sure we got all the messages we expected to get
        latch.await(20000l, TimeUnit.MILLISECONDS);

        assertEquals("Consumer did not receive all messages.", 0, latch.getCount());
        connection.close();
    }
}
