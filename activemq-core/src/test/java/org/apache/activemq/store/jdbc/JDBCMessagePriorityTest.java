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

package org.apache.activemq.store.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;
import junit.framework.Test;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.MessagePriorityTest;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.util.ThreadTracker;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class JDBCMessagePriorityTest extends MessagePriorityTest {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCMessagePriorityTest.class);
    EmbeddedDataSource dataSource;
    JDBCPersistenceAdapter jdbc;

    @Override
    protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws Exception {
        jdbc = new JDBCPersistenceAdapter();
        dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        dataSource.setShutdownDatabase(null);
        jdbc.setDataSource(dataSource);
        jdbc.deleteAllMessages();
        jdbc.setCleanupPeriod(2000);
        return jdbc;
    }


    protected void tearDown() throws Exception {
       super.tearDown();
       try {
            if (dataSource != null) {
                // ref http://svn.apache.org/viewvc/db/derby/code/trunk/java/testing/org/apache/derbyTesting/junit/JDBCDataSource.java?view=markup
                dataSource.setShutdownDatabase("shutdown");
                dataSource.getConnection();
           }
       } catch (Exception ignored) {
       } finally {
            dataSource.setShutdownDatabase(null);
       }

    }

    // this cannot be a general test as kahaDB just has support for 3 priority levels
    public void testDurableSubsReconnectWithFourLevels() throws Exception {
        ActiveMQTopic topic = (ActiveMQTopic) sess.createTopic("TEST");
        final String subName = "priorityDisconnect";
        TopicSubscriber sub = sess.createDurableSubscriber(topic, subName);
        sub.close();

        final int MED_PRI = LOW_PRI + 1;
        final int MED_HIGH_PRI = HIGH_PRI - 1;

        ProducerThread lowPri = new ProducerThread(topic, MSG_NUM, LOW_PRI);
        ProducerThread medPri = new ProducerThread(topic, MSG_NUM, MED_PRI);
        ProducerThread medHighPri = new ProducerThread(topic, MSG_NUM, MED_HIGH_PRI);
        ProducerThread highPri = new ProducerThread(topic, MSG_NUM, HIGH_PRI);

        lowPri.start();
        highPri.start();
        medPri.start();
        medHighPri.start();

        lowPri.join();
        highPri.join();
        medPri.join();
        medHighPri.join();


        final int closeFrequency = MSG_NUM;
        final int[] priorities = new int[]{HIGH_PRI, MED_HIGH_PRI, MED_PRI, LOW_PRI};
        sub = sess.createDurableSubscriber(topic, subName);
        for (int i = 0; i < MSG_NUM * 4; i++) {
            Message msg = sub.receive(10000);
            LOG.debug("received i=" + i + ", m=" + (msg != null ?
                    msg.getJMSMessageID() + ", priority: " + msg.getJMSPriority()
                    : null));
            assertNotNull("Message " + i + " was null", msg);
            assertEquals("Message " + i + " has wrong priority", priorities[i / MSG_NUM], msg.getJMSPriority());
            if (i > 0 && i % closeFrequency == 0) {
                LOG.info("Closing durable sub.. on: " + i);
                sub.close();
                sub = sess.createDurableSubscriber(topic, subName);
            }
        }
        LOG.info("closing on done!");
        sub.close();
    }

    public void initCombosForTestConcurrentDurableSubsReconnectWithXLevels() {
        addCombinationValues("prioritizeMessages", new Object[]{Boolean.TRUE, Boolean.FALSE});
    }

    public void testConcurrentDurableSubsReconnectWithXLevels() throws Exception {
        ActiveMQTopic topic = (ActiveMQTopic) sess.createTopic("TEST");
        final String subName = "priorityDisconnect";
        Connection consumerConn = factory.createConnection();
        consumerConn.setClientID("priorityDisconnect");
        consumerConn.start();
        Session consumerSession = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        TopicSubscriber sub = consumerSession.createDurableSubscriber(topic, subName);
        sub.close();

        final int maxPriority = 5;

        final AtomicInteger[] messageCounts = new AtomicInteger[maxPriority];
        final long[] messageIds = new long[maxPriority];
        Vector<ProducerThread> producers = new Vector<ProducerThread>();
        for (int priority = 0; priority < maxPriority; priority++) {
            producers.add(new ProducerThread(topic, MSG_NUM, priority));
            messageCounts[priority] = new AtomicInteger(0);
            messageIds[priority] = 1l;
        }

        for (ProducerThread producer : producers) {
            producer.start();
        }

        final int closeFrequency = MSG_NUM / 2;
        HashMap dups = new HashMap();
        sub = consumerSession.createDurableSubscriber(topic, subName);
        for (int i = 0; i < MSG_NUM * maxPriority; i++) {
            Message msg = sub.receive(10000);
            LOG.debug("received i=" + i + ", m=" + (msg != null ?
                    msg.getJMSMessageID() + ", priority: " + msg.getJMSPriority()
                    : null));
            assertNotNull("Message " + i + " was null, counts: " + Arrays.toString(messageCounts), msg);
            assertNull("no duplicate message failed on : " + msg.getJMSMessageID(), dups.put(msg.getJMSMessageID(), subName));
            messageCounts[msg.getJMSPriority()].incrementAndGet();
            assertEquals("message is in order : " + msg,
                    messageIds[msg.getJMSPriority()],((ActiveMQMessage)msg).getMessageId().getProducerSequenceId());
            messageIds[msg.getJMSPriority()]++;
            if (i > 0 && i % closeFrequency == 0) {
                LOG.info("Closing durable sub.. on: " + i + ", counts: " + Arrays.toString(messageCounts));
                sub.close();
                sub = consumerSession.createDurableSubscriber(topic, subName);
            }
        }
        LOG.info("closing on done!");
        sub.close();
        consumerSession.close();
        consumerConn.close();

        for (ProducerThread producer : producers) {
            producer.join();
        }
    }

    public void initCombosForTestConcurrentRate() {
        addCombinationValues("prefetchVal", new Object[]{new Integer(1), new Integer(500)});
    }

    public void testConcurrentRate() throws Exception {
        ActiveMQTopic topic = (ActiveMQTopic) sess.createTopic("TEST");
        final String subName = "priorityConcurrent";
        Connection consumerConn = factory.createConnection();
        consumerConn.setClientID("subName");
        consumerConn.start();
        Session consumerSession = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber sub = consumerSession.createDurableSubscriber(topic, subName);
        sub.close();

        final int TO_SEND = 2000;
        final Vector<Message> duplicates = new Vector<Message>();
        final int[] dups = new int[TO_SEND * 4];
        long start;
        double max = 0, sum = 0;
        MessageProducer messageProducer = sess.createProducer(topic);
        TextMessage message = sess.createTextMessage();
        for (int i = 0; i < TO_SEND; i++) {
            int priority = i % 10;
            message.setText(i + "-" + priority);
            message.setIntProperty("seq", i);
            message.setJMSPriority(priority);
            if (i > 0 && i % 1000 == 0) {
                LOG.info("Max send time: " + max + ". Sending message: " + message.getText());
            }
            start = System.currentTimeMillis();
            messageProducer.send(message, DeliveryMode.PERSISTENT, message.getJMSPriority(), 0);
            long duration = System.currentTimeMillis() - start;
            max = Math.max(max, duration);
            if (duration == max) {
                LOG.info("new max: " + max + " on i=" + i + ", " + message.getText());
            }
            sum += duration;
        }

        LOG.info("Sent: " + TO_SEND + ", max send time: " + max);

        double noConsumerAve = (sum * 100 / TO_SEND);
        sub = consumerSession.createDurableSubscriber(topic, subName);
        final AtomicInteger count = new AtomicInteger();
        sub.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                try {
                    count.incrementAndGet();
                    if (count.get() % 100 == 0) {
                        LOG.info("onMessage: count: " + count.get() + ", " + ((TextMessage) message).getText() + ", seqNo " + message.getIntProperty("seq") + ", " + message.getJMSMessageID());
                    }
                    int seqNo = message.getIntProperty("seq");
                    if (dups[seqNo] == 0) {
                        dups[seqNo] = 1;
                    } else {
                        LOG.error("Duplicate: " + ((TextMessage) message).getText() + ", " + message.getJMSMessageID());
                        duplicates.add(message);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        LOG.info("Activated consumer");
        sum = max = 0;
        for (int i = TO_SEND; i < (TO_SEND * 2); i++) {
            int priority = i % 10;
            message.setText(i + "-" + priority);
            message.setIntProperty("seq", i);
            message.setJMSPriority(priority);
            if (i > 0 && i % 1000 == 0) {
                LOG.info("Max send time: " + max + ". Sending message: " + message.getText());
            }
            start = System.currentTimeMillis();
            messageProducer.send(message, DeliveryMode.PERSISTENT, message.getJMSPriority(), 0);
            long duration = System.currentTimeMillis() - start;
            max = Math.max(max, duration);
            if (duration == max) {
                LOG.info("new max: " + max + " on i=" + i + ", " + message.getText());
            }
            sum += duration;
        }
        LOG.info("Sent another: " + TO_SEND + ", max send time: " + max);

        double withConsumerAve = (sum * 100 / TO_SEND);
        final int reasonableMultiplier = 4; // not so reasonable, but on slow disks it can be
        assertTrue("max X times as slow with consumer:" + withConsumerAve + " , noConsumerMax:" + noConsumerAve,
                withConsumerAve < noConsumerAve * reasonableMultiplier);
        Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                LOG.info("count: " + count.get());
                return TO_SEND * 2 == count.get();
            }
        }, 60 * 1000);

        assertTrue("No duplicates : " + duplicates, duplicates.isEmpty());
        assertEquals("got all messages", TO_SEND * 2, count.get());
    }

    public void testCleanupPriorityDestination() throws Exception {
        assertEquals("no messages pending", 0, messageTableCount());

        ActiveMQTopic topic = (ActiveMQTopic) sess.createTopic("TEST");
        final String subName = "priorityConcurrent";
        Connection consumerConn = factory.createConnection();
        consumerConn.setClientID("subName");
        consumerConn.start();
        Session consumerSession = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber sub = consumerSession.createDurableSubscriber(topic, subName);
        sub.close();

        MessageProducer messageProducer = sess.createProducer(topic);
        Message message = sess.createTextMessage();
        message.setJMSPriority(2);
        messageProducer.send(message, DeliveryMode.PERSISTENT, message.getJMSPriority(), 0);
        message.setJMSPriority(5);
        messageProducer.send(message, DeliveryMode.PERSISTENT, message.getJMSPriority(), 0);

        assertEquals("two messages pending", 2, messageTableCount());

        sub = consumerSession.createDurableSubscriber(topic, subName);

        message = sub.receive(5000);
        assertEquals("got high priority", 5, message.getJMSPriority());

        waitForAck(5);

        for (int i=0; i<10; i++) {
            jdbc.cleanup();
        }
        assertEquals("one messages pending", 1, messageTableCount());

        message = sub.receive(5000);
        assertEquals("got high priority", 2, message.getJMSPriority());

        waitForAck(2);

        for (int i=0; i<10; i++) {
            jdbc.cleanup();
        }
        assertEquals("no messages pending", 0, messageTableCount());
    }


    public void testCleanupNonPriorityDestination() throws Exception {
        assertEquals("no messages pending", 0, messageTableCount());

        ActiveMQTopic topic = (ActiveMQTopic) sess.createTopic("TEST_CLEANUP_NO_PRIORITY");
        final String subName = "subName";
        Connection consumerConn = factory.createConnection();
        consumerConn.setClientID("subName");
        consumerConn.start();
        Session consumerSession = consumerConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber sub = consumerSession.createDurableSubscriber(topic, subName);
        sub.close();

        MessageProducer messageProducer = sess.createProducer(topic);
        Message message = sess.createTextMessage("ToExpire");
        messageProducer.send(message, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, 4000);

        message = sess.createTextMessage("A");
        messageProducer.send(message);
        message = sess.createTextMessage("B");
        messageProducer.send(message);
        message = null;

        assertEquals("three messages pending", 3, messageTableCount());

        // let first message expire
        TimeUnit.SECONDS.sleep(5);

        sub = consumerSession.createDurableSubscriber(topic, subName);
        message = sub.receive(5000);
        assertNotNull("got message", message);
        LOG.info("Got: " + message);

        waitForAck(0, 1);

        for (int i=0; i<10; i++) {
            jdbc.cleanup();
        }
        assertEquals("one messages pending", 1, messageTableCount());

        message = sub.receive(5000);
        assertNotNull("got message two", message);
        LOG.info("Got: " + message);

        waitForAck(0, 2);

        for (int i=0; i<10; i++) {
            jdbc.cleanup();
        }
        assertEquals("no messages pending", 0, messageTableCount());
    }

    private int messageTableCount() throws Exception {
        int count = -1;
        java.sql.Connection c = dataSource.getConnection();
        try {
            PreparedStatement s = c.prepareStatement("SELECT COUNT(*) FROM ACTIVEMQ_MSGS");
            ResultSet rs = s.executeQuery();
            if (rs.next()) {
                count = rs.getInt(1);
            }
        } finally {
            if (c!=null) {
                c.close();
            }
        }
        return count;
    }

    private void waitForAck(final int priority) throws Exception {
        waitForAck(priority, 0);
    }

    private void waitForAck(final int priority, final int minId) throws Exception {
       assertTrue("got ack for " + priority, Wait.waitFor(new Wait.Condition() {
           @Override
           public boolean isSatisified() throws Exception {
               int id = 0;
               java.sql.Connection c = dataSource.getConnection();
               try {
                    PreparedStatement s = c.prepareStatement("SELECT LAST_ACKED_ID FROM ACTIVEMQ_ACKS WHERE PRIORITY=" + priority);
                    ResultSet rs = s.executeQuery();
                    if (rs.next()) {
                        id = rs.getInt(1);
                    }
                } finally {
                    if (c!=null) {
                        c.close();
                    }
                }
               return id>minId;
           }
       }));
    }

    private int messageTableDump() throws Exception {
        int count = -1;
        java.sql.Connection c = dataSource.getConnection();
        try {
            PreparedStatement s = c.prepareStatement("SELECT * FROM ACTIVEMQ_MSGS");
            ResultSet rs = s.executeQuery();
            if (rs.next()) {
                count = rs.getInt(1);
            }
        } finally {
            if (c!=null) {
                c.close();
            }
        }
        return count;
    }

    public static Test suite() {
        return suite(JDBCMessagePriorityTest.class);
    }

}
