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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Vector;
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
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.MessagePriorityTest;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class JDBCMessagePriorityTest extends MessagePriorityTest {

    private static final Log LOG = LogFactory.getLog(JDBCMessagePriorityTest.class);

    @Override
    protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws Exception {
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        dataSource.setShutdownDatabase("false");
        jdbc.setDataSource(dataSource);
        jdbc.deleteAllMessages();
        jdbc.setCleanupPeriod(2000);
        return jdbc;
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
        Vector<ProducerThread> producers = new Vector<ProducerThread>();
        for (int priority = 0; priority < maxPriority; priority++) {
            producers.add(new ProducerThread(topic, MSG_NUM, priority));
            messageCounts[priority] = new AtomicInteger(0);
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
            assertNull("no duplicate message failed on : " + msg.getJMSMessageID(), dups.put(msg.getJMSMessageID(), subName));
            assertNotNull("Message " + i + " was null", msg);
            messageCounts[msg.getJMSPriority()].incrementAndGet();
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

    public static Test suite() {
        return suite(JDBCMessagePriorityTest.class);
    }

}
