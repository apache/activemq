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
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Message;
import javax.jms.TopicSubscriber;
import junit.framework.Test;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.MessagePriorityTest;
import org.apache.activemq.store.PersistenceAdapter;
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
            LOG.info("received i=" + i + ", m=" + (msg!=null?
                    msg.getJMSMessageID() + ", priority: " + msg.getJMSPriority()
                    : null) );
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
        addCombinationValues("prioritizeMessages", new Object[] {Boolean.TRUE, Boolean.FALSE});
    }

    public void testConcurrentDurableSubsReconnectWithXLevels() throws Exception {
        ActiveMQTopic topic = (ActiveMQTopic) sess.createTopic("TEST");
        final String subName = "priorityDisconnect";
        TopicSubscriber sub = sess.createDurableSubscriber(topic, subName);
        sub.close();

        final int maxPriority = 5;

        final AtomicInteger[] messageCounts = new AtomicInteger[maxPriority];
        Vector<ProducerThread> producers = new Vector<ProducerThread>();
        for (int priority=0; priority <maxPriority; priority++) {
            producers.add(new ProducerThread(topic, MSG_NUM, priority));
            messageCounts[priority] = new AtomicInteger(0);
        }

        for (ProducerThread producer : producers) {
            producer.start();
        }

        final int closeFrequency = MSG_NUM/2;

        sub = sess.createDurableSubscriber(topic, subName);
        for (int i=0; i < MSG_NUM * maxPriority; i++) {
            Message msg = sub.receive(10000);
            LOG.info("received i=" + i + ", m=" + (msg!=null?
                    msg.getJMSMessageID() + ", priority: " + msg.getJMSPriority()
                    : null) );
            assertNotNull("Message " + i + " was null", msg);
            messageCounts[msg.getJMSPriority()].incrementAndGet();
            if (i > 0 && i % closeFrequency == 0) {
                LOG.info("Closing durable sub.. on: " + i + ", counts: " + Arrays.toString(messageCounts));
                sub.close();
                sub = sess.createDurableSubscriber(topic, subName);
            }
        }
        LOG.info("closing on done!");
        sub.close();

        for (ProducerThread producer : producers) {
            producer.join();
        }
    }

    public static Test suite() {
        return suite(JDBCMessagePriorityTest.class);
    }

}
