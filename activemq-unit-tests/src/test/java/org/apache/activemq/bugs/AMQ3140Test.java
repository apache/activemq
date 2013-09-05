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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.IOHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ3140Test {

    private static final int MESSAGES_PER_THREAD = 100;

    private static final int THREAD_COUNT = 10;

    private BrokerService broker;

    private static final String QUEUE_NAME = "test";

    private static class Sender extends Thread {

        private static final int DELAY = 3000;

        @Override
        public void run() {
            try {
                ConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
                Connection connection = cf.createConnection();
                connection.start();
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
                Message message = session.createTextMessage("test");
                for (int i = 0; i < MESSAGES_PER_THREAD; i++) {
                    message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, DELAY);
                    producer.send(message);
                }
                session.close();
                connection.close();
            } catch (JMSException e) {
                fail(e.getMessage());
            }
        }
    }

    @Before
    public void setup() throws Exception {
        File schedulerDirectory = new File("target/test/ScheduledDB");

        IOHelper.mkdirs(schedulerDirectory);
        IOHelper.deleteChildren(schedulerDirectory);

        broker = new BrokerService();
        broker.setSchedulerSupport(true);
        broker.setPersistent(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setDataDirectory("target");
        broker.setSchedulerDirectoryFile(schedulerDirectory);
        broker.setUseJmx(false);
        broker.addConnector("vm://localhost");

        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test
    public void noMessageLostOnConcurrentScheduling() throws JMSException, InterruptedException {

        final AtomicLong receiveCounter = new AtomicLong();

        ConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                receiveCounter.incrementAndGet();
            }
        });

        List<Sender> senderThreads = new ArrayList<Sender>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            Sender sender = new Sender();
            senderThreads.add(sender);
        }
        for (Sender sender : senderThreads) {
            sender.start();
        }
        for (Sender sender : senderThreads) {
            sender.join();
        }

        // wait until all scheduled messages has been received
        TimeUnit.MINUTES.sleep(2);

        session.close();
        connection.close();

        assertEquals(MESSAGES_PER_THREAD * THREAD_COUNT, receiveCounter.get());
    }

}