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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.IOHelper;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LostScheduledMessagesTest {

    private BrokerService broker;

    private static final File schedulerDirectory = new File("target/test/ScheduledDB");
    private static final File messageDirectory = new File("target/test/MessageDB");
    private static final String QUEUE_NAME = "test";

    @Before
    public void setup() throws Exception {
        IOHelper.mkdirs(schedulerDirectory);
        IOHelper.deleteChildren(schedulerDirectory);

        IOHelper.mkdirs(messageDirectory);
        IOHelper.deleteChildren(messageDirectory);
    }

    private void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setSchedulerSupport(true);
        broker.setPersistent(true);
        broker.setDeleteAllMessagesOnStartup(false);
        broker.setDataDirectory("target");
        broker.setSchedulerDirectoryFile(schedulerDirectory);
        broker.setDataDirectoryFile(messageDirectory);
        broker.setUseJmx(false);
        broker.addConnector("vm://localhost");
        broker.start();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        BasicConfigurator.resetConfiguration();
    }

    @Test
    public void MessagePassedNotUsingScheduling() throws Exception {
        doTest(false);
    }

    @Test
    public void MessageLostWhenUsingScheduling() throws Exception {
        doTest(true);
    }

    private void doTest(boolean useScheduling) throws Exception {

        int DELIVERY_DELAY_MS = 5000;

        startBroker();

        long startTime = System.currentTimeMillis();

        // Send a message scheduled for delivery in 5 seconds
        ConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
        Message message = session.createTextMessage("test");
        if (useScheduling) {
            message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, DELIVERY_DELAY_MS);
        }
        producer.send(message);

        session.close();
        connection.close();

        broker.getServices();

        // shut down broker
        broker.stop();
        broker.waitUntilStopped();

        // Make sure that broker have stopped within delivery delay
        long shutdownTime = System.currentTimeMillis();
        assertTrue("Failed to shut down broker in expected time. Test results inconclusive", shutdownTime - startTime < DELIVERY_DELAY_MS);

        // make sure that delivery falls into down time window
        TimeUnit.MILLISECONDS.sleep(DELIVERY_DELAY_MS);

        // Start new broker instance
        startBroker();

        final AtomicLong receiveCounter = new AtomicLong();

        cf = new ActiveMQConnectionFactory("vm://localhost");
        connection = cf.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
                receiveCounter.incrementAndGet();
            }
        });

        // Wait for a while to let MQ process the message
        TimeUnit.MILLISECONDS.sleep(DELIVERY_DELAY_MS * 2);

        session.close();
        connection.close();

        assertEquals(1, receiveCounter.get());
    }
}
