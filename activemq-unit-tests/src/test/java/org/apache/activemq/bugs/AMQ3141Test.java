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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ3141Test {

    private static final int MAX_MESSAGES = 100;

    private static final long DELAY_IN_MS = 100;

    private static final String QUEUE_NAME = "target.queue";

    private BrokerService broker;

    private final CountDownLatch messageCountDown = new CountDownLatch(MAX_MESSAGES);

    private ConnectionFactory factory;

    @Before
    public void setup() throws Exception {

        broker = new BrokerService();
        broker.setPersistent(true);
        broker.setSchedulerSupport(true);
        broker.setDataDirectory("target");
        broker.setUseJmx(false);
        broker.addConnector("vm://localhost");

        File schedulerDirectory = new File("target/test/ScheduledDB");
        IOHelper.mkdirs(schedulerDirectory);
        IOHelper.deleteChildren(schedulerDirectory);
        broker.setSchedulerDirectoryFile(schedulerDirectory);

        broker.start();
        broker.waitUntilStarted();

        factory = new ActiveMQConnectionFactory("vm://localhost");
    }

    private void sendMessages() throws Exception {
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session.createQueue(QUEUE_NAME));
        for (int i = 0; i < MAX_MESSAGES; i++) {
            Message message = session.createTextMessage();
            message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, DELAY_IN_MS);
            producer.send(message);
        }
        connection.close();
    }

    @Test
    public void testNoMissingMessagesOnShortScheduleDelay() throws Exception {

        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                messageCountDown.countDown();
            }
        });
        sendMessages();

        boolean receiveComplete = messageCountDown.await(5, TimeUnit.SECONDS);

        connection.close();

        assertTrue("expect all messages received but " + messageCountDown.getCount() + " are missing", receiveComplete);
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

}
