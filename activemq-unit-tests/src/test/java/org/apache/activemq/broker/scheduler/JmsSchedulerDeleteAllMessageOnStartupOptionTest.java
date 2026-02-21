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

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ScheduledMessage;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class JmsSchedulerDeleteAllMessageOnStartupOptionTest extends JobSchedulerTestSupport {

    private static final transient Logger LOG = LoggerFactory.getLogger(JmsSchedulerDeleteAllMessageOnStartupOptionTest.class);

    @Override
    protected boolean shouldDeleteAllScheduledMessagesOnStartup() throws Exception {
        return true;
    }

    @Test
    public void testDeleteAllMessageOnRestart() throws Exception {
        // Send a message delayed by 8 seconds
        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();
        long time_ms = 10 * 1000;
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("test msg");
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, time_ms);
        producer.send(message);
        producer.close();
        // Shutdown broker
        restartBroker(RestartType.NORMAL);
        // Make sure the consumer won't get the message
        connection = createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(destination);
        final int COUNT = 1;
        final CountDownLatch latch = new CountDownLatch(COUNT);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                latch.countDown();
            }
        });
        latch.await(20, TimeUnit.SECONDS);
        assertEquals(latch.getCount(), COUNT);
        connection.close();
    }
}
