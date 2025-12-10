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
package org.apache.activemq;

import java.util.concurrent.CountDownLatch;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category(ParallelTest.class)
public class JmsSendWithAsyncCallbackTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(JmsSendWithAsyncCallbackTest.class);

    private Connection connection;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        connection = createConnection();
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    public void testAsyncCallbackIsFaster() throws JMSException, InterruptedException {
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getName());

        // setup a consumer to drain messages..
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
            }
        });

        // warmup...
        for (int i = 0; i < 10; i++) {
            benchmarkNonCallbackRate();
            benchmarkCallbackRate();
        }

        double callbackRate = benchmarkCallbackRate();
        double nonCallbackRate = benchmarkNonCallbackRate();

        LOG.info(String.format("AsyncCallback Send rate: %,.2f m/s", callbackRate));
        LOG.info(String.format("NonAsyncCallback Send rate: %,.2f m/s", nonCallbackRate));

        // The async style HAS to be faster than the non-async style..
        assertTrue("async rate[" + callbackRate + "] should beat non-async rate[" + nonCallbackRate + "]", callbackRate / nonCallbackRate > 1.5);
    }

    private double benchmarkNonCallbackRate() throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getName());
        int count = 1000;
        ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            producer.send(session.createTextMessage("Hello"));
        }
        return 1000.0 * count / (System.currentTimeMillis() - start);
    }

    private double benchmarkCallbackRate() throws JMSException, InterruptedException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(getName());
        int count = 1000;
        final CountDownLatch messagesSent = new CountDownLatch(count);
        ActiveMQMessageProducer producer = (ActiveMQMessageProducer) session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            producer.send(session.createTextMessage("Hello"), new AsyncCallback() {
                @Override
                public void onSuccess() {
                    messagesSent.countDown();
                }

                @Override
                public void onException(JMSException exception) {
                    exception.printStackTrace();
                }
            });
        }
        messagesSent.await();
        return 1000.0 * count / (System.currentTimeMillis() - start);
    }
}
