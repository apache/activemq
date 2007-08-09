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

package org.apache.activemq.usecases;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

/**
 * @author Peter Henning
 * @version $Revision: 1.1.1.1 $
 */
public class CreateTemporaryQueueBeforeStartTest extends TestCase {
    protected String bindAddress = "tcp://localhost:61621";
    private Connection connection;
    private BrokerService broker = new BrokerService();

    public void testCreateTemporaryQueue() throws Exception {
        connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        assertTrue("No queue created!", queue != null);
        Topic topic = session.createTemporaryTopic();
        assertTrue("No topic created!", topic != null);
    }

    public void testTryToReproduceNullPointerBug() throws Exception {
        String url = bindAddress;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        QueueConnection queueConnection = factory.createQueueConnection();
        this.connection = queueConnection;
        QueueSession session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        QueueSender sender = session.createSender(null); // Unidentified
        Queue receiverQueue = session.createTemporaryQueue();
        QueueReceiver receiver = session.createReceiver(receiverQueue);
        queueConnection.start();
    }

    public void testTemporaryQueueConsumer() throws Exception {
        final int number = 20;
        final AtomicInteger count = new AtomicInteger(0);
        for (int i = 0; i < number; i++) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    try {
                        QueueConnection connection = createConnection();
                        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                        Queue queue = session.createTemporaryQueue();
                        QueueReceiver consumer = session.createReceiver(queue);
                        connection.start();

                        if (count.incrementAndGet() >= number) {
                            synchronized (count) {
                                count.notify();
                            }
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
            thread.start();
        }
        int maxWaitTime = 20000;
        synchronized (count) {
            long waitTime = maxWaitTime;
            long start = System.currentTimeMillis();
            while (count.get() < number) {
                if (waitTime <= 0) {
                    break;
                } else {
                    count.wait(waitTime);
                    waitTime = maxWaitTime - (System.currentTimeMillis() - start);
                }
            }
        }
        assertTrue("Unexpected count: " + count, count.get() == number);
    }

    protected QueueConnection createConnection() throws Exception {
        ActiveMQConnectionFactory factory = createConnectionFactory();
        return factory.createQueueConnection();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(bindAddress);
    }

    protected void setUp() throws Exception {
        broker.setPersistent(false);
        broker.addConnector(bindAddress);
        broker.start();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        broker.stop();
        super.tearDown();
    }
}
