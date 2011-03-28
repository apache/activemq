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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ2645Test extends EmbeddedBrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ2645Test.class);
    private final static String QUEUE_NAME = "test.daroo.q";

    public void testWaitForTransportInterruptionProcessingHang()
            throws Exception {
        final ConnectionFactory fac = new ActiveMQConnectionFactory(
                "failover:(" + this.bindAddress + ")");
        final Connection connection = fac.createConnection();
        try {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue(QUEUE_NAME);
            final MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();

            producer.send(session.createTextMessage("test"));

            final CountDownLatch afterRestart = new CountDownLatch(1);
            final CountDownLatch twoNewMessages = new CountDownLatch(1);
            final CountDownLatch thirdMessageReceived = new CountDownLatch(1);

            final MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE_NAME));
            consumer.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    try {
                        afterRestart.await();

                        final TextMessage txtMsg = (TextMessage) message;
                        if (txtMsg.getText().equals("test")) {
                            producer.send(session.createTextMessage("test 1"));
                            TimeUnit.SECONDS.sleep(5);
                            // THIS SECOND send() WILL CAUSE CONSUMER DEADLOCK
                            producer.send(session.createTextMessage("test 2"));
                            LOG.info("Two new messages produced.");
                            twoNewMessages.countDown();
                        } else if (txtMsg.getText().equals("test 3")) {
                            thirdMessageReceived.countDown();
                        }
                    } catch (Exception e) {
                        LOG.error(e.toString());
                        throw new RuntimeException(e);
                    }
                }
            });

            LOG.info("Stopping broker....");
            broker.stop();

            LOG.info("Creating new broker...");
            broker = createBroker();
            startBroker();
            broker.waitUntilStarted();

            afterRestart.countDown();
            assertTrue("Consumer is deadlocked!", twoNewMessages.await(60, TimeUnit.SECONDS));

            producer.send(session.createTextMessage("test 3"));
            assertTrue("Consumer got third message after block", thirdMessageReceived.await(60, TimeUnit.SECONDS));

        } finally {
            broker.stop();
        }

    }

    @Override
    protected void setUp() throws Exception {
        bindAddress = "tcp://0.0.0.0:61617";
        super.setUp();
    }
}
