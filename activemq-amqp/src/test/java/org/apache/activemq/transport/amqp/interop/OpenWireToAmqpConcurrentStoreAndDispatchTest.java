/*
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
package org.apache.activemq.transport.amqp.interop;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.activemq.store.kahadb.KahaDBStore.PROPERTY_CANCELED_TASK_MOD_METRIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class OpenWireToAmqpConcurrentStoreAndDispatchTest extends AmqpClientTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(OpenWireToAmqpConcurrentStoreAndDispatchTest.class);

    private final String transformer;

    @Parameters(name="Transformer->{0}")
    public static Collection<Object[]> data() {
        System.setProperty(PROPERTY_CANCELED_TASK_MOD_METRIC, "100");
        return Arrays.asList(new Object[][] {
                {"jms"}
            });
    }

    public OpenWireToAmqpConcurrentStoreAndDispatchTest(String transformer) {
        this.transformer = transformer;
    }

    @Override
    protected String getAmqpTransformer() {
        return transformer;
    }

    @Override
    protected boolean isPersistent() {
        return true;
    }

    @Override
    protected boolean isUseOpenWireConnector() {
        return true;
    }

    @Test(timeout = 6000000)
    @Ignore("takes more than 6 mins to complete but fails earlier without fix")
    public void testNoErrorOnSend() throws Exception {

        final int numIterations = 100;
        int numConsumers = 3;
        final int numProducers = 10;
        final int numMessages = 2000;
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicInteger sent = new AtomicInteger();
        final AtomicInteger received = new AtomicInteger();
        final AtomicBoolean errorOnSend = new AtomicBoolean(false);

        final AtomicInteger toSend = new AtomicInteger(numMessages);

        final Random random = new Random();
        for (int i=0; i<numIterations; i++) {
            done.set(false);
            sent.set(0);
            received.set(0);
            toSend.set(numMessages);

            ExecutorService executorService = Executors.newCachedThreadPool();
            for (int j = 0; j < numConsumers; j++) {
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        AmqpConnection connection = null;
                        try {
                            AmqpClient client = createAmqpClient();
                            connection = trackConnection(client.connect());
                            AmqpSession session = connection.createSession();

                            AmqpReceiver receiver = session.createReceiver("queue://" + getTestName(), null, false, true);

                            while (!done.get() && received.get() < numMessages) {
                                receiver.flow(1);
                                AmqpMessage message = receiver.receive(5, TimeUnit.SECONDS);
                                if (message != null) {
                                    received.incrementAndGet();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            if (connection != null) {
                                connection.close();
                            }
                        }
                    }
                });
            }

            final byte[] payload = new byte[100];
            for (int k = 0; k < numProducers; k++) {
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        Connection connection = null;
                        try {
                            ActiveMQConnectionFactory connectionFactory =
                                    new ActiveMQConnectionFactory(brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString());
                            connection = connectionFactory.createConnection();
                            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                            MessageProducer producer = session.createProducer(new ActiveMQQueue(getTestName()));
                            BytesMessage bytesMessage = session.createBytesMessage();
                            bytesMessage.writeBytes(payload);
                            bytesMessage.setStringProperty("PP", "VALUE");
                            while (!done.get() && toSend.decrementAndGet() >= 0) {
                                producer.send(bytesMessage);
                                sent.incrementAndGet();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            errorOnSend.set(true);
                        } finally {
                            if (connection != null) {
                                try {
                                    connection.close();
                                } catch (JMSException ignored) {}
                            }
                        }
                    }
                });
            }

            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);

            done.set(true);
            assertEquals("[" + i + "] sent all requested", numMessages, sent.get());
            assertEquals("[" + i + "] got all sent", numMessages, received.get());
            assertFalse("[" + i + "] no error on send", errorOnSend.get());
        }
    }
}
