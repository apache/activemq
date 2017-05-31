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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.jmx.DestinationView;
import org.apache.activemq.broker.jmx.QueueView;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MessageExpiryTimeDifferenceTest {

    public static final String QUEUE_NAME = "timeout.test";
    private ActiveMQConnection connection;
    private BrokerService broker;
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        createBroker();

        connection = createConnection();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
            }
        }

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    /**
     * if the client clock is slightly ahead of the brokers clock a message
     * could be expired on the client. When the expiry is sent to the broker it
     * checks if the message is also considered expired on the broker side.
     *
     * If the broker clock is behind the message could be considered not expired
     * on the broker and not removed from the broker's dispatched list. This
     * leaves the broker reporting a message inflight from the broker's
     * perspective even though the message has been expired on the
     * consumer(client) side
     *
     * The BrokerFlight is used to manipulate the expiration timestamp on the
     * message when it is sent and ack'd from the consumer to simulate a time
     * difference between broker and client in the unit test. This is rather
     * invasive but it it difficult to test this deterministically in a unit
     * test.
     */
    @Test(timeout = 30000)
    public void testInflightCountAfterExpiry() throws Exception {

        connection.start();

        // push message to queue
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("timeout.test");
        MessageProducer producer = session.createProducer(queue);
        TextMessage textMessage = session.createTextMessage(QUEUE_NAME);

        producer.send(textMessage);
        session.close();

        // try to consume message
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageConsumer consumer = session.createConsumer(queue);
        final CountDownLatch messageReceived = new CountDownLatch(1);

        // call consume in a separate thread
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        executorService.submit(new Runnable() {

            @Override
            public void run() {
                Message message = null;
                try {
                    message = consumer.receive(1000);
                } catch (JMSException e) {
                    fail();
                }

                // message should be null as it should have expired and the
                // consumer.receive(timeout) should return null.
                assertNull(message);
                messageReceived.countDown();
            }
        });

        messageReceived.await(20, TimeUnit.SECONDS);

        QueueView queueView = getQueueView(broker, QUEUE_NAME);
        assertEquals("Should be No inflight messages", 0, queueView.getInFlightCount());

    }

    private void createBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setPersistent(false);
        broker.setAdvisorySupport(false);
        broker.addConnector("tcp://localhost:0");

        // add a plugin to ensure the expiry happens on the client side the
        // acknowledge() reset the expiration time to 30 seconds in the future.
        //
        // this simulates a scenario where the client clock is *0 seconds ahead
        // of the broker's clock.
        broker.setPlugins(new BrokerPlugin[] { new BrokerPlugin() {

            @Override
            public Broker installPlugin(Broker broker) throws Exception {
                return new BrokerFilter(broker) {

                    private AtomicInteger counter = new AtomicInteger();
                    private org.apache.activemq.command.Message dispatchedMessage;

                    @Override
                    public void preProcessDispatch(MessageDispatch messageDispatch) {
                        if (counter.get() == 0 && messageDispatch.getDestination().getPhysicalName().contains("timeout.test")) {
                            // Set the expiration to now
                            dispatchedMessage = messageDispatch.getMessage();
                            dispatchedMessage.setExpiration(System.currentTimeMillis() - 100);

                            counter.incrementAndGet();
                        }
                        super.preProcessDispatch(messageDispatch);
                    }

                    @Override
                    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
                        // set the expiration in the future, to simulate broker's clock is
                        // 30 seconds behind client clock
                        if (ack.isExpiredAck()) {
                            dispatchedMessage.setExpiration(System.currentTimeMillis() + 300000);
                        }
                        super.acknowledge(consumerExchange, ack);
                    }
                };
            }
        } });

        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connectionUri);
    }

    protected ActiveMQConnection createConnection() throws Exception {
        return (ActiveMQConnection) createConnectionFactory().createConnection();
    }

    private QueueView getQueueView(BrokerService broker, String queueName) throws Exception {
        Map<ObjectName, DestinationView> queueViews = broker.getAdminView().getBroker().getQueueViews();

        for (ObjectName key : queueViews.keySet()) {
            DestinationView destinationView = queueViews.get(key);

            if (destinationView instanceof QueueView) {
                QueueView queueView = (QueueView) destinationView;

                if (queueView.getName().equals(queueName)) {
                    return queueView;
                }

            }
        }
        return null;
    }
}
