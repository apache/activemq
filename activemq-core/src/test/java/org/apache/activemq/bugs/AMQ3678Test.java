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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQTopicSubscriber;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.fail;


public class AMQ3678Test implements MessageListener {

    public int deliveryMode = DeliveryMode.NON_PERSISTENT;


    private BrokerService broker;

    AtomicInteger messagesSent = new AtomicInteger(0);
    AtomicInteger messagesReceived = new AtomicInteger(0);

    ActiveMQTopic destination = new ActiveMQTopic("XYZ");

    int port;
    int jmxport;


    final CountDownLatch latch = new CountDownLatch(2);


    public static void main(String[] args) throws Exception {

    }


    public static int findFreePort() throws IOException {
        ServerSocket socket = null;

        try {
            // 0 is open a socket on any free port
            socket = new ServerSocket(0);
            return socket.getLocalPort();
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }


    @Test
    public void countConsumers() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:" + port);
        factory.setAlwaysSyncSend(true);
        factory.setDispatchAsync(false);

        final Connection producerConnection = factory.createConnection();
        producerConnection.start();

        final Connection consumerConnection = factory.createConnection();

        consumerConnection.setClientID("subscriber1");
        Session consumerMQSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        ActiveMQTopicSubscriber activeConsumer = (ActiveMQTopicSubscriber) consumerMQSession.createDurableSubscriber(destination, "myTopic?consumer.prefetchSize=1");

        activeConsumer.setMessageListener(this);

        consumerConnection.start();


        final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageProducer producer = producerSession.createProducer(destination);
        producer.setDeliveryMode(deliveryMode);

        Thread t = new Thread(new Runnable() {

            private boolean done = false;

            public void run() {
                while (!done) {
                    if (messagesSent.get() == 50) {
                        try {
                            broker.getAdminView().removeTopic(destination.getTopicName());
                        } catch (Exception e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                            System.err.flush();
                            fail("Unable to remove destination:"
                                    + destination.getPhysicalName());
                        }
                    }

                    try {
                        producer.send(producerSession.createTextMessage());
                        int val = messagesSent.incrementAndGet();

                        System.out.println("sent message (" + val + ")");
                        System.out.flush();

                        if (val == 100) {
                            done = true;
                            latch.countDown();
                            producer.close();
                            producerSession.close();

                        }
                    } catch (JMSException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        });

        t.start();

        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                fail("did not receive all the messages");
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            fail("did not receive all the messages, exception waiting for latch");
            e.printStackTrace();
        }


//   


    }

    @Before
    public void setUp() throws Exception {

        try {
            port = findFreePort();
            jmxport = findFreePort();
        } catch (Exception e) {
            fail("Unable to obtain a free port on which to start the broker");
        }

        System.out.println("Starting broker");
        System.out.flush();
        broker = new BrokerService();
        broker.setPersistent(false);
        ManagementContext ctx = new ManagementContext(ManagementFactory.getPlatformMBeanServer());
        ctx.setConnectorPort(jmxport);
        broker.setManagementContext(ctx);
        broker.setUseJmx(true);
//        broker.setAdvisorySupport(false);
//        broker.setDeleteAllMessagesOnStartup(true);

        broker.addConnector("tcp://localhost:" + port).setName("Default");
        broker.start();


        System.out.println("End of Broker Setup");
        System.out.flush();
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }


    @Override
    public void onMessage(Message message) {
        try {
            message.acknowledge();
            int val = messagesReceived.incrementAndGet();
            System.out.println("received message (" + val + ")");
            System.out.flush();
            if (messagesReceived.get() == 100) {
                latch.countDown();
            }
        } catch (JMSException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


}
