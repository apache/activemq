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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import junit.framework.Test;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Small burn test moves sends a moderate amount of messages through the broker,
 * to checking to make sure that the broker does not lock up after a while of
 * sustained messaging.
 * 
 * @version $Revision$
 */
public class LoadTestBurnIn extends JmsTestSupport {
    private static final transient Log log = LogFactory.getLog(LoadTestBurnIn.class);

    public static Test suite() {
        return suite(LoadTestBurnIn.class);
    }

    protected void setUp() throws Exception {
        log.info("Start: " + getName());
        super.setUp();
    }

    protected void tearDown() throws Exception {
        try {
            super.tearDown();
        } catch (Throwable e) {
            e.printStackTrace(System.out);
        } finally {
            log.info("End: " + getName());
        }
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected BrokerService createBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("broker://(tcp://localhost:0)?useJmx=true"));
        // return BrokerFactory.createBroker(new
        // URI("xbean:org/apache/activemq/broker/store/loadtester.xml"));
    }

    protected ConnectionFactory createConnectionFactory() throws URISyntaxException, IOException {
        return new ActiveMQConnectionFactory(((TransportConnector)broker.getTransportConnectors().get(0)).getServer().getConnectURI());
    }

    public ActiveMQDestination destination;
    public int deliveryMode;
    public byte destinationType;
    public boolean durableConsumer;

    public int messageCount = 50000;
    public int messageSize = 1024;

    public void initCombosForTestSendReceive() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),});
        addCombinationValues("durableConsumer", new Object[] {Boolean.TRUE,});
        addCombinationValues("messageSize", new Object[] {Integer.valueOf(101), Integer.valueOf(102), Integer.valueOf(103), Integer.valueOf(104), Integer.valueOf(105),
                                                          Integer.valueOf(106), Integer.valueOf(107), Integer.valueOf(108),});
    }

    public void testSendReceive() throws Exception {

        // Durable consumer combination is only valid with topics
        if (durableConsumer && destinationType != ActiveMQDestination.TOPIC_TYPE)
            return;

        connection.setClientID(getName());
        connection.getPrefetchPolicy().setAll(1000);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination(session, destinationType);
        MessageConsumer consumer;
        if (durableConsumer) {
            consumer = session.createDurableSubscriber((Topic)destination, "sub1:" + System.currentTimeMillis());
        } else {
            consumer = session.createConsumer(destination);
        }
        profilerPause("Ready: ");

        final CountDownLatch producerDoneLatch = new CountDownLatch(1);

        // Send the messages, async
        new Thread() {
            public void run() {
                Connection connection2 = null;
                try {
                    connection2 = factory.createConnection();
                    Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(destination);
                    producer.setDeliveryMode(deliveryMode);
                    for (int i = 0; i < messageCount; i++) {
                        BytesMessage m = session.createBytesMessage();
                        m.writeBytes(new byte[messageSize]);
                        producer.send(m);
                    }
                    producer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                } finally {
                    safeClose(connection2);
                    producerDoneLatch.countDown();
                }

            }
        }.start();

        // Make sure all the messages were delivered.
        Message message = null;
        for (int i = 0; i < messageCount; i++) {
            message = consumer.receive(5000);
            assertNotNull("Did not get message: " + i, message);
        }

        profilerPause("Done: ");

        assertNull(consumer.receiveNoWait());
        message.acknowledge();

        // Make sure the producer thread finishes.
        assertTrue(producerDoneLatch.await(5, TimeUnit.SECONDS));
    }

}
