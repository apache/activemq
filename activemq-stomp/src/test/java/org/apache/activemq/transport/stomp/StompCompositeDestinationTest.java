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
package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for support of composite destination support over STOMP
 */
public class StompCompositeDestinationTest extends StompTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompCompositeDestinationTest.class);

    protected ActiveMQConnection connection;

    @Override
    public void tearDown() throws Exception {
        try {
            connection.close();
        } catch (Exception ex) {}

        super.tearDown();
    }

    @Test(timeout = 20000)
    public void testSubscribeToCompositeQueue() throws Exception {
        stompConnect();

        String destinationA = "StompA";
        String destinationB = "StompB";

        String frame = "CONNECT\n" +
                       "login:system\n" +
                       "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        LOG.info("Subscribing to destination: {},{}", destinationA, destinationB);

        frame = "SUBSCRIBE\n" +
                "destination:/queue/" + destinationA + ",/queue/" + destinationB + "\n" +
                "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // Test in same order as the subscribe command

        sendMessage(destinationA, false);
        sendMessage(destinationB, false);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        // Test the reverse ordering

        sendMessage(destinationB, false);
        sendMessage(destinationA, false);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        stompConnection.disconnect();
    }

    @Test(timeout = 20000)
    public void testSubscribeToCompositeQueueTrailersDefault() throws Exception {
        stompConnect();

        String destinationA = "StompA";
        String destinationB = "StompB";

        String frame = "CONNECT\n" +
                       "login:system\n" +
                       "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        LOG.info("Subscribing to destination: {},{}", destinationA, destinationB);

        frame = "SUBSCRIBE\n" +
                "destination:/queue/" + destinationA + "," + destinationB + "\n" +
                "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // Test in same order as the subscribe command

        sendMessage(destinationA, false);
        sendMessage(destinationB, false);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        // Test the reverse ordering

        sendMessage(destinationB, false);
        sendMessage(destinationA, false);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        stompConnection.disconnect();
    }

    @Test(timeout = 20000)
    public void testSubscribeToCompositeTopics() throws Exception {
        stompConnect();

        String destinationA = "StompA";
        String destinationB = "StompB";

        String frame = "CONNECT\n" +
                       "login:system\n" +
                       "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        LOG.info("Subscribing to destination: {},{}", destinationA, destinationB);

        frame = "SUBSCRIBE\n" +
                "destination:/topic/" + destinationA + ",/topic/" + destinationB + "\n" +
                "ack:auto\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        // Test in same order as the subscribe command

        sendMessage(destinationA, true);
        sendMessage(destinationB, true);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        // Test the reverse ordering

        sendMessage(destinationB, true);
        sendMessage(destinationA, true);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("MESSAGE"));

        stompConnection.disconnect();
    }

    @Test(timeout = 60000)
    public void testSendMessageToCompositeQueue() throws Exception {
        stompConnect();

        String destinationA = "StompA";
        String destinationB = "StompB";

        String frame = "CONNECT\n" +
                       "login:system\n" +
                       "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" +
                "destination:/queue/" + destinationA + ",/queue/" + destinationB +
                "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        final BrokerViewMBean brokerView = getProxyToBroker();
        assertTrue("Should be two destinations for the dispatch", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getQueues().length == 2;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(150)));

        QueueViewMBean viewOfA = getProxyToQueue(destinationA);
        QueueViewMBean viewOfB = getProxyToQueue(destinationB);

        assertNotNull(viewOfA);
        assertNotNull(viewOfB);

        assertEquals(1, viewOfA.getQueueSize());
        assertEquals(1, viewOfB.getQueueSize());

        stompConnection.disconnect();
    }

    @Test(timeout = 60000)
    public void testSendMessageToCompositeQueueNoPrefixes() throws Exception {
        stompConnect();

        String destinationA = "StompA.Queue";
        String destinationB = "StompB.Queue";

        String frame = "CONNECT\n" +
                       "login:system\n" +
                       "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" +
                "destination:" + destinationA + "," + destinationB +
                "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        final BrokerViewMBean brokerView = getProxyToBroker();
        assertTrue("Should be two destinations for the dispatch", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                for(ObjectName queueName : brokerView.getQueues()) {
                    LOG.info("Broker Has Queue: {}", queueName);
                }
                return brokerView.getQueues().length == 2;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(150)));

        QueueViewMBean viewOfA = getProxyToQueue(destinationA);
        QueueViewMBean viewOfB = getProxyToQueue(destinationB);

        assertNotNull(viewOfA);
        assertNotNull(viewOfB);

        assertEquals(1, viewOfA.getQueueSize());
        assertEquals(1, viewOfB.getQueueSize());

        stompConnection.disconnect();
    }

    @Test(timeout = 60000)
    public void testSendMessageToCompositeTopic() throws Exception {
        stompConnect();

        String destinationA = "StompA";
        String destinationB = "StompB";

        String frame = "CONNECT\n" +
                       "login:system\n" +
                       "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        frame = "SEND\n" +
                "destination:/topic/" + destinationA + ",/topic/" + destinationB +
                "\n\n" + "Hello World" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        final BrokerViewMBean brokerView = getProxyToBroker();
        assertTrue("Should be two destinations for the dispatch", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerView.getTopics().length == 2;
            }
        }, TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS.toMillis(150)));

        TopicViewMBean viewOfA = getProxyToTopic(destinationA);
        TopicViewMBean viewOfB = getProxyToTopic(destinationB);

        assertNotNull(viewOfA);
        assertNotNull(viewOfB);

        assertEquals(1, viewOfA.getEnqueueCount());
        assertEquals(1, viewOfB.getEnqueueCount());

        stompConnection.disconnect();
    }

    private void sendMessage(String destinationName, boolean topic) throws JMSException {
        Connection connection = cf.createConnection("system", "manager");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = null;

        if (topic) {
            destination = session.createTopic(destinationName);
        } else {
            destination = session.createQueue(destinationName);
        }

        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("test");
        producer.send(message);

        connection.close();
    }
}
