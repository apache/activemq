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

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test shows that when all messages are expired the QueueBrowser will
 * still finish properly and not hang indefinitely.  If a queue browser subscription
 * detects an expired message, it will tell the broker to expire the message but still
 * dispatch the message to the client as we want to get a snapshot in time.  This prevents
 * the problem of the browser enumeration returning true for hasMoreElements and then
 * hanging forever because all messages expired on dispatch.
 *
 * See: https://issues.apache.org/jira/browse/AMQ-5340
 *
 * <p>
 * This test is based on a test case submitted by Henno Vermeulen for AMQ-5340
 */
public class JmsQueueBrowserExpirationTest {

    private static final int MESSAGES_TO_SEND = 50;
    // Message expires after 1 second
    private static final long TTL = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(JmsQueueBrowserExpirationTest.class);

    private BrokerService broker;
    private URI connectUri;
    private ActiveMQConnectionFactory factory;
    private final ActiveMQQueue queue = new ActiveMQQueue("TEST");

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);

        TransportConnector connector = broker.addConnector("vm://localhost");
        broker.deleteAllMessages();
        broker.start();
        broker.waitUntilStarted();
        connectUri = connector.getConnectUri();
        factory = new ActiveMQConnectionFactory(connectUri);
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout=10000)
    public void testBrowsingExpiration() throws JMSException, InterruptedException {

        sendTestMessages();

        // Browse the queue.
        Connection browserConnection = factory.createConnection();
        browserConnection.start();

        int browsed = browse(queue, browserConnection);

        // The number of messages browsed should be equal to the number of
        // messages sent.
        assertEquals(MESSAGES_TO_SEND, browsed);

        long begin = System.nanoTime();
        while (browsed != 0) {
            // Give JMS threads more opportunity to do their work.
            Thread.sleep(100);
            browsed = browse(queue, browserConnection);
            LOG.info("[{}ms] found {}", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin), browsed);
        }
        LOG.info("Finished");
        browserConnection.close();
    }

   @Test(timeout=10000)
   public void testDoNotReceiveExpiredMessage() throws Exception {
      int WAIT_TIME = 1000;

      Connection connection = factory.createConnection();
      connection.start();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue producerQueue = session.createQueue("MyTestQueue");

      MessageProducer producer = session.createProducer(producerQueue);
      producer.setTimeToLive(WAIT_TIME);

      TextMessage message = session.createTextMessage("Test message");
      producer.send(producerQueue, message);

      int count = getMessageCount(producerQueue, session);
      assertEquals(1, count);

      Thread.sleep(WAIT_TIME + 1000);

      count = getMessageCount(producerQueue, session);
      assertEquals(0, count);

      producer.close();
      session.close();
      connection.close();
   }

    private int getMessageCount(Queue destination, Session session) throws Exception {
        int result = 0;
        QueueBrowser browser = session.createBrowser(destination);
        Enumeration<?> enumeration = browser.getEnumeration();
        while (enumeration.hasMoreElements()) {
            ++result;
            enumeration.nextElement();
        }
        browser.close();

        return result;
    }

    private int browse(ActiveMQQueue queue, Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        QueueBrowser browser = session.createBrowser(queue);
        Enumeration<?> enumeration = browser.getEnumeration();
        int browsed = 0;
        while (enumeration.hasMoreElements()) {
            TextMessage m = (TextMessage) enumeration.nextElement();
            browsed++;
            LOG.debug("B[{}]: {}", browsed, m.getText());
        }
        browser.close();
        session.close();
        return browsed;
    }

    protected void sendTestMessages() throws JMSException {
        // Send the messages to the Queue.
        Connection prodConnection = factory.createConnection();
        prodConnection.start();
        Session prodSession = prodConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = prodSession.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.setTimeToLive(TTL);

        for (int i = 1; i <= MESSAGES_TO_SEND; i++) {
            String msgStr = "Message: " + i;
            producer.send(prodSession.createTextMessage(msgStr));
            LOG.info("P&C: {}", msgStr);
        }

        prodSession.close();
    }
}