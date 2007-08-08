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

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.IdGenerator;

/**
 * @version $Revision: 1.4 $
 */
public class LargeMessageTestSupport extends ClientTestSupport implements MessageListener {

    private static final org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(LargeMessageTestSupport.class);

    protected static final int LARGE_MESSAGE_SIZE = 128 * 1024;
    protected static final int MESSAGE_COUNT = 100;
    protected Connection producerConnection;
    protected Connection consumerConnection;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected Session producerSession;
    protected Session consumerSession;
    protected byte[] largeMessageData;
    protected Destination destination;
    protected boolean isTopic = true;
    protected boolean isDurable = true;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected IdGenerator idGen = new IdGenerator();
    protected boolean validMessageConsumption = true;
    protected AtomicInteger messageCount = new AtomicInteger(0);

    protected int prefetchValue = 10000000;

    protected Destination createDestination() {
        String subject = getClass().getName();
        if (isTopic) {
            return new ActiveMQTopic(subject);
        } else {
            return new ActiveMQQueue(subject);
        }
    }

    protected MessageConsumer createConsumer() throws JMSException {
        if (isTopic && isDurable) {
            return consumerSession.createDurableSubscriber((Topic)destination, idGen.generateId());
        } else {
            return consumerSession.createConsumer(destination);
        }
    }

    public void setUp() throws Exception {
        super.setUp();
        ClientTestSupport.removeMessageStore();
        log.info("Setting up . . . . . ");
        messageCount.set(0);

        destination = createDestination();
        largeMessageData = new byte[LARGE_MESSAGE_SIZE];
        for (int i = 0; i < LARGE_MESSAGE_SIZE; i++) {
            if (i % 2 == 0) {
                largeMessageData[i] = 'a';
            } else {
                largeMessageData[i] = 'z';
            }
        }

        try {
            // allow the broker to start
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new JMSException(e.getMessage());
        }

        ActiveMQConnectionFactory fac = getConnectionFactory();
        producerConnection = fac.createConnection();
        setPrefetchPolicy((ActiveMQConnection)producerConnection);
        producerConnection.start();

        consumerConnection = fac.createConnection();
        setPrefetchPolicy((ActiveMQConnection)consumerConnection);
        consumerConnection.setClientID(idGen.generateId());
        consumerConnection.start();
        producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = producerSession.createProducer(createDestination());
        producer.setDeliveryMode(deliveryMode);
        consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = createConsumer();
        consumer.setMessageListener(this);
        log.info("Setup complete");
    }

    protected void setPrefetchPolicy(ActiveMQConnection activeMQConnection) {
        activeMQConnection.getPrefetchPolicy().setTopicPrefetch(prefetchValue);
        activeMQConnection.getPrefetchPolicy().setQueuePrefetch(prefetchValue);
        activeMQConnection.getPrefetchPolicy().setDurableTopicPrefetch(prefetchValue);
        activeMQConnection.getPrefetchPolicy().setQueueBrowserPrefetch(prefetchValue);
        activeMQConnection.getPrefetchPolicy().setOptimizeDurableTopicPrefetch(prefetchValue);
    }

    public void tearDown() throws Exception {
        Thread.sleep(1000);
        producerConnection.close();
        consumerConnection.close();

        super.tearDown();

        largeMessageData = null;
    }

    protected boolean isSame(BytesMessage msg1) throws Exception {
        boolean result = false;
        ((ActiveMQMessage)msg1).setReadOnlyBody(true);

        for (int i = 0; i < LARGE_MESSAGE_SIZE; i++) {
            result = msg1.readByte() == largeMessageData[i];
            if (!result)
                break;
        }

        return result;
    }

    public void onMessage(Message msg) {
        try {
            BytesMessage ba = (BytesMessage)msg;
            validMessageConsumption &= isSame(ba);
            assertTrue(ba.getBodyLength() == LARGE_MESSAGE_SIZE);
            if (messageCount.incrementAndGet() >= MESSAGE_COUNT) {
                synchronized (messageCount) {
                    messageCount.notify();
                }
            }
            log.info("got message = " + messageCount);
            if (messageCount.get() % 50 == 0) {
                log.info("count = " + messageCount);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testLargeMessages() throws Exception {
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            log.info("Sending message: " + i);
            BytesMessage msg = producerSession.createBytesMessage();
            msg.writeBytes(largeMessageData);
            producer.send(msg);
        }
        long now = System.currentTimeMillis();
        while (now + 60000 > System.currentTimeMillis() && messageCount.get() < MESSAGE_COUNT) {
            log.info("message count = " + messageCount);
            synchronized (messageCount) {
                messageCount.wait(1000);
            }
        }
        log.info("Finished count = " + messageCount);
        assertTrue("Not enough messages - expected " + MESSAGE_COUNT + " but got " + messageCount, messageCount.get() == MESSAGE_COUNT);
        assertTrue("received messages are not valid", validMessageConsumption);
        Thread.sleep(1000);
        log.info("FINAL count = " + messageCount);
    }
}
