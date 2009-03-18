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

import java.util.Vector;

import junit.framework.TestCase;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AMQ2149Test extends TestCase {

    private static final Log log = LogFactory.getLog(AMQ2149Test.class);

    private String BROKER_URL;
    private final String SEQ_NUM_PROPERTY = "seqNum";

    final int MESSAGE_LENGTH_BYTES = 75000;
    final int MAX_TO_SEND  = 2000;
    final long SLEEP_BETWEEN_SEND_MS = 5;
    final int NUM_SENDERS_AND_RECEIVERS = 10;
    
    BrokerService broker;
    Vector<Throwable> exceptions = new Vector<Throwable>();
    
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.addConnector("tcp://localhost:0");
        broker.deleteAllMessages();
        
        SystemUsage usage = new SystemUsage();
        MemoryUsage memoryUsage = new MemoryUsage();
        memoryUsage.setLimit(2048 * 7 * NUM_SENDERS_AND_RECEIVERS);
        usage.setMemoryUsage(memoryUsage);
        broker.setSystemUsage(usage);
        broker.start();

        BROKER_URL = "failover:("
            + broker.getTransportConnectors().get(0).getUri()
            +")?maxReconnectDelay=1000&useExponentialBackOff=false";
    }
    
    public void tearDown() throws Exception {
        broker.stop();
    }
    
    private String buildLongString() {
        final StringBuilder stringBuilder = new StringBuilder(
                MESSAGE_LENGTH_BYTES);
        for (int i = 0; i < MESSAGE_LENGTH_BYTES; ++i) {
            stringBuilder.append((int) (Math.random() * 10));
        }
        return stringBuilder.toString();
    }

    private class Receiver implements MessageListener {

        private final String queueName;

        private final Connection connection;

        private final Session session;

        private final MessageConsumer messageConsumer;

        private volatile long nextExpectedSeqNum = 0;

        public Receiver(String queueName) throws JMSException {
            this.queueName = queueName;
            connection = new ActiveMQConnectionFactory(BROKER_URL)
                    .createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            messageConsumer = session.createConsumer(new ActiveMQQueue(
                    queueName));
            messageConsumer.setMessageListener(this);
            connection.start();
        }

        public void onMessage(Message message) {
            try {
                final long seqNum = message.getLongProperty(SEQ_NUM_PROPERTY);
                if ((seqNum % 100) == 0) {
                    log.info(queueName + " received " + seqNum);
                }
                if (seqNum != nextExpectedSeqNum) {
                    log.warn(queueName + " received " + seqNum + " expected "
                            + nextExpectedSeqNum);
                    fail(queueName + " received " + seqNum + " expected "
                            + nextExpectedSeqNum);
                }
                ++nextExpectedSeqNum;
            } catch (Throwable e) {
                log.error(queueName + " onMessage error", e);
                exceptions.add(e);
            }
        }

    }

    private class Sender implements Runnable {

        private final String queueName;

        private final Connection connection;

        private final Session session;

        private final MessageProducer messageProducer;

        private volatile long nextSequenceNumber = 0;

        public Sender(String queueName) throws JMSException {
            this.queueName = queueName;
            connection = new ActiveMQConnectionFactory(BROKER_URL)
                    .createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            messageProducer = session.createProducer(new ActiveMQQueue(
                    queueName));
            messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            connection.start();
        }

        public void run() {
            final String longString = buildLongString();
            while (nextSequenceNumber <= MAX_TO_SEND) {
                try {
                    final Message message = session
                            .createTextMessage(longString);
                    message.setLongProperty(SEQ_NUM_PROPERTY,
                            nextSequenceNumber);
                    ++nextSequenceNumber;
                    messageProducer.send(message);
                } catch (Exception e) {
                    log.error(queueName + " send error", e);
                    exceptions.add(e);
                }
                try {
                    Thread.sleep(SLEEP_BETWEEN_SEND_MS);
                } catch (InterruptedException e) {
                    log.warn(queueName + " sleep interrupted", e);
                }
            }
        }
    }

    public void testOutOfOrderWithMemeUsageLimit() throws Exception {
        Vector<Thread> threads = new Vector<Thread>();
        
        for (int i = 0; i < NUM_SENDERS_AND_RECEIVERS; ++i) {
            final String queueName = "test.queue." + i;
            new Receiver(queueName);
            Thread thread = new Thread(new Sender(queueName));
            thread.start();
            threads.add(thread);
        }
        
        final long expiry = System.currentTimeMillis() + 1000 * 60 * 5;
        while(!threads.isEmpty() && exceptions.isEmpty() && System.currentTimeMillis() < expiry) {
            Thread sendThread = threads.firstElement();
            sendThread.join(1000*10);
            if (!sendThread.isAlive()) {
                threads.remove(sendThread);
            }
        }
        assertTrue("No timeout waiting for senders to complete", System.currentTimeMillis() < expiry);
        assertTrue("No exceptions", exceptions.isEmpty());
    }

}
