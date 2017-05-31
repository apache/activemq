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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionControl;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AMQ4157Test {
    static final Logger LOG = LoggerFactory.getLogger(AMQ4157Test.class);
    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    private final Destination destination = new ActiveMQQueue("Test");
    private final String payloadString = new String(new byte[8*1024]);
    private final boolean useBytesMessage= true;
    private final int parallelProducer = 20;
    private final int parallelConsumer = 100;

    private final Vector<Exception> exceptions = new Vector<Exception>();
    long toSend = 1000;

    @Test
    public void testPublishCountsWithRollbackConsumer() throws Exception {

        startBroker(true);

        final AtomicLong sharedCount = new AtomicLong(toSend);
        ExecutorService executorService = Executors.newCachedThreadPool();

        for (int i=0; i< parallelConsumer; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        consumeOneAndRollback();
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            });
        }

        for (int i=0; i< parallelProducer; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        publishMessages(sharedCount, 0);
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.MINUTES);
        assertTrue("Producers done in time", executorService.isTerminated());
        assertTrue("No exceptions: " + exceptions, exceptions.isEmpty());

        restartBroker(500);

        LOG.info("Attempting consume of {} messages", toSend);

        consumeMessages(toSend);
    }

    private void consumeOneAndRollback() throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = session.createConsumer(destination);
        Message message = null;
        while (message == null) {
            message = consumer.receive(1000);
        }
        session.rollback();
        connection.close();
    }

    private void consumeMessages(long count) throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(destination);
        for (int i=0; i<count; i++) {
            assertNotNull("got message "+ i, consumer.receive(20000));
        }
        assertNull("none left over", consumer.receive(2000));
    }

    private void restartBroker(int restartDelay) throws Exception {
        stopBroker();
        TimeUnit.MILLISECONDS.sleep(restartDelay);
        startBroker(false);
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    private void publishMessages(AtomicLong count, int expiry) throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.setWatchTopicAdvisories(false);
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(destination);
        while ( (count.getAndDecrement()) > 0) {
            Message message = null;
            if (useBytesMessage) {
                message = session.createBytesMessage();
                ((BytesMessage) message).writeBytes(payloadString.getBytes());
            } else {
                message = session.createTextMessage(payloadString);
            }
            producer.send(message, DeliveryMode.PERSISTENT, 5, expiry);
        }
        connection.syncSendPacket(new ConnectionControl());
        connection.close();
    }

    public void startBroker(boolean deleteAllMessages) throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        broker.addConnector("tcp://0.0.0.0:0");
        broker.start();

        String options = "?jms.redeliveryPolicy.maximumRedeliveries=-1&jms.prefetchPolicy.all=1000&jms.watchTopicAdvisories=false&jms.useAsyncSend=true&jms.alwaysSessionAsync=false&jms.dispatchAsync=false&socketBufferSize=131072&ioBufferSize=16384&wireFormat.tightEncodingEnabled=false&wireFormat.cacheSize=8192";
        connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri() + options);
    }
}