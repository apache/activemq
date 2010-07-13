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

package org.apache.bugs;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.CountDownLatch;


public class AMQ1730Test extends TestCase {

    private static final Log log = LogFactory.getLog(AMQ1730Test.class);


    private static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";


    BrokerService brokerService;

    private static final int MESSAGE_COUNT = 250;

    public AMQ1730Test() {
        super();
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        brokerService = new BrokerService();
        brokerService.addConnector("tcp://localhost:0");
        brokerService.setUseJmx(false);
        brokerService.start();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        brokerService.stop();
    }

    public void testRedelivery() throws Exception {

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                brokerService.getTransportConnectors().get(0).getConnectUri().toString() + "?jms.prefetchPolicy.queuePrefetch=100");

        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue("queue.test");

        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            log.info("Sending message " + i);
            TextMessage message = session.createTextMessage("Message " + i);
            producer.send(message);
        }

        producer.close();
        session.close();
        connection.stop();
        connection.close();

        final CountDownLatch countDownLatch = new CountDownLatch(MESSAGE_COUNT);

        final ValueHolder<Boolean> messageRedelivered = new ValueHolder<Boolean>(false);

        DefaultMessageListenerContainer messageListenerContainer = new DefaultMessageListenerContainer();
        messageListenerContainer.setConnectionFactory(connectionFactory);
        messageListenerContainer.setDestination(queue);
        messageListenerContainer.setAutoStartup(false);
        messageListenerContainer.setConcurrentConsumers(1);
        messageListenerContainer.setMaxConcurrentConsumers(16);
        messageListenerContainer.setMaxMessagesPerTask(10);
        messageListenerContainer.setReceiveTimeout(10000);
        messageListenerContainer.setRecoveryInterval(5000);
        messageListenerContainer.setAcceptMessagesWhileStopping(false);
        messageListenerContainer.setCacheLevel(DefaultMessageListenerContainer.CACHE_NONE);
        messageListenerContainer.setSessionTransacted(false);
        messageListenerContainer.setMessageListener(new MessageListener() {


            public void onMessage(Message message) {
                if (!(message instanceof TextMessage)) {
                    throw new RuntimeException();
                }
                try {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    int messageDeliveryCount = message.getIntProperty(JMSX_DELIVERY_COUNT);
                    if (messageDeliveryCount > 1) {
                        messageRedelivered.set(true);
                    }
                    log.info("[Count down latch: " + countDownLatch.getCount() + "][delivery count: " + messageDeliveryCount + "] - " + "Received message with id: " + message.getJMSMessageID() + " with text: " + text);

                } catch (JMSException e) {
                    e.printStackTrace();
                }
                finally {
                    countDownLatch.countDown();
                }
            }

        });
        messageListenerContainer.afterPropertiesSet();

        messageListenerContainer.start();

        countDownLatch.await();
        messageListenerContainer.stop();
        messageListenerContainer.destroy();

        assertFalse("no message has redelivery > 1", messageRedelivered.get());
    }

    private class ValueHolder<T> {

        private T value;

        public ValueHolder(T value) {
            super();
            this.value = value;
        }

        void set(T value) {
            this.value = value;
        }

        T get() {
            return value;
        }

    }

}
