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
package org.apache.activemq.jms2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class ActiveMQJMS2MessageListenerTest extends ActiveMQJMS2TestBase {

    private final String destinationName;
    private final String destinationType;
    private final int ackMode;
    private final String messagePayload;

    public ActiveMQJMS2MessageListenerTest(String destinationType, int ackMode) {
        this.destinationName = "AMQ.JMS2.ACKMODE." + Integer.toString(ackMode) + destinationType.toUpperCase();
        this.destinationType = destinationType;
        this.ackMode = ackMode;
        this.messagePayload = "Test message destType: " + destinationType + " ackMode: " + Integer.toString(ackMode);
    }

    @Parameterized.Parameters(name="destinationType={0}, ackMode={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"queue", ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE },
                {"queue", Session.AUTO_ACKNOWLEDGE },
                {"queue", Session.CLIENT_ACKNOWLEDGE },
                {"queue", Session.DUPS_OK_ACKNOWLEDGE },
                {"queue", Session.SESSION_TRANSACTED }
        });
    }

    @Test
    public void testMessageListener() {

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, ackMode)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, destinationName);
            assertNotNull(destination);
            QueueViewMBean localQueueViewMBean = getQueueViewMBean((ActiveMQDestination)destination);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);

            AtomicInteger receivedMessageCount = new AtomicInteger();
            AtomicInteger exceptionCount = new AtomicInteger();
            CountDownLatch countDownLatch = new CountDownLatch(2);

            jmsConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    countDownLatch.countDown();
                    receivedMessageCount.incrementAndGet();
                    try {
                        switch(ackMode) {
                        case Session.CLIENT_ACKNOWLEDGE: message.acknowledge(); break;
                        case ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE: message.acknowledge(); break;
                        default: break;
                        }
                    } catch (JMSException e) {
                        exceptionCount.incrementAndGet();
                    }
                }
            });
            jmsContext.start();

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, "text", messagePayload);

            List<String> sentMessageIds = new LinkedList<>();
            for(int deliveryMode : Arrays.asList(DeliveryMode.NON_PERSISTENT, DeliveryMode.PERSISTENT)) {
                MessageData sendMessageData = new MessageData();
                sendMessageData.setMessage(message).setDeliveryMode(deliveryMode);
                sentMessageIds.add(ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData));
            }

            // For session transacted ack we ack after all messages are sent
            switch(ackMode) {
            case ActiveMQSession.SESSION_TRANSACTED:
                assertEquals(Long.valueOf(0), Long.valueOf(localQueueViewMBean.getEnqueueCount()));
                jmsContext.commit();
                assertEquals(Long.valueOf(2), Long.valueOf(localQueueViewMBean.getEnqueueCount()));
                break;
            default: 
                assertEquals(Long.valueOf(2), Long.valueOf(localQueueViewMBean.getEnqueueCount()));
                break;
            }

            countDownLatch.await(5, TimeUnit.SECONDS);

            assertEquals(Integer.valueOf(2), Integer.valueOf(receivedMessageCount.get()));
            assertEquals(Integer.valueOf(0), Integer.valueOf(exceptionCount.get()));

            // For session transacted we ack after all messages are received
            switch(ackMode) {
            case ActiveMQSession.SESSION_TRANSACTED:
                assertEquals(Long.valueOf(0), Long.valueOf(localQueueViewMBean.getDequeueCount()));
                jmsContext.commit();
                break;
            default: break;
            }
            jmsConsumer.close();

            assertTrue("DequeueCount = 2 and QueueSize = 0 expected", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return localQueueViewMBean.getDequeueCount() == 2l && localQueueViewMBean.getQueueSize() == 0l;
                }
            }, 5000l, 100l));

        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
