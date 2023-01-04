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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class ActiveMQJMS2MessageTypesTest extends ActiveMQJMS2TestBase {

    private final Set<Integer> VALID_PRIORITIES = Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    private final String clientID;
    private final String destinationType;
    private final String messagePayload;
    private final String messageType;

    public ActiveMQJMS2MessageTypesTest(String destinationType, String messageType) {
        this.clientID = destinationType + "-" + messageType;
        this.destinationType = destinationType;
        this.messagePayload = "Test message payload";
        this.messageType = messageType;
    }

    @Parameterized.Parameters(name="destinationType={0}, messageType={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"queue", "bytes"},
                {"queue", "map"},
                {"queue", "object"},
                {"queue", "stream"},
                {"queue", "text"},
                {"topic", "bytes"},
                {"topic", "map"},
                {"topic", "object"},
                {"topic", "stream"},
                {"topic", "text"},
                {"temp-queue", "bytes"},
                {"temp-queue", "map"},
                {"temp-queue", "object"},
                {"temp-queue", "stream"},
                {"temp-queue", "text"},
                {"temp-topic", "bytes"},
                {"temp-topic", "map"},
                {"temp-topic", "object"},
                {"temp-topic", "stream"},
                {"temp-topic", "text"},
        });
    }

    @Test
    public void testMessageDeliveryMode() {

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);
            assertNotNull(jmsConsumer);

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);

            List<String> sentMessageIds = new LinkedList<>();
            for(int deliveryMode : Arrays.asList(DeliveryMode.NON_PERSISTENT, DeliveryMode.PERSISTENT)) {
                MessageData sendMessageData = new MessageData();
                sendMessageData.setMessage(message).setDeliveryMode(deliveryMode);
                sentMessageIds.add(ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData));
            }

            List<Message> recvMessages = new CopyOnWriteArrayList<>();
            AtomicBoolean receivedExpected = new AtomicBoolean(false);
            AtomicInteger exceptionCount = new AtomicInteger();

            jmsConsumer.setMessageListener(new MessageListener() {
                private boolean recvNonPersistent = false;
                private boolean recvPersistent = false;

                @Override
                public void onMessage(Message message) {
                    recvMessages.add(message);
                    try {
                        switch(message.getJMSDeliveryMode()) {
                        case DeliveryMode.NON_PERSISTENT: recvNonPersistent = true; break;
                        case DeliveryMode.PERSISTENT: recvPersistent = true; break;
                        default: break;
                        }

                        if(recvNonPersistent && recvPersistent) {
                            receivedExpected.set(true);
                        }
                    } catch (JMSException e) {
                        exceptionCount.incrementAndGet();
                    }
                }
            });
            jmsContext.start();

            assertTrue("Expected to receive both DeliveryModes", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return receivedExpected.get();
                }
            }, 5000l, 100l));
            jmsConsumer.close();
            jmsContext.stop();

            assertEquals(Integer.valueOf(2), Integer.valueOf(recvMessages.size()));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testMessageDeliveryModeInvalid() {

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);
            jmsContext.start();

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);
            MessageData sendMessageData = new MessageData();
            sendMessageData.setMessage(message).setDeliveryMode(99);
            boolean caught = false;
            try {
                ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData);
                fail("IlegalStateRuntimeException expected");
            } catch (IllegalStateRuntimeException e) {
                assertEquals("unknown delivery mode: 99", e.getMessage());
                caught = true;
            }
            assertTrue(caught);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testMessagePriority() {

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);
            assertNotNull(jmsConsumer);

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);
            List<String> sentMessageIds = new LinkedList<>();
            for(int priority : VALID_PRIORITIES) {
                MessageData sendMessageData = new MessageData();
                sendMessageData.setMessage(message).setPriority(priority);
                sentMessageIds.add(ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData));
            }

            final List<Message> recvMessages = new CopyOnWriteArrayList<>();
            final CountDownLatch countDownLatch = new CountDownLatch(10);

            jmsConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    recvMessages.add(message);
                    countDownLatch.countDown();
                }
            });
            jmsContext.start();

            assertTrue("Expected to receive 10 messages", countDownLatch.await(5, TimeUnit.SECONDS));
            jmsConsumer.close();
            jmsContext.stop();

            // Check that exactly 10 messages have been received 
            assertEquals(Integer.valueOf(10), Integer.valueOf(recvMessages.size()));

            int validatedCount = 0;
            for(int validatedPriority : VALID_PRIORITIES) {
                for(javax.jms.Message tmpMessage : recvMessages) {
                    if(tmpMessage.getJMSPriority() == validatedPriority) {
                        MessageData messageData = new MessageData();
                        messageData.setMessageType(messageType).setMessagePayload(messagePayload).setPriority(validatedPriority);
                        ActiveMQJMS2TestSupport.validateMessageData(tmpMessage, messageData);
                        validatedCount++;
                    }
                }
            }
            assertEquals(Integer.valueOf(10), Integer.valueOf(validatedCount));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testMessagePriorityInvalidLower() {

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);
            MessageData sendMessageData = new MessageData();
            sendMessageData.setMessage(message).setPriority(-1);
            boolean caught = false;
            try {
                ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData);
                fail("IlegalStateRuntimeException expected");
            } catch (IllegalStateRuntimeException e) {
                assertEquals("default priority must be a value between 0 and 9", e.getMessage());
                caught = true;
            }
            assertTrue(caught);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testMessagePriorityInvalidHigher() {

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);
            MessageData sendMessageData = new MessageData();
            sendMessageData.setMessage(message).setPriority(10);
            boolean caught = false;
            try {
                ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData);
                fail("IlegalStateRuntimeException expected");
            } catch (IllegalStateRuntimeException e) {
                assertEquals("default priority must be a value between 0 and 9", e.getMessage());
                caught = true;
            }
            assertTrue(caught);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testMessageTimestampTimeToLive() {

        long timeToLive = 900000l;

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);
            assertNotNull(jmsConsumer);

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);

            List<String> sentMessageIds = new LinkedList<>();
            long messageExpiration = Long.MIN_VALUE;
            long messageTimestamp = Long.MIN_VALUE;

            MessageData sendMessageData = new MessageData();
            sendMessageData.setMessage(message).setTimeToLive(timeToLive);
            sentMessageIds.add(ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData));
            messageExpiration = message.getJMSExpiration();
            messageTimestamp = message.getJMSTimestamp();
            assertNotEquals(Long.MIN_VALUE, messageExpiration);
            assertNotEquals(Long.MIN_VALUE, messageTimestamp);

            final List<Message> recvMessages = new CopyOnWriteArrayList<>();
            final CountDownLatch countDownLatch = new CountDownLatch(1);

            jmsConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    recvMessages.add(message);
                    countDownLatch.countDown();
                }
            });
            jmsContext.start();

            assertTrue("Expected to receive 1 message", countDownLatch.await(5l, TimeUnit.SECONDS));
            jmsConsumer.close();
            jmsContext.stop();

            // Check that exactly 1 messages have been received 
            assertEquals(Integer.valueOf(1), Integer.valueOf(recvMessages.size()));

            int validatedCount = 0;
            for(javax.jms.Message tmpMessage : recvMessages) {
                MessageData recvMessageData = new MessageData();
                recvMessageData.setMessageType(messageType).setMessagePayload(messagePayload).setExpiration(messageExpiration).setTimestamp(messageTimestamp);
                ActiveMQJMS2TestSupport.validateMessageData(tmpMessage, recvMessageData);
                validatedCount++;
            }
            assertEquals(Integer.valueOf(1), Integer.valueOf(validatedCount));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testMessageDisableTimestamp() {

        long timeToLive = 900000l;

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);
            assertNotNull(jmsConsumer);

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);

            List<String> sentMessageIds = new LinkedList<>();
            long messageExpiration = Long.MIN_VALUE;
            long messageTimestamp = Long.MIN_VALUE;
            MessageData sendMessageData = new MessageData();
            sendMessageData.setMessage(message).setTimeToLive(timeToLive).setDisableMessageTimestamp(true);
            sentMessageIds.add(ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData));
            messageExpiration = message.getJMSExpiration();
            messageTimestamp = message.getJMSTimestamp();
            assertEquals(0l, messageTimestamp);
            assertNotEquals(Long.MIN_VALUE, messageExpiration);

            final List<Message> recvMessages = new CopyOnWriteArrayList<>();
            final CountDownLatch countDownLatch = new CountDownLatch(1);

            jmsConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    recvMessages.add(message);
                    countDownLatch.countDown();
                }
            });
            jmsContext.start();

            assertTrue("Expected to receive 1 message", countDownLatch.await(5l, TimeUnit.SECONDS));
            jmsConsumer.close();
            jmsContext.stop();

            for(javax.jms.Message tmpMessage : recvMessages) {
                MessageData recvMessageData = new MessageData();
                recvMessageData.setMessagePayload(messagePayload).setMessageType(messageType).setExpiration(messageExpiration).setTimestamp(messageTimestamp).setDisableMessageTimestamp(true);
                ActiveMQJMS2TestSupport.validateMessageData(tmpMessage, recvMessageData);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testMessageNonQOSHeaders() {

        String jmsCorrelationID = UUID.randomUUID().toString();

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);
            assertNotNull(jmsConsumer);

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);
            Destination jmsReplyTo = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName + ".REPLYTO");
            String jmsType = message.getClass().getName(); 

            List<String> sentMessageIds = new LinkedList<>();
            MessageData sendMessageData = new MessageData();
            sendMessageData.setMessage(message).setCorrelationID(jmsCorrelationID).setReplyTo(jmsReplyTo).setJMSType(jmsType);
            sentMessageIds.add(ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData));

            final List<Message> recvMessages = new CopyOnWriteArrayList<>();
            final CountDownLatch countDownLatch = new CountDownLatch(1);

            jmsConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    recvMessages.add(message);
                    countDownLatch.countDown();
                }
            });
            jmsContext.start();

            assertTrue("Expected to receive 1 message", countDownLatch.await(5l, TimeUnit.SECONDS));
            jmsConsumer.close();
            jmsContext.stop();

            for(javax.jms.Message tmpMessage : recvMessages) {
                MessageData recvMessageData = new MessageData();
                recvMessageData.setMessagePayload(messagePayload).setMessageType(messageType).setCorrelationID(jmsCorrelationID).setReplyTo(jmsReplyTo).setJMSType(jmsType);
                ActiveMQJMS2TestSupport.validateMessageData(tmpMessage, recvMessageData);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }   

    @Test
    public void testMessageDisableMessageID() {

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);
            JMSConsumer jmsConsumer = jmsContext.createConsumer(destination);
            assertNotNull(jmsConsumer);

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);
            String jmsMessageID = (ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, message));

            final List<Message> recvMessages = new CopyOnWriteArrayList<>();
            final CountDownLatch countDownLatch = new CountDownLatch(1);

            jmsConsumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    recvMessages.add(message);
                    countDownLatch.countDown();
                }
            });
            jmsContext.start();

            assertTrue("Expected to receive 1 message", countDownLatch.await(5l, TimeUnit.SECONDS));
            jmsConsumer.close();
            jmsContext.stop();

            for(javax.jms.Message tmpMessage : recvMessages) {
                MessageData messageData = new MessageData();
                messageData.setMessageType(messageType).setMessagePayload(messagePayload).setMessageID(jmsMessageID);
                ActiveMQJMS2TestSupport.validateMessageData(tmpMessage, messageData);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testTopicDurableSubscriber() {
        // Skip if this is not a topic test run
        if(!"topic".equals(destinationType)) {
            return;
        }

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            jmsContext.setClientID(clientID);

            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);
            assertTrue(destination instanceof Topic);
            Topic topic = Topic.class.cast(destination);

            JMSConsumer jmsConsumerRegister = jmsContext.createDurableConsumer(topic, methodNameDestinationName);
            assertNotNull(jmsConsumerRegister);
            jmsContext.start();
            jmsConsumerRegister.close();

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);
            String jmsMessageID = (ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, message));

            JMSConsumer jmsConsumerDurable = jmsContext.createDurableConsumer(topic, methodNameDestinationName);
            assertNotNull(jmsConsumerDurable);

            final List<Message> recvMessages = new CopyOnWriteArrayList<>();
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            jmsConsumerDurable.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    recvMessages.add(message);
                    countDownLatch.countDown();
                }
            });

            assertTrue("Expected to receive 1 message", countDownLatch.await(5l, TimeUnit.SECONDS));
            jmsConsumerDurable.close();
            jmsContext.stop();

            for(javax.jms.Message tmpMessage : recvMessages) {
                MessageData messageData = new MessageData();
                messageData.setMessageType(messageType).setMessagePayload(messagePayload).setMessageID(jmsMessageID);
                ActiveMQJMS2TestSupport.validateMessageData(tmpMessage, messageData);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testTopicDurableSubscriberSelector() {
        // Skip if this is not a topic test run
        if(!"topic".equals(destinationType)) {
            return;
        }

        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            jmsContext.setClientID(clientID);

            Destination destination = ActiveMQJMS2TestSupport.generateDestination(jmsContext, destinationType, methodNameDestinationName);
            assertNotNull(destination);
            assertTrue(destination instanceof Topic);
            Topic topic = Topic.class.cast(destination);

            JMSConsumer jmsConsumerRegister = jmsContext.createDurableConsumer(topic, methodNameDestinationName, "JMSPriority=7", false);
            assertNotNull(jmsConsumerRegister);
            assertEquals("JMSPriority=7", jmsConsumerRegister.getMessageSelector());
            jmsContext.start();

            jmsConsumerRegister.close();

            Message message = ActiveMQJMS2TestSupport.generateMessage(jmsContext, messageType, messagePayload);

            List<String> sentMessageIds = new LinkedList<>();
            String matchMessageId = null;
            for(int priority=0; priority<10; priority++) {
                MessageData sendMessageData = new MessageData();
                sendMessageData.setMessage(message).setPriority(priority);
                String sentMessageId = ActiveMQJMS2TestSupport.sendMessage(jmsContext, destination, sendMessageData);
                sentMessageIds.add(sentMessageId);
                if(priority == 7) {
                    matchMessageId = sentMessageId;
                }
            }

            JMSConsumer jmsConsumerDurable = jmsContext.createDurableConsumer(topic, methodNameDestinationName, "JMSPriority=7", false);
            assertNotNull(jmsConsumerDurable);
            assertEquals("JMSPriority=7", jmsConsumerDurable.getMessageSelector());

            final List<Message> recvMessages = new CopyOnWriteArrayList<>();
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            jmsConsumerDurable.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    recvMessages.add(message);
                    countDownLatch.countDown();
                }
            });

            assertTrue("Expected to receive 1 message", countDownLatch.await(5l, TimeUnit.SECONDS));
            jmsConsumerDurable.close();
            jmsContext.stop();

            for(javax.jms.Message tmpMessage : recvMessages) {
                MessageData recvMessageData = new MessageData();
                recvMessageData.setMessageType(messageType).setMessagePayload(messagePayload).setMessageID(matchMessageId);
                ActiveMQJMS2TestSupport.validateMessageData(tmpMessage, recvMessageData);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
