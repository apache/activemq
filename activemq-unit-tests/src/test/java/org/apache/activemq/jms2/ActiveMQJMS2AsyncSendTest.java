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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jakarta.jms.CompletionListener;
import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQJMS2AsyncSendTest extends ActiveMQJMS2TestBase{

    private static final Logger log = LoggerFactory.getLogger(ActiveMQJMS2AsyncSendTest.class);

    @Test
    public void testSendMessageWithSessionApi_spec_7_3_1() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#quality-of-service
        CountDownLatch latch = new CountDownLatch(1);
        CompletionListener completionListener = new CompletionListener() {

            @Override
            public void onCompletion(Message message) {
                latch.countDown();
            }

            @Override
            public void onException(Message message, Exception e) {
                throw new RuntimeException(e);
            }
        };

        messageProducer.send(
                session.createQueue(methodNameDestinationName),
                session.createTextMessage("Test-" + methodNameDestinationName),
                completionListener);
        boolean status = latch.await(10L, TimeUnit.SECONDS);
        if (!status) {
            fail("the completion listener was not triggered within 10 seconds or threw an exception");
        }
    }

    @Test
    public void testSendMessageWithContextApi_spec_7_3_1() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#quality-of-service
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            String textBody = "Test-" + methodNameDestinationName;
            jmsContext.start();
            CountDownLatch latch = new CountDownLatch(1);
            CompletionListener completionListener = new CompletionListener() {

                @Override
                public void onCompletion(Message message) {
                    latch.countDown();
                }

                @Override
                public void onException(Message message, Exception e) {
                    throw new RuntimeException(e);
                }
            };
            jmsProducer.setAsync(completionListener);
            jmsProducer.send(destination, textBody);
            boolean status = latch.await(10L, TimeUnit.SECONDS);
            if (!status) {
                fail("the completion listener was not triggered within 10 seconds or threw an exception");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testOnExceptionTriggered_spec_7_3_2() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#exceptions
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            String textBody = "Test-" + methodNameDestinationName;
            jmsContext.start();
            CountDownLatch latch = new CountDownLatch(1);
            CompletionListener completionListener = new CompletionListener() {

                @Override
                public void onCompletion(Message message) throws RuntimeException {
                    throw new RuntimeException("throw runtime exception");
                }

                @Override
                public void onException(Message message, Exception e) {
                    latch.countDown();
                }
            };

            jmsProducer.setAsync(completionListener);
            jmsProducer.send(destination, textBody);
            boolean status = latch.await(10L, TimeUnit.SECONDS);
            if (!status) {
                fail("the completion listener onException method was not triggered within 10 seconds");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCorrectMessageOrder_spec7_3_3() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-order-2
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            ArrayList<String> expectedOrderedMessages = new ArrayList<>();
            ArrayList<String> actualOrderedMessages = new ArrayList<>();
            Object mutex = new Object();
            jmsContext.start();
            int num_msgs = 100;
            CountDownLatch latch = new CountDownLatch(num_msgs);
            CompletionListener completionListener = new CompletionListener() {
                @Override
                public void onCompletion(Message message) {
                    synchronized (mutex) {
                        try {
                            String text = ((TextMessage) message).getText();
                            actualOrderedMessages.add(text);
                        } catch (JMSException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    latch.countDown();
                }

                @Override
                public void onException(Message message, Exception e) {
                    throw new RuntimeException(e);
                }
            };

            jmsProducer.setAsync(completionListener);
            for (int i = 0; i < num_msgs; i++) {
                String textBody = "Test-" + methodNameDestinationName + "-" + String.valueOf(i);
                expectedOrderedMessages.add(textBody);
                jmsProducer.send(destination, textBody);
            }
            boolean status = latch.await(10L, TimeUnit.SECONDS);
            if (!status) {
                fail("the completion listener was not triggered within 10 seconds or threw an exception");
            }
            for (int i = 0; i < num_msgs; i++) {
                String got = actualOrderedMessages.get(i);
                String expected = expectedOrderedMessages.get(i);
                if (!got.equals(expected)) {
                    fail(String.format("Message out of order. Got %s but expected %s", got, expected));
                }
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testUnableToCloseContextInCompletionListener_spec_7_3_4() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#close-commit-or-rollback
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            String textBody = "Test-" + methodNameDestinationName;
            jmsContext.start();
            CountDownLatch latch = new CountDownLatch(1);
            CompletionListener completionListener = new CompletionListener() {

                @Override
                public void onCompletion(Message message) {
                    jmsContext.close(); // This should cause a RuntimeException to throw and trigger the onException
                }

                @Override
                public void onException(Message message, Exception e) {
                    latch.countDown();
                }
            };

            jmsProducer.setAsync(completionListener);
            jmsProducer.send(destination, textBody);
            boolean status = latch.await(10L, TimeUnit.SECONDS);
            if (!status) {
                fail("the completion listener onException was not triggered within 10 seconds or threw an exception");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testUnableToCloseProducerInCompletionListener_spec_7_3_4() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#close-commit-or-rollback
        CountDownLatch latch = new CountDownLatch(1);
        CompletionListener completionListener = new CompletionListener() {
            @Override
            public void onCompletion(Message message) {
                try {
                    messageProducer.close(); // This should cause a RuntimeException to throw and trigger the onException
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onException(Message message, Exception e) {
                latch.countDown();
            }
        };
        messageProducer.send(session.createQueue(methodNameDestinationName),
                session.createTextMessage("Test-" + methodNameDestinationName), completionListener);
        boolean status = latch.await(10L, TimeUnit.SECONDS);
        if (!status) {
            fail("the completion listener onException was not triggered within 10 seconds or threw an exception");
        }
    }

    @Test
    public void testUnableToCommitTransactionInCompletionListener_spec_7_3_4() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#close-commit-or-rollback
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(
                DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.SESSION_TRANSACTED)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            String textBody = "Test-" + methodNameDestinationName;
            jmsContext.start();
            CountDownLatch latch = new CountDownLatch(1);
            CompletionListener completionListener = new CompletionListener() {

                @Override
                public void onCompletion(Message message) {
                    jmsContext.commit(); // This should cause a RuntimeException to throw and trigger the onException
                }

                @Override
                public void onException(Message message, Exception e) {
                    latch.countDown();
                }
            };

            jmsProducer.setAsync(completionListener);
            jmsProducer.send(destination, textBody);
            boolean status = latch.await(10L, TimeUnit.SECONDS);
            if (!status) {
                fail("the completion listener onException was not triggered within 10 seconds or threw an exception");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testUnableToRollbackTransactionInCompletionListener_spec_7_3_4() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#close-commit-or-rollback
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(
                DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.SESSION_TRANSACTED)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            String textBody = "Test-" + methodNameDestinationName;
            jmsContext.start();
            CountDownLatch latch = new CountDownLatch(1);
            CompletionListener completionListener = new CompletionListener() {

                @Override
                public void onCompletion(Message message) {
                    jmsContext.rollback(); // This should cause a RuntimeException to throw and trigger the onException
                }

                @Override
                public void onException(Message message, Exception e) {
                    latch.countDown();
                }
            };

            jmsProducer.setAsync(completionListener);
            jmsProducer.send(destination, textBody);
            boolean status = latch.await(10L, TimeUnit.SECONDS);
            if (!status) {
                fail("the completion listener onException was not triggered within 10 seconds or threw an exception");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCloseContextWailUntilAllIncompleteSentToFinish_spec_7_3_4() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#close-commit-or-rollback
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            ArrayList<String> expectedOrderedMessages = new ArrayList<>();
            ArrayList<String> actualOrderedMessages = new ArrayList<>();
            Object mutex = new Object();
            jmsContext.start();
            int num_msgs = 100;
            CompletionListener completionListener = new CompletionListener() {
                @Override
                public void onCompletion(Message message) {
                    synchronized (mutex) {
                        try {
                            String text = ((TextMessage) message).getText();
                            actualOrderedMessages.add(text);
                        } catch (JMSException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                @Override
                public void onException(Message message, Exception e) {
                    throw new RuntimeException(e);
                }
            };

            jmsProducer.setAsync(completionListener);
            for (int i = 0; i < num_msgs; i++) {
                String textBody = "Test-" + methodNameDestinationName + "-" + String.valueOf(i);
                expectedOrderedMessages.add(textBody);
                jmsProducer.send(destination, textBody);
            }
            jmsContext.close();
            if (expectedOrderedMessages.size() != actualOrderedMessages.size()) {
                fail("jmsContext doesn't wait until all inComplete send to finish");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCommitContextWailUntilAllIncompleteSentToFinish_spec_7_3_4() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#close-commit-or-rollback
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.SESSION_TRANSACTED)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            ArrayList<String> expectedOrderedMessages = new ArrayList<>();
            ArrayList<String> actualOrderedMessages = new ArrayList<>();
            Object mutex = new Object();
            jmsContext.start();
            int num_msgs = 100;
            CompletionListener completionListener = new CompletionListener() {
                @Override
                public void onCompletion(Message message) {
                    synchronized (mutex) {
                        try {
                            String text = ((TextMessage) message).getText();
                            actualOrderedMessages.add(text);
                        } catch (JMSException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                @Override
                public void onException(Message message, Exception e) {
                    throw new RuntimeException(e);
                }
            };

            jmsProducer.setAsync(completionListener);
            for (int i = 0; i < num_msgs; i++) {
                String textBody = "Test-" + methodNameDestinationName + "-" + String.valueOf(i);
                expectedOrderedMessages.add(textBody);
                jmsProducer.send(destination, textBody);
            }
            jmsContext.commit();
            if (expectedOrderedMessages.size() != actualOrderedMessages.size()) {
                fail("jmsContext doesn't wait until all inComplete send to finish");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testAbleToAccessMessageHeaderAfterAsyncSendCompleted_spec7_3_6_spec7_3_9() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#message-headers
        // We won't throw exception because it's optional as stated in the spec.
        // "If the Jakarta Messaging provider does not throw an exception then the behaviour is undefined."
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            String textBody = "Test-" + methodNameDestinationName;
            jmsContext.start();
            CountDownLatch latch = new CountDownLatch(1);
            CompletionListener completionListener = new CompletionListener() {

                @Override
                public void onCompletion(Message message) {
                    try {
                        if (!((TextMessage) message).getText().equals(textBody)) {
                            log.error("messages don't match");
                            throw new RuntimeException("messages don't match");
                        }
                    } catch (JMSException e) {
                        throw new RuntimeException(e);
                    }
                    latch.countDown();
                }

                @Override
                public void onException(Message message, Exception e) {
                    throw new RuntimeException(e);
                }
            };
            jmsProducer.setAsync(completionListener);
            TextMessage message = jmsContext.createTextMessage();
            message.setText(textBody);
            jmsProducer.send(destination, message);
            // Trying to get the message header
            int deliveryMode = message.getJMSDeliveryMode();
            boolean status = latch.await(10L, TimeUnit.SECONDS);
            if (!status) {
                fail("the completion listener was not triggered within 10 seconds or threw an exception");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCompletionListenerThreadingRestriction_spec7_3_7() throws Exception {
        // (https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#restrictions-on-threading)
        // The session can continue to be used by current application thread. Session is used one thread at a time (CompletionListener, Application thread ... etc)
        CountDownLatch latch = new CountDownLatch(1);
        CompletionListener completionListener = new CompletionListener() {
            @Override
            public void onCompletion(Message message) {
                try {
                    // Simulate busy processing of the message for 5 seconds.
                    Thread.sleep(5 * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            }

            @Override
            public void onException(Message message, Exception e) {
                throw new RuntimeException(e);
            }
        };

        messageProducer.send(
                session.createQueue(methodNameDestinationName),
                session.createTextMessage("Test-" + methodNameDestinationName),
                completionListener);
        MessageConsumer consumer = session.createConsumer(session.createQueue(methodNameDestinationName));
        Message msg = consumer.receive(2 * 1000);
        if (msg == null) {
            fail("session in the original thread of control was dedicated to the thread of control of CompletionListener");
        }
        String gotTextBody = ((TextMessage) msg).getText();
        if (!gotTextBody.equals("Test-" + methodNameDestinationName)) {
            fail("receive message is different than the one originally sent");
        }
        boolean status = latch.await(10L, TimeUnit.SECONDS);
        if (!status) {
            fail("the completion listener was not triggered within 10 seconds or threw an exception");
        }
    }

    @Test
    public void testCompletionListenerInvokedInDifferentThread_spec7_3_8() throws Exception {
        // https://jakarta.ee/specifications/messaging/3.1/jakarta-messaging-spec-3.1#use-of-the-completionlistener-by-the-jakarta-messaging-provider
        // The CompletionListener has to be invoked in different thread
        try(JMSContext jmsContext = activemqConnectionFactory.createContext(DEFAULT_JMS_USER, DEFAULT_JMS_PASS, Session.AUTO_ACKNOWLEDGE)) {
            assertNotNull(jmsContext);
            JMSProducer jmsProducer = jmsContext.createProducer();
            Destination destination = jmsContext.createQueue(methodNameDestinationName);
            String textBody = "Test-" + methodNameDestinationName;
            jmsContext.start();
            String testThreadName = Thread.currentThread().getName();
            CountDownLatch latch = new CountDownLatch(1);
            CompletionListener completionListener = new CompletionListener() {

                @Override
                public void onCompletion(Message message) {
                    String onCompletionThreadName = Thread.currentThread().getName();
                    if (!onCompletionThreadName.equals(testThreadName)) {
                        latch.countDown();
                    } else {
                        log.error("onCompletion is executed in the same thread as the application thread.");
                    }
                }

                @Override
                public void onException(Message message, Exception e) {
                    throw new RuntimeException(e);
                }
            };

            jmsProducer.setAsync(completionListener);
            jmsProducer.send(destination, textBody);
            boolean status = latch.await(10L, TimeUnit.SECONDS);
            if (!status) {
                fail("the completion listener was not triggered within 10 seconds or threw an exception");
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
