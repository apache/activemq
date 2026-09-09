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
package org.apache.activemq.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Destination;
import jakarta.jms.MessageConsumer;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.wireformat.WireFormat;

public class WebClientTopicNetworkTextMessageForwardingTest extends JmsMultipleBrokersTestSupport {

    private ActiveMQConnectionFactory originalFactory;
    private WebClient webClient;

    @Override
    protected void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();

        String options = "?persistent=false&useJmx=false";
        createBroker(new URI("broker:(tcp://localhost:0)/BrokerA" + options));
        createBroker(new URI("broker:(tcp://localhost:0)/BrokerB" + options));
        originalFactory = getWebClientFactory();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (webClient != null) {
                webClient.close();
            }
            setWebClientFactory(originalFactory);
        } finally {
            super.tearDown();
        }
    }

    public void testWebClientMarshalsTopicMessagesBeforeNetworkForwardingCopy() throws Exception {
        bridgeBrokers("BrokerA", "BrokerB");

        startAllBrokers();
        waitForBridgeFormation();

        Destination destination = createDestination("TEST.TEXT.FORWARD", true);
        MessageConsumer consumer = createSyncConsumer("BrokerB", destination);
        assertConsumersConnect("BrokerA", destination, 1, TimeUnit.SECONDS.toMillis(30));

        ActiveMQConnectionFactory webClientFactory =
            new ActiveMQConnectionFactory(brokers.get("BrokerA").broker.getVmConnectorURI());
        // Keep the first copy on the broker network path so this regression
        // exercises the AJAX/web ingress marshalling guarantee.
        webClientFactory.setCopyMessageOnSend(false);
        setWebClientFactory(webClientFactory);
        webClient = new WebClient();

        CoordinatedActiveMQTextMessage message = new CoordinatedActiveMQTextMessage();
        message.setText("payload");

        webClient.send(destination, message);

        TextMessage received = (TextMessage) consumer.receive(TimeUnit.SECONDS.toMillis(5));
        assertNotNull("message not received", received);
        assertEquals("payload", received.getText());
        assertNull("unexpected extra message", consumer.receive(500));

        assertTrue("web client should marshal the message before send", message.wasBeforeMarshalled());
        assertFalse("network forwarding copy should not happen before marshalling", message.wasCopiedBeforeMarshall());
    }

    private static ActiveMQConnectionFactory getWebClientFactory() throws Exception {
        Field field = WebClient.class.getDeclaredField("factory");
        field.setAccessible(true);
        return (ActiveMQConnectionFactory) field.get(null);
    }

    private static void setWebClientFactory(ActiveMQConnectionFactory factory) throws Exception {
        Field field = WebClient.class.getDeclaredField("factory");
        field.setAccessible(true);
        field.set(null, factory);
    }

    private static final class CoordinatedActiveMQTextMessage extends ActiveMQTextMessage {
        private volatile boolean beforeMarshalled;
        private volatile boolean copiedBeforeMarshall;

        @Override
        public void beforeMarshall(WireFormat wireFormat) throws IOException {
            beforeMarshalled = true;
            super.beforeMarshall(wireFormat);
        }

        @Override
        public Message copy() {
            CoordinatedActiveMQTextMessage copy = new CoordinatedActiveMQTextMessage();
            if (!beforeMarshalled) {
                copiedBeforeMarshall = true;

                CountDownLatch allowMarshall = new CountDownLatch(1);
                AtomicReference<Throwable> marshallingError = new AtomicReference<Throwable>();
                Thread marshaller = new Thread(() -> {
                    try {
                        assertTrue("timed out waiting to marshall", allowMarshall.await(5, TimeUnit.SECONDS));
                        beforeMarshall(null);
                    } catch (Throwable t) {
                        marshallingError.set(t);
                    }
                }, "web-client-marshaller");

                marshaller.start();
                super.copy(copy);
                allowMarshall.countDown();

                try {
                    marshaller.join(TimeUnit.SECONDS.toMillis(5));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }

                assertFalse("marshaller thread did not finish", marshaller.isAlive());
                if (marshallingError.get() != null) {
                    throw new AssertionError("marshaller failed", marshallingError.get());
                }

                copy.text = text;
                return copy;
            }

            super.copy(copy);
            copy.text = text;
            return copy;
        }

        private boolean wasBeforeMarshalled() {
            return beforeMarshalled;
        }

        private boolean wasCopiedBeforeMarshall() {
            return copiedBeforeMarshall;
        }
    }
}
