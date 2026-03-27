/*
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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AmqpToOpenWireNetworkRaceTest {

    private static final String TOPIC_NAME = "race.amqp.to.openwire.topic";
    private static final String RACE_PROPERTY = "forceTextCopyMarshallRace";

    private BrokerService localBroker;
    private BrokerService remoteBroker;
    private TransportConnector localOpenWireConnector;
    private TransportConnector localAmqpConnector;
    private TransportConnector remoteOpenWireConnector;

    @Before
    public void setUp() throws Exception {
        CoordinatedTextMessage.resetRaceState();

        remoteBroker = new BrokerService();
        remoteBroker.setBrokerName("remote-broker");
        remoteBroker.setPersistent(false);
        remoteBroker.setUseJmx(false);
        remoteBroker.setSchedulerSupport(false);
        remoteOpenWireConnector = remoteBroker.addConnector("tcp://localhost:0?maximumConnections=1000");
        remoteBroker.start();
        remoteBroker.waitUntilStarted();

        localBroker = new BrokerService();
        localBroker.setBrokerName("local-broker");
        localBroker.setPersistent(false);
        localBroker.setUseJmx(false);
        localBroker.setSchedulerSupport(false);
        localBroker.setPlugins(new BrokerPlugin[] { new RaceMessagePlugin() });
        localOpenWireConnector = localBroker.addConnector("tcp://localhost:0?maximumConnections=1000");
        localAmqpConnector = localBroker.addConnector("amqp://localhost:0?transport.transformer=jms&maximumConnections=1000");

        String remoteUri = "static:(tcp://localhost:" + remoteOpenWireConnector.getPublishableConnectURI().getPort() + ")";
        NetworkConnector connector = localBroker.addNetworkConnector(remoteUri);
        connector.setDuplex(false);
        connector.setDynamicOnly(false);
        connector.setConduitSubscriptions(true);

        localBroker.start();
        localBroker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (localBroker != null) {
            localBroker.stop();
            localBroker.waitUntilStopped();
        }

        if (remoteBroker != null) {
            remoteBroker.stop();
            remoteBroker.waitUntilStopped();
        }
    }

    @Test(timeout = 60000)
    public void testAmqpTextMessageKeepsBodyAcrossLocalOpenWireAndNetworkBridgeDispatch() throws Exception {
        final String payload = "payload-race";

        ActiveMQConnectionFactory localOpenWireFactory =
            new ActiveMQConnectionFactory("tcp://localhost:" + localOpenWireConnector.getPublishableConnectURI().getPort());
        ActiveMQConnectionFactory remoteOpenWireFactory =
            new ActiveMQConnectionFactory("tcp://localhost:" + remoteOpenWireConnector.getPublishableConnectURI().getPort());
        JmsConnectionFactory amqpFactory =
            new JmsConnectionFactory("amqp://localhost:" + localAmqpConnector.getPublishableConnectURI().getPort());

        Connection localOpenWireConnection = localOpenWireFactory.createConnection("admin", "password");
        Connection remoteOpenWireConnection = remoteOpenWireFactory.createConnection("admin", "password");
        Connection amqpConnection = amqpFactory.createConnection("admin", "password");

        try {
            localOpenWireConnection.start();
            remoteOpenWireConnection.start();
            amqpConnection.start();

            Session localSession = localOpenWireConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session remoteSession = remoteOpenWireConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session amqpSession = amqpConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer localConsumer = localSession.createConsumer(localSession.createTopic(TOPIC_NAME));
            MessageConsumer remoteConsumer = remoteSession.createConsumer(remoteSession.createTopic(TOPIC_NAME));
            MessageProducer producer = amqpSession.createProducer(amqpSession.createTopic(TOPIC_NAME));
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            assertTrue("Timed out waiting for network bridge to connect", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return !localBroker.getNetworkConnectors().isEmpty() &&
                           !localBroker.getNetworkConnectors().get(0).activeBridges().isEmpty();
                }
            }, 20000, 100));

            TextMessage warmup = amqpSession.createTextMessage("warmup");
            producer.send(warmup);

            assertTextMessageBody("warmup should reach the local OpenWire consumer", localConsumer.receive(5000), "warmup");
            assertTextMessageBody("warmup should traverse the network bridge", remoteConsumer.receive(5000), "warmup");

            CoordinatedTextMessage.resetRaceState();

            TextMessage outbound = amqpSession.createTextMessage(payload);
            outbound.setBooleanProperty(RACE_PROPERTY, true);
            outbound.setStringProperty("expectedPayload", payload);
            producer.send(outbound);

            assertTextMessageBody("local OpenWire consumer should receive the full body", localConsumer.receive(5000), payload);

            Message remoteMessage = remoteConsumer.receive(5000);
            assertNotNull("Timed out waiting for forwarded message", remoteMessage);
            assertTrue("Expected TextMessage but got " + remoteMessage.getClass(), remoteMessage instanceof TextMessage);
            assertEquals("payload-race", remoteMessage.getStringProperty("expectedPayload"));
            assertEquals("remote OpenWire consumer should receive the full body", payload, ((TextMessage) remoteMessage).getText());

            CoordinatedTextMessage.assertRaceWasExercised();
        } finally {
            amqpConnection.close();
            remoteOpenWireConnection.close();
            localOpenWireConnection.close();
        }
    }

    private static void assertTextMessageBody(String message, Message received, String expectedBody) throws Exception {
        assertNotNull(message, received);
        assertTrue("Expected TextMessage but got " + received.getClass(), received instanceof TextMessage);
        assertEquals(expectedBody, ((TextMessage) received).getText());
    }

    private static final class RaceMessagePlugin implements BrokerPlugin {

        @Override
        public Broker installPlugin(Broker broker) throws Exception {
            return new BrokerFilter(broker) {
                @Override
                public void send(ProducerBrokerExchange producerExchange, org.apache.activemq.command.Message messageSend) throws Exception {
                    if (messageSend instanceof ActiveMQTextMessage &&
                        Boolean.TRUE.equals(messageSend.getProperty(RACE_PROPERTY)) &&
                        !(messageSend instanceof CoordinatedTextMessage)) {
                        messageSend = new CoordinatedTextMessage((ActiveMQTextMessage) messageSend);
                    }

                    super.send(producerExchange, messageSend);
                }
            };
        }
    }

    private static final class CoordinatedTextMessage extends ActiveMQTextMessage {

        private static final Method ACTIVE_MQ_TEXT_MESSAGE_COPY_METHOD = findCopyMethod();
        private static final long WAIT_TIMEOUT_SECONDS = 5;
        private static final ThreadLocal<Boolean> COPYING_BODY_SNAPSHOT = new ThreadLocal<Boolean>();

        private static volatile RaceState raceState = new RaceState();
        private final boolean coordinateRace;

        CoordinatedTextMessage(ActiveMQTextMessage source) throws Exception {
            this(true);
            invokeCopy(source, this);
        }

        private CoordinatedTextMessage(boolean coordinateRace) {
            this.coordinateRace = coordinateRace;
        }

        static void resetRaceState() {
            raceState = new RaceState();
        }

        static void assertRaceWasExercised() {
            RaceState state = raceState;
            assertTrue("copy() was never invoked on the coordinated message", state.copyStarted.getCount() == 0);
            assertTrue("beforeMarshall() was never invoked on the coordinated message", state.beforeMarshallStarted.getCount() == 0);
            assertTrue("copy() did not snapshot the text body before cloning", state.copyStoreContentStarted.getCount() == 0);
            assertTrue("beforeMarshall() never completed on the coordinated message", state.marshallingCompleted.getCount() == 0);
            assertTrue("Unexpected coordination failure: " + state.failure.get(), state.failure.get() == null);
        }

        @Override
        public org.apache.activemq.command.Message copy() {
            RaceState state = raceState;

            if (coordinateRace) {
                state.copyStarted.countDown();
                await(state.beforeMarshallStarted, "beforeMarshall to start", state);
            }

            CoordinatedTextMessage copy = new CoordinatedTextMessage(false);
            COPYING_BODY_SNAPSHOT.set(Boolean.TRUE);
            try {
                invokeCopy(this, copy);
            } finally {
                COPYING_BODY_SNAPSHOT.remove();
            }
            return copy;
        }

        @Override
        public void storeContent() {
            if (coordinateRace && Boolean.TRUE.equals(COPYING_BODY_SNAPSHOT.get())) {
                raceState.copyStoreContentStarted.countDown();
            }

            super.storeContent();
        }

        @Override
        public void beforeMarshall(WireFormat wireFormat) throws IOException {
            RaceState state = raceState;

            if (coordinateRace) {
                state.beforeMarshallStarted.countDown();
                await(state.copyStarted, "copy to start", state);
            }

            try {
                super.beforeMarshall(wireFormat);
            } finally {
                if (coordinateRace) {
                    state.marshallingCompleted.countDown();
                }
            }
        }

        private static Method findCopyMethod() {
            try {
                Method method = ActiveMQTextMessage.class.getDeclaredMethod("copy", ActiveMQTextMessage.class);
                method.setAccessible(true);
                return method;
            } catch (Exception e) {
                throw new RuntimeException("Failed to access ActiveMQTextMessage.copy(ActiveMQTextMessage)", e);
            }
        }

        private static void invokeCopy(ActiveMQTextMessage source, ActiveMQTextMessage target) {
            try {
                ACTIVE_MQ_TEXT_MESSAGE_COPY_METHOD.invoke(source, target);
            } catch (Exception e) {
                throw new RuntimeException("Failed to copy ActiveMQTextMessage into coordinated wrapper", e);
            }
        }

        private static void await(CountDownLatch latch, String action, RaceState state) {
            try {
                if (!latch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    AssertionError failure = new AssertionError("Timed out waiting for " + action);
                    state.failure.compareAndSet(null, failure);
                    throw failure;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                state.failure.compareAndSet(null, e);
                throw new AssertionError("Interrupted while waiting for " + action, e);
            }
        }

    }

    private static final class RaceState {
        private final CountDownLatch copyStarted = new CountDownLatch(1);
        private final CountDownLatch beforeMarshallStarted = new CountDownLatch(1);
        private final CountDownLatch copyStoreContentStarted = new CountDownLatch(1);
        private final CountDownLatch marshallingCompleted = new CountDownLatch(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
    }
}
