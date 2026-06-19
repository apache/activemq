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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.TopicRegion;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.Wait;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.util.IdGenerator;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.util.BufferingResponseListener;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AjaxTwoBrokerTopicNetworkTest {

    private static final Logger LOG = LoggerFactory.getLogger(AjaxTwoBrokerTopicNetworkTest.class);
    private static final String DESTINATION = "topic://TEST.TEXT.FORWARD";

    private BrokerService brokerA;
    private BrokerService brokerB;
    private Server server;
    private Connection consumerConnection;
    private TestActiveMQConnectionFactory webClientFactory;
    private int port;

    @Before
    public void setUp() throws Exception {
        resetWebClientFactory();

        brokerB = createBroker("BrokerB");
        brokerB.start();
        brokerB.waitUntilStarted();

        brokerA = createBroker("BrokerA");
        URI remoteUri = ((TransportConnector) brokerB.getTransportConnectors().get(0)).getPublishableConnectURI();
        brokerA.addNetworkConnector("static:(" + remoteUri + ")");
        brokerA.start();
        brokerA.waitUntilStarted();

        assertTrue("bridge formed", Wait.waitFor(() -> !brokerA.getNetworkConnectors().get(0).activeBridges().isEmpty()));

        webClientFactory = new TestActiveMQConnectionFactory(brokerA.getVmConnectorURI().toString());
        webClientFactory.setCopyMessageOnSend(false);

        port = getPort();
        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);

        WebAppContext context = new WebAppContext();
        context.setResourceBase("src/main/webapp");
        context.setContextPath("/");
        context.setAttribute(WebClient.CONNECTION_FACTORY_ATTRIBUTE, webClientFactory);
        context.setServer(server);

        server.setHandler(context);
        server.setConnectors(new Connector[] {connector});
        server.start();

        waitForJettySocketToAccept("http://localhost:" + port);
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (consumerConnection != null) {
                consumerConnection.close();
            }
            if (server != null) {
                server.stop();
            }
            if (brokerA != null) {
                brokerA.stop();
                brokerA.waitUntilStopped();
            }
            if (brokerB != null) {
                brokerB.stop();
                brokerB.waitUntilStopped();
            }
        } finally {
            resetWebClientFactory();
        }
    }

    @Test(timeout = 30 * 1000)
    public void testAjaxTopicSendRetainsBodyAcrossNetworkBridge() throws Exception {
        ActiveMQConnectionFactory consumerFactory = new ActiveMQConnectionFactory(brokerB.getVmConnectorURI());
        consumerConnection = consumerFactory.createConnection();
        consumerConnection.start();

        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(consumerSession.createTopic("TEST.TEXT.FORWARD"));

        TopicRegion topicRegion = (TopicRegion) ((RegionBroker) brokerA.getRegionBroker()).getTopicRegion();
        assertTrue("network demand propagated", Wait.waitFor(() -> topicRegion.getSubscriptions().size() >= 1));

        HttpClient httpClient = new HttpClient();
        httpClient.start();
        try {
            postAjaxSend(httpClient, "destination=" + DESTINATION + "&type=send&message=payload");

            TextMessage received = (TextMessage) consumer.receive(TimeUnit.SECONDS.toMillis(5));
            assertNotNull("message not received", received);
            assertEquals("payload", received.getText());

            CoordinatedActiveMQTextMessage sentMessage = webClientFactory.getLastCreatedMessage();
            assertNotNull("test message not captured", sentMessage);
            assertTrue("ajax ingress should marshal before send", sentMessage.wasBeforeMarshalled());
            assertFalse("network forwarding copy should not happen before marshalling",
                sentMessage.wasCopiedBeforeMarshall());
        } finally {
            httpClient.stop();
        }
    }

    private BrokerService createBroker(String name) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(name);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:0");
        return broker;
    }

    private void postAjaxSend(HttpClient httpClient, String content) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<Throwable>();

        httpClient.newRequest("http://localhost:" + port + "/amq")
            .method(HttpMethod.POST)
            .header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
            .content(new InputStreamContentProvider(new ByteArrayInputStream(content.getBytes())))
            .send(new BufferingResponseListener() {
                @Override
                public void onComplete(org.eclipse.jetty.client.api.Result result) {
                    if (result.isFailed()) {
                        failure.set(result.getFailure());
                    } else if (result.getResponse().getStatus() != 200) {
                        failure.set(new AssertionError("unexpected status: " + result.getResponse().getStatus()));
                    }
                    latch.countDown();
                }
            });

        assertTrue("ajax send completed", latch.await(10, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("ajax send failed", failure.get());
        }
    }

    private static void resetWebClientFactory() throws Exception {
        Field field = WebClient.class.getDeclaredField("factory");
        field.setAccessible(true);
        field.set(null, null);
    }

    private int getPort() {
        ServerSocket socket = null;
        try {
            socket = ServerSocketFactory.getDefault().createServerSocket(0);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new AssertionError(e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private void waitForJettySocketToAccept(String bindLocation) throws Exception {
        URL url = new URL(bindLocation);
        assertTrue("Jetty endpoint is available", Wait.waitFor(() -> {
            try {
                java.net.Socket socket = SocketFactory.getDefault().createSocket(url.getHost(), url.getPort());
                socket.close();
                return true;
            } catch (Exception e) {
                LOG.warn("verify jetty available, failed to connect to {}", url, e);
                return false;
            }
        }, 60 * 1000));
    }

    private static final class TestActiveMQConnectionFactory extends ActiveMQConnectionFactory {
        private final AtomicReference<CoordinatedActiveMQTextMessage> lastCreatedMessage =
            new AtomicReference<CoordinatedActiveMQTextMessage>();

        private TestActiveMQConnectionFactory(String brokerURL) {
            super(brokerURL);
        }

        @Override
        protected ActiveMQConnection createActiveMQConnection(Transport transport, JMSStatsImpl stats) throws Exception {
            return new CoordinatedConnection(this, transport, stats);
        }

        private IdGenerator clientIdGenerator() {
            return getClientIdGenerator();
        }

        private IdGenerator connectionIdGenerator() {
            return getConnectionIdGenerator();
        }

        private void setLastCreatedMessage(CoordinatedActiveMQTextMessage message) {
            lastCreatedMessage.set(message);
        }

        private CoordinatedActiveMQTextMessage getLastCreatedMessage() {
            return lastCreatedMessage.get();
        }
    }

    private static final class CoordinatedConnection extends ActiveMQConnection {
        private final TestActiveMQConnectionFactory factory;

        private CoordinatedConnection(TestActiveMQConnectionFactory factory, Transport transport, JMSStatsImpl stats)
            throws Exception {
            super(transport, factory.clientIdGenerator(), factory.connectionIdGenerator(), stats);
            this.factory = factory;
        }

        @Override
        public Session createSession(boolean transacted, int acknowledgeMode) throws jakarta.jms.JMSException {
            checkClosedOrFailed();
            ensureConnectionInfoSent();
            if (!transacted) {
                if (acknowledgeMode == Session.SESSION_TRANSACTED) {
                    throw new jakarta.jms.JMSException(
                        "acknowledgeMode SESSION_TRANSACTED cannot be used for an non-transacted Session");
                } else if (acknowledgeMode < Session.SESSION_TRANSACTED
                        || acknowledgeMode > ActiveMQSession.MAX_ACK_CONSTANT) {
                    throw new jakarta.jms.JMSException(
                        "invalid acknowledgeMode: " + acknowledgeMode
                            + ". Valid values are Session.AUTO_ACKNOWLEDGE (1), "
                            + "Session.CLIENT_ACKNOWLEDGE (2), Session.DUPS_OK_ACKNOWLEDGE (3), "
                            + "ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE (4) or for transacted sessions "
                            + "Session.SESSION_TRANSACTED (0)");
                }
            }

            return new CoordinatedSession(this, factory, getNextSessionId(),
                transacted ? Session.SESSION_TRANSACTED : acknowledgeMode, isDispatchAsync(), isAlwaysSessionAsync());
        }
    }

    private static final class CoordinatedSession extends ActiveMQSession {
        private final TestActiveMQConnectionFactory factory;

        private CoordinatedSession(ActiveMQConnection connection, TestActiveMQConnectionFactory factory,
            SessionId sessionId, int acknowledgeMode, boolean asyncDispatch, boolean sessionAsyncDispatch)
            throws jakarta.jms.JMSException {
            super(connection, sessionId, acknowledgeMode, asyncDispatch, sessionAsyncDispatch);
            this.factory = factory;
        }

        @Override
        public TextMessage createTextMessage() throws jakarta.jms.JMSException {
            CoordinatedActiveMQTextMessage message = new CoordinatedActiveMQTextMessage();
            configureMessage(message);
            factory.setLastCreatedMessage(message);
            return message;
        }

        @Override
        public TextMessage createTextMessage(String text) throws jakarta.jms.JMSException {
            CoordinatedActiveMQTextMessage message = new CoordinatedActiveMQTextMessage();
            message.setText(text);
            configureMessage(message);
            factory.setLastCreatedMessage(message);
            return message;
        }
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
                }, "ajax-servlet-marshaller");

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
