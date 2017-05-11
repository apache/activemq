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
package org.apache.activemq.transport.amqp.interop;

import static org.apache.activemq.transport.amqp.AmqpSupport.CONNECTION_OPEN_FAILED;
import static org.apache.activemq.transport.amqp.AmqpSupport.PLATFORM;
import static org.apache.activemq.transport.amqp.AmqpSupport.PRODUCT;
import static org.apache.activemq.transport.amqp.AmqpSupport.VERSION;
import static org.apache.activemq.transport.amqp.AmqpSupport.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.amqp.AmqpSupport;
import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpClientTestSupport;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpMessage;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSender;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.activemq.transport.amqp.client.AmqpValidator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test broker handling of AMQP connections with various configurations.
 */
@RunWith(Parameterized.class)
public class AmqpConnectionsTest extends AmqpClientTestSupport {

    private static final Symbol QUEUE_PREFIX = Symbol.valueOf("queue-prefix");
    private static final Symbol TOPIC_PREFIX = Symbol.valueOf("topic-prefix");
    private static final Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
    private static final Symbol DELAYED_DELIVERY = Symbol.valueOf("DELAYED_DELIVERY");

    @Parameters(name="{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"amqp", false},
            {"amqp+ws", false},
            {"amqp+ssl", true},
            {"amqp+wss", true}
        });
    }

    public AmqpConnectionsTest(String connectorScheme, boolean secure) {
        super(connectorScheme, secure);
    }

    @Override
    protected String getAdditionalConfig() {
        return "&wireFormat.maxAmqpFrameSize=1048576";
    }

    @Test(timeout = 60000)
    public void testCanConnect() throws Exception {
        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        AmqpConnection connection = trackConnection(client.connect());
        assertNotNull(connection);

        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());


        Connection protonConnection = connection.getConnection();
        org.apache.qpid.proton.engine.Transport protonTransport = protonConnection.getTransport();

        protonTransport.getChannelMax();

        assertNull(protonTransport.head());
        assertNull(protonTransport.tail());
        assertNull(protonTransport.getInputBuffer());
        assertNull(protonTransport.getOutputBuffer());

        try {
            protonTransport.bind(protonConnection);
            fail("Should not be able to mutate");
        } catch (UnsupportedOperationException e) {}

        try {
            protonTransport.close();
            fail("Should not be able to mutate");
        } catch (UnsupportedOperationException e) {}

        try {
            protonTransport.setChannelMax(1);
            fail("Should not be able to mutate");
        } catch (UnsupportedOperationException e) {}

        connection.close();

        assertEquals(0, getProxyToBroker().getCurrentConnectionsCount());
    }

    @Test(timeout = 60000)
    public void testConnectionCarriesExpectedCapabilities() throws Exception {
        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        client.setValidator(new AmqpValidator() {

            @Override
            public void inspectOpenedResource(Connection connection) {

                Symbol[] offered = connection.getRemoteOfferedCapabilities();
                if (!contains(offered, ANONYMOUS_RELAY)) {
                    markAsInvalid("Broker did not indicate it support anonymous relay");
                }

                if (!contains(offered, DELAYED_DELIVERY)) {
                    markAsInvalid("Broker did not indicate it support delayed message delivery");
                }

                Map<Symbol, Object> properties = connection.getRemoteProperties();
                if (!properties.containsKey(QUEUE_PREFIX)) {
                    markAsInvalid("Broker did not send a queue prefix value");
                }

                if (!properties.containsKey(TOPIC_PREFIX)) {
                    markAsInvalid("Broker did not send a queue prefix value");
                }

                if (!properties.containsKey(PRODUCT)) {
                    markAsInvalid("Broker did not send a queue product name value");
                }

                if (!properties.containsKey(VERSION)) {
                    markAsInvalid("Broker did not send a queue version value");
                }

                if (!properties.containsKey(PLATFORM)) {
                    markAsInvalid("Broker did not send a queue platform name value");
                } else {
                    LOG.info("Broker platform = {}", properties.get(PLATFORM));
                }
            }
        });

        AmqpConnection connection = trackConnection(client.connect());
        assertNotNull(connection);

        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());

        connection.getStateInspector().assertValid();
        connection.close();

        assertEquals(0, getProxyToBroker().getCurrentConnectionsCount());
    }

    @Test(timeout = 60000)
    public void testConnectionCarriesContainerId() throws Exception {
        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        client.setValidator(new AmqpValidator() {

            @Override
            public void inspectOpenedResource(Connection connection) {
                String remoteContainer = connection.getRemoteContainer();
                if (remoteContainer == null || !remoteContainer.equals(brokerService.getBrokerName())) {
                    markAsInvalid("Broker did not send a valid container ID");
                } else {
                    LOG.info("Broker container ID = {}", remoteContainer);
                }
            }
        });

        AmqpConnection connection = trackConnection(client.connect());
        assertNotNull(connection);

        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());

        connection.getStateInspector().assertValid();
        connection.close();

        assertEquals(0, getProxyToBroker().getCurrentConnectionsCount());
    }

    @Test(timeout = 60000)
    public void testCanConnectWithDifferentContainerIds() throws Exception {
        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        AmqpConnection connection1 = trackConnection(client.createConnection());
        AmqpConnection connection2 = trackConnection(client.createConnection());

        connection1.setContainerId(getTestName() + "-Client:1");
        connection2.setContainerId(getTestName() + "-Client:2");

        connection1.connect();
        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());

        connection2.connect();
        assertEquals(2, getProxyToBroker().getCurrentConnectionsCount());

        connection1.close();
        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());

        connection2.close();
        assertEquals(0, getProxyToBroker().getCurrentConnectionsCount());
    }

    @Test(timeout = 60000)
    public void testCannotConnectWithSameContainerId() throws Exception {
        AmqpClient client = createAmqpClient();
        assertNotNull(client);

        AmqpConnection connection1 = trackConnection(client.createConnection());
        AmqpConnection connection2 = trackConnection(client.createConnection());

        connection1.setContainerId(getTestName());
        connection2.setContainerId(getTestName());

        connection1.connect();
        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());

        connection2.setStateInspector(new AmqpValidator() {

            @Override
            public void inspectOpenedResource(Connection connection) {
                if (!connection.getRemoteProperties().containsKey(CONNECTION_OPEN_FAILED)) {
                    markAsInvalid("Broker did not set connection establishment failed property");
                }
            }

            @Override
            public void inspectClosedResource(Connection connection) {
                ErrorCondition remoteError = connection.getRemoteCondition();
                if (remoteError == null || remoteError.getCondition() == null) {
                    markAsInvalid("Broker did not add error condition for duplicate client ID");
                } else {
                    if (!remoteError.getCondition().equals(AmqpError.INVALID_FIELD)) {
                        markAsInvalid("Broker did not set condition to " + AmqpError.INVALID_FIELD);
                    }

                    if (!remoteError.getCondition().equals(AmqpError.INVALID_FIELD)) {
                        markAsInvalid("Broker did not set condition to " + AmqpError.INVALID_FIELD);
                    }
                }

                // Validate the info map contains a hint that the container/client id was the problem
                Map<?, ?> infoMap = remoteError.getInfo();
                if (infoMap == null) {
                    markAsInvalid("Broker did not set an info map on condition");
                } else if (!infoMap.containsKey(AmqpSupport.INVALID_FIELD)) {
                    markAsInvalid("Info map does not contain expected key");
                } else {
                    Object value = infoMap.get(AmqpSupport.INVALID_FIELD);
                    if(!AmqpSupport.CONTAINER_ID.equals(value)) {
                        markAsInvalid("Info map does not contain expected value: " + value);
                    }
                }
            }
        });

        try {
            connection2.connect();
            fail("Should not be able to connect with same container Id.");
        } catch (Exception ex) {
            LOG.info("Second connection with same container Id failed as expected.");
        }

        connection2.getStateInspector().assertValid();

        assertEquals(1, getProxyToBroker().getCurrentConnectionsCount());

        connection1.close();
        assertEquals(0, getProxyToBroker().getCurrentConnectionsCount());
    }

    @Test(timeout = 60000)
    public void testSimpleSendOneReceive() throws Exception {

        AmqpClient client = createAmqpClient();
        AmqpConnection connection = trackConnection(client.connect());
        AmqpSession session = connection.createSession();

        AmqpSender sender = session.createSender("queue://" + getTestName());
        AmqpReceiver receiver = session.createReceiver("queue://" + getTestName());

        AmqpMessage message = new AmqpMessage();

        final int PAYLOAD_SIZE = 1024 * 1024;

        byte[] payload = new byte[PAYLOAD_SIZE];
        for (int i = 0; i < PAYLOAD_SIZE; i++) {
            payload[i] = (byte) (i % PAYLOAD_SIZE);
        }

        message.setMessageId("msg" + 1);
        message.setMessageAnnotation("serialNo", 1);
        message.setBytes(payload);

        sender.send(message);
        sender.close();

        LOG.info("Attempting to read message with receiver");
        receiver.flow(2);
        AmqpMessage received = receiver.receive(10, TimeUnit.SECONDS);
        assertNotNull("Should have read message", received);
        assertEquals("msg1", received.getMessageId());
        received.accept();

        receiver.close();

        connection.close();
    }
}
