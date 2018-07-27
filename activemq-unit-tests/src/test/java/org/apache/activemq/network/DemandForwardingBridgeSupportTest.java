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
package org.apache.activemq.network;

import junit.framework.Test;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.command.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;

public class DemandForwardingBridgeSupportTest extends NetworkTestSupport {

    private DemandForwardingBridge bridge;
    private StubConnection producerConnection;
    private ProducerInfo producerInfo;
    private StubConnection consumerConnection;
    private SessionInfo consumerSessionInfo;


    public void testOverflow() throws Exception {
        NetworkBridgeConfiguration configuration = getDefaultBridgeConfiguration();

        configuration.setExcludedDestinations(Arrays.asList(ActiveMQDestination.createDestination("OTHER.>",
                ActiveMQDestination.TOPIC_TYPE)));
        configuration.setDynamicallyIncludedDestinations(Arrays.asList(ActiveMQDestination.createDestination(
                "TEST", ActiveMQDestination.QUEUE_TYPE)));

        configureAndStartBridge(configuration);
        assertReceiveMessageOn("TEST", ActiveMQDestination.QUEUE_TYPE);
        assertReceiveNoMessageOn("OTHER.T1", ActiveMQDestination.TOPIC_TYPE);
    }

    private void assertReceiveMessageOn(String destinationName, byte destinationType) throws Exception,
            InterruptedException {

        ActiveMQDestination destination = ActiveMQDestination.createDestination(destinationName, destinationType);

        // Send the message to the local broker.
        producerConnection.send(createMessage(producerInfo, destination, destinationType));

        // Make sure the message was delivered via the remote.
        Message m = createConsumerAndReceiveMessage(destination);

        assertNotNull(m);
    }

    private void assertReceiveNoMessageOn(String destinationName, byte destinationType) throws Exception,
            InterruptedException {

        ActiveMQDestination destination = ActiveMQDestination.createDestination(destinationName, destinationType);

        // Send the message to the local broker.
        producerConnection.send(createMessage(producerInfo, destination, destinationType));

        // Make sure the message was delivered via the remote.
        Message m = createConsumerAndReceiveMessage(destination);
        assertNull(m);
    }

    private Message createConsumerAndReceiveMessage(ActiveMQDestination destination) throws Exception {
        // Now create remote consumer that should cause message to move to this
        // remote consumer.
        ConsumerInfo consumerInfo = createConsumerInfo(consumerSessionInfo, destination);
        consumerConnection.send(consumerInfo);

        Message m = receiveMessage(consumerConnection);
        return m;
    }

    private void configureAndStartBridge(NetworkBridgeConfiguration configuration) throws Exception {
        bridge = new DemandForwardingBridge(configuration, createTransport(), createRemoteTransport());
        bridge.setBrokerService(broker);
        bridge.setDynamicallyIncludedDestinations(configuration.getDynamicallyIncludedDestinations().toArray(
                new ActiveMQDestination[configuration.getDynamicallyIncludedDestinations().size()]
        ));
        bridge.setExcludedDestinations(configuration.getExcludedDestinations().toArray(
                new ActiveMQDestination[configuration.getExcludedDestinations().size()]
        ));
        bridge.setStaticallyIncludedDestinations(configuration.getStaticallyIncludedDestinations().toArray(
                new ActiveMQDestination[configuration.getStaticallyIncludedDestinations().size()]
        ));
        bridge.start();
    }

    public NetworkBridgeConfiguration getDefaultBridgeConfiguration() {
        NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
        config.setBrokerName("local");
        config.setDispatchAsync(false);
        return config;
    }

    // create sockets with max waiting value accepted
    @Override
    protected String getLocalURI() {
        int port = findFreePort();
        return String.format("tcp://localhost:%d?connectionTimeout=2147483647", port);
    }

    @Override
    protected String getRemoteURI() {
        int port = findFreePort();
        return String.format("tcp://localhost:%d?connectionTimeout=2147483647",port);
    }

    private static int findFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            int port = socket.getLocalPort();
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore IOException on close()
            }
            return port;
        } catch (IOException e) {
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
        }
        throw new IllegalStateException("Could not find a free TCP/IP port to start embedded Jetty HTTP Server on");
    }


    @Override
    protected void setUp() throws Exception {
        super.setUp();

        producerConnection = createConnection();
        ConnectionInfo producerConnectionInfo = createConnectionInfo();
        SessionInfo producerSessionInfo = createSessionInfo(producerConnectionInfo);
        producerInfo = createProducerInfo(producerSessionInfo);
        producerConnection.send(producerConnectionInfo);
        producerConnection.send(producerSessionInfo);
        producerConnection.send(producerInfo);

        consumerConnection = createRemoteConnection();
        ConnectionInfo consumerConnectionInfo = createConnectionInfo();
        consumerSessionInfo = createSessionInfo(consumerConnectionInfo);
        consumerConnection.send(consumerConnectionInfo);
        consumerConnection.send(consumerSessionInfo);
    }


    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        return suite(DemandForwardingBridgeSupportTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
