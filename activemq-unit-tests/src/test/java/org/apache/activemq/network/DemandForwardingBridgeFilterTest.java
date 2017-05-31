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
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;

import java.util.Arrays;


public class DemandForwardingBridgeFilterTest extends NetworkTestSupport {

    private DemandForwardingBridge bridge;

    private StubConnection producerConnection;

    private ProducerInfo producerInfo;

    private StubConnection consumerConnection;

    private SessionInfo consumerSessionInfo;

    public void testWildcardOnExcludedDestination() throws Exception {

        NetworkBridgeConfiguration configuration = getDefaultBridgeConfiguration();

        configuration.setExcludedDestinations(Arrays.asList(ActiveMQDestination.createDestination("OTHER.>",
                ActiveMQDestination.TOPIC_TYPE)));
        configuration.setDynamicallyIncludedDestinations(Arrays.asList(ActiveMQDestination.createDestination(
                "TEST", ActiveMQDestination.QUEUE_TYPE)));

        configureAndStartBridge(configuration);

        assertReceiveMessageOn("TEST", ActiveMQDestination.QUEUE_TYPE);
        assertReceiveNoMessageOn("OTHER.T1", ActiveMQDestination.TOPIC_TYPE);
    }

    public void testWildcardOnTwoExcludedDestination() throws Exception {
        NetworkBridgeConfiguration configuration = getDefaultBridgeConfiguration();

        configuration.setExcludedDestinations(Arrays.asList(ActiveMQDestination.createDestination("OTHER.>", ActiveMQDestination.QUEUE_TYPE),
                ActiveMQDestination.createDestination("TEST.X1", ActiveMQDestination.QUEUE_TYPE)));
        configuration.setDynamicallyIncludedDestinations(Arrays.asList(ActiveMQDestination.createDestination(
                "TEST.X2", ActiveMQDestination.QUEUE_TYPE)));

        configureAndStartBridge(configuration);

        assertReceiveMessageOn("TEST.X2", ActiveMQDestination.QUEUE_TYPE);
        assertReceiveNoMessageOn("OTHER.X1", ActiveMQDestination.QUEUE_TYPE);
        assertReceiveNoMessageOn("TEST.X1", ActiveMQDestination.QUEUE_TYPE);
    }


    public void testWildcardOnDynamicallyIncludedDestination() throws Exception {

        NetworkBridgeConfiguration configuration = getDefaultBridgeConfiguration();

        configuration.setDynamicallyIncludedDestinations(Arrays.asList(ActiveMQDestination.createDestination("OTHER.>", ActiveMQDestination.QUEUE_TYPE),
                ActiveMQDestination.createDestination("TEST.X2", ActiveMQDestination.QUEUE_TYPE)));

        configureAndStartBridge(configuration);


        assertReceiveMessageOn("OTHER.X1", ActiveMQDestination.QUEUE_TYPE);
        assertReceiveMessageOn("TEST.X2", ActiveMQDestination.QUEUE_TYPE);
    }

    public void testDistinctTopicAndQueue() throws Exception {

        NetworkBridgeConfiguration configuration = getDefaultBridgeConfiguration();

        configuration.setExcludedDestinations(Arrays.asList(ActiveMQDestination.createDestination(">",
                ActiveMQDestination.TOPIC_TYPE)));
        configuration.setDynamicallyIncludedDestinations(Arrays.asList(ActiveMQDestination.createDestination(
                ">", ActiveMQDestination.QUEUE_TYPE)));

        configureAndStartBridge(configuration);

        assertReceiveMessageOn("TEST", ActiveMQDestination.QUEUE_TYPE);
        assertReceiveNoMessageOn("TEST", ActiveMQDestination.TOPIC_TYPE);
    }

    public void testListOfExcludedDestinationWithWildcard() throws Exception {

        NetworkBridgeConfiguration configuration = getDefaultBridgeConfiguration();

        configuration.setExcludedDestinations(Arrays.asList(ActiveMQDestination.createDestination("OTHER.>", ActiveMQDestination.TOPIC_TYPE),
                ActiveMQDestination.createDestination("TEST.*", ActiveMQDestination.TOPIC_TYPE)));
        configuration.setDynamicallyIncludedDestinations(Arrays.asList(ActiveMQDestination.createDestination(
                "TEST.X1", ActiveMQDestination.QUEUE_TYPE)));

        configureAndStartBridge(configuration);

        assertReceiveMessageOn("TEST.X1", ActiveMQDestination.QUEUE_TYPE);
        assertReceiveNoMessageOn("OTHER.T1", ActiveMQDestination.TOPIC_TYPE);
        assertReceiveNoMessageOn("OTHER.T2", ActiveMQDestination.TOPIC_TYPE);
    }

    public void testExcludeStaticDestinations() throws Exception {

        NetworkBridgeConfiguration configuration = getDefaultBridgeConfiguration();

        configuration.setExcludedDestinations(Arrays.asList(ActiveMQDestination.createDestination("TEST.X1", ActiveMQDestination.QUEUE_TYPE), ActiveMQDestination.createDestination("OTHER.X1", ActiveMQDestination.QUEUE_TYPE)));
        configuration.setStaticallyIncludedDestinations(Arrays.asList(ActiveMQDestination.createDestination(
                "TEST.>", ActiveMQDestination.QUEUE_TYPE), ActiveMQDestination.createDestination(
                "OTHER.X1", ActiveMQDestination.QUEUE_TYPE), ActiveMQDestination.createDestination(
                "OTHER.X2", ActiveMQDestination.QUEUE_TYPE)));

        configureAndStartBridge(configuration);

        assertReceiveNoMessageOn("TEST.X1", ActiveMQDestination.QUEUE_TYPE);
        assertReceiveMessageOn("TEST.X2", ActiveMQDestination.QUEUE_TYPE);
        assertReceiveNoMessageOn("OTHER.X1", ActiveMQDestination.QUEUE_TYPE);
        assertReceiveMessageOn("OTHER.X2", ActiveMQDestination.QUEUE_TYPE);
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

    protected void tearDown() throws Exception {
        bridge.stop();
        super.tearDown();
    }

    public static Test suite() {
        return suite(DemandForwardingBridgeFilterTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public NetworkBridgeConfiguration getDefaultBridgeConfiguration() {
        NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
        config.setBrokerName("local");
        config.setDispatchAsync(false);
        return config;
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

}