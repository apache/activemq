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

import javax.jms.DeliveryMode;

import junit.framework.Test;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.util.Wait;

public class DemandForwardingBridgeTest extends NetworkTestSupport {

    public ActiveMQDestination destination;
    public byte destinationType;
    public int deliveryMode;
    private DemandForwardingBridge bridge;

    public void initCombosForTestSendThenAddConsumer() {
        addCombinationValues("deliveryMode", new Object[] {new Integer(DeliveryMode.NON_PERSISTENT), new Integer(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {new Byte(ActiveMQDestination.QUEUE_TYPE)});
    }

    public void testSendThenAddConsumer() throws Exception {

        // Start a producer on local broker
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        destination = createDestinationInfo(connection1, connectionInfo1, destinationType);

        // Start a consumer on a remote broker
        final StubConnection connection2 = createRemoteConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);

        // Send the message to the local broker.
        connection1.send(createMessage(producerInfo, destination, deliveryMode));

        // Verify that the message stayed on the local broker.
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        connection1.send(consumerInfo1);
        Message m = receiveMessage(connection1);
        assertNotNull(m);
        // Close consumer to cause the message to rollback.
        connection1.send(consumerInfo1.createRemoveCommand());

        // Now create remote consumer that should cause message to move to this
        // remote consumer.
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        connection2.request(consumerInfo2);

        // Make sure the message was delivered via the remote.
        assertTrue("message was received", Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return receiveMessage(connection2) != null;
            }            
        }));
    }

    public void initCombosForTestAddConsumerThenSend() {
        addCombinationValues("deliveryMode", new Object[] {new Integer(DeliveryMode.NON_PERSISTENT), new Integer(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {new Byte(ActiveMQDestination.QUEUE_TYPE), new Byte(ActiveMQDestination.TOPIC_TYPE)});
    }

    public void testAddConsumerThenSend() throws Exception {

        // Start a producer on local broker
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        destination = createDestinationInfo(connection1, connectionInfo1, destinationType);

        // Start a consumer on a remote broker
        StubConnection connection2 = createRemoteConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo2, destination);
        connection2.send(consumerInfo);

        // Give demand forwarding bridge a chance to finish forwarding the
        // subscriptions.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }

        // Send the message to the local boker.
        connection1.request(createMessage(producerInfo, destination, deliveryMode));
        // Make sure the message was delivered via the remote.
        Message m = receiveMessage(connection2);
    }

    protected void setUp() throws Exception {
        super.setUp();
        NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
        config.setBrokerName("local");
        config.setDispatchAsync(false);
        bridge = new DemandForwardingBridge(config, createTransport(), createRemoteTransport());
        bridge.setBrokerService(broker);
        bridge.start();
    }

    protected void tearDown() throws Exception {
        bridge.stop();
        super.tearDown();
    }

    public static Test suite() {
        return suite(DemandForwardingBridgeTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
