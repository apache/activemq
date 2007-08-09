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
package org.apache.activemq.transport.fanout;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.DeliveryMode;

import junit.framework.Test;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.network.NetworkTestSupport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportFilter;
import org.apache.activemq.transport.mock.MockTransport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FanoutTransportBrokerTest extends NetworkTestSupport {

    private static final Log log = LogFactory.getLog(FanoutTransportBrokerTest.class);

    public ActiveMQDestination destination;
    public int deliveryMode;

    private String remoteURI = "tcp://localhost:0?wireFormat.tcpNoDelayEnabled=true";

    public static Test suite() {
        return suite(FanoutTransportBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombosForTestPublisherFansout() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST")});
    }

    public void xtestPublisherFansout() throws Exception {

        // Start a normal consumer on the local broker
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.request(consumerInfo1);

        // Start a normal consumer on a remote broker
        StubConnection connection2 = createRemoteConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        // Start a fanout publisher.
        log.info("Starting the fanout connection.");
        StubConnection connection3 = createFanoutConnection();
        ConnectionInfo connectionInfo3 = createConnectionInfo();
        SessionInfo sessionInfo3 = createSessionInfo(connectionInfo3);
        ProducerInfo producerInfo3 = createProducerInfo(sessionInfo3);
        connection3.send(connectionInfo3);
        connection3.send(sessionInfo3);
        connection3.send(producerInfo3);

        // Send the message using the fail over publisher.
        connection3.request(createMessage(producerInfo3, destination, deliveryMode));

        assertNotNull(receiveMessage(connection1));
        assertNoMessagesLeft(connection1);

        assertNotNull(receiveMessage(connection2));
        assertNoMessagesLeft(connection2);

    }

    public void initCombosForTestPublisherWaitsForServerToBeUp() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST")});
    }

    public void testPublisherWaitsForServerToBeUp() throws Exception {

        // Start a normal consumer on the local broker
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.request(consumerInfo1);

        // Start a normal consumer on a remote broker
        StubConnection connection2 = createRemoteConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        // Start a fanout publisher.
        log.info("Starting the fanout connection.");
        final StubConnection connection3 = createFanoutConnection();
        ConnectionInfo connectionInfo3 = createConnectionInfo();
        SessionInfo sessionInfo3 = createSessionInfo(connectionInfo3);
        final ProducerInfo producerInfo3 = createProducerInfo(sessionInfo3);
        connection3.send(connectionInfo3);
        connection3.send(sessionInfo3);
        connection3.send(producerInfo3);

        // Send the message using the fail over publisher.
        connection3.request(createMessage(producerInfo3, destination, deliveryMode));

        assertNotNull(receiveMessage(connection1));
        assertNoMessagesLeft(connection1);

        assertNotNull(receiveMessage(connection2));
        assertNoMessagesLeft(connection2);

        final CountDownLatch publishDone = new CountDownLatch(1);

        // The MockTransport is on the remote connection.
        // Slip in a new transport filter after the MockTransport
        MockTransport mt = (MockTransport)connection3.getTransport().narrow(MockTransport.class);
        mt.install(new TransportFilter(mt.getNext()) {
            public void oneway(Object command) throws IOException {
                log.info("Dropping: " + command);
                // just eat it! to simulate a recent failure.
            }
        });

        // Send a message (async) as this will block
        new Thread() {
            public void run() {
                // Send the message using the fail over publisher.
                try {
                    connection3.request(createMessage(producerInfo3, destination, deliveryMode));
                } catch (Throwable e) {
                    e.printStackTrace();
                }
                publishDone.countDown();
            }
        }.start();

        // Assert that we block:
        assertFalse(publishDone.await(3, TimeUnit.SECONDS));

        // Restart the remote server. State should be re-played and the publish
        // should continue.
        remoteURI = remoteConnector.getServer().getConnectURI().toString();
        restartRemoteBroker();

        // This should reconnect, and resend
        assertTrue(publishDone.await(10, TimeUnit.SECONDS));

    }

    protected String getLocalURI() {
        return "tcp://localhost:0?wireFormat.tcpNoDelayEnabled=true";
    }

    protected String getRemoteURI() {
        return remoteURI;
    }

    protected StubConnection createFanoutConnection() throws Exception {
        URI fanoutURI = new URI("fanout://static://(" + connector.getServer().getConnectURI() + "," + "mock://" + remoteConnector.getServer().getConnectURI() + ")");
        Transport transport = TransportFactory.connect(fanoutURI);
        StubConnection connection = new StubConnection(transport);
        connections.add(connection);
        return connection;
    }

}
