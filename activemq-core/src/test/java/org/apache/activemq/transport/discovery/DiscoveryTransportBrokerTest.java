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
package org.apache.activemq.transport.discovery;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.DeliveryMode;

import junit.framework.Test;

import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.network.NetworkTestSupport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DiscoveryTransportBrokerTest extends NetworkTestSupport {

    static final private Log log = LogFactory.getLog(DiscoveryTransportBrokerTest.class);

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
    }

    public void testPublisherFailsOver() throws Exception {
        ActiveMQDestination destination = new ActiveMQQueue("TEST");
        int deliveryMode = DeliveryMode.NON_PERSISTENT;

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

        // Start a failover publisher.
        StubConnection connection3 = createFailoverConnection();
        ConnectionInfo connectionInfo3 = createConnectionInfo();
        SessionInfo sessionInfo3 = createSessionInfo(connectionInfo3);
        ProducerInfo producerInfo3 = createProducerInfo(sessionInfo3);
        connection3.send(connectionInfo3);
        connection3.send(sessionInfo3);
        connection3.send(producerInfo3);

        // Send the message using the fail over publisher.
        connection3.request(createMessage(producerInfo3, destination, deliveryMode));

        // The message will be sent to one of the brokers.
        FailoverTransport ft = (FailoverTransport)connection3.getTransport().narrow(FailoverTransport.class);

        // See which broker we were connected to.
        StubConnection connectionA;
        StubConnection connectionB;
        TransportConnector serverA;
        if (connector.getServer().getConnectURI().equals(ft.getConnectedTransportURI())) {
            connectionA = connection1;
            connectionB = connection2;
            serverA = connector;
        } else {
            connectionA = connection2;
            connectionB = connection1;
            serverA = remoteConnector;
        }

        assertNotNull(receiveMessage(connectionA));
        assertNoMessagesLeft(connectionB);

        // Dispose the server so that it fails over to the other server.
        log.info("Disconnecting active server");
        serverA.stop();

        log.info("Sending request that should failover");
        connection3.request(createMessage(producerInfo3, destination, deliveryMode));

        assertNotNull(receiveMessage(connectionB));
        assertNoMessagesLeft(connectionA);

    }

    protected String getLocalURI() {
        return "tcp://localhost:0?wireFormat.tcpNoDelayEnabled=true";
    }

    protected String getRemoteURI() {
        return "tcp://localhost:0?wireFormat.tcpNoDelayEnabled=true";
    }

    protected TransportConnector createConnector() throws Exception, IOException, URISyntaxException {
        TransportConnector x = super.createConnector();
        x.setDiscoveryUri(new URI(getDiscoveryUri()));
        return x;
    }

    protected String getDiscoveryUri() {
        return "multicast://default";
    }

    protected TransportConnector createRemoteConnector() throws Exception, IOException, URISyntaxException {
        TransportConnector x = super.createRemoteConnector();
        x.setDiscoveryUri(new URI(getDiscoveryUri()));
        return x;
    }

    protected StubConnection createFailoverConnection() throws Exception {
        URI failoverURI = new URI("discovery:" + getDiscoveryUri());
        Transport transport = TransportFactory.connect(failoverURI);
        StubConnection connection = new StubConnection(transport);
        connections.add(connection);
        return connection;
    }

    public static Test suite() {
        return suite(DiscoveryTransportBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
