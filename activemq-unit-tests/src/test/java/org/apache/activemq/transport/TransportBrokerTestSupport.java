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
package org.apache.activemq.transport;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLSocket;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTest;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.transport.nio.NIOSSLTransport;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.NioSslTestUtil;
import org.apache.activemq.util.Wait;

public abstract class TransportBrokerTestSupport extends BrokerTest {

    protected TransportConnector connector;
    private ArrayList<StubConnection> connections = new ArrayList<StubConnection>();
    protected String enabledProtocols = null;


    @Override
    protected void setUp() throws Exception {
        enabledProtocols = null;
        super.setUp();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService service = super.createBroker();
        connector = service.addConnector(getBindLocation());
        return service;
    }

    protected abstract String getBindLocation();

    @Override
    protected void tearDown() throws Exception {
        for (Iterator<StubConnection> iter = connections.iterator(); iter.hasNext();) {
            try {
                StubConnection connection = iter.next();
                connection.stop();
                iter.remove();
            } catch (Exception ex) {
            }
        }

        if (connector != null) {
            try {
                connector.stop();
            } catch (Exception ex) {
            }
        }

        super.tearDown();
    }

    protected URI getBindURI() throws URISyntaxException {
        return new URI(getBindLocation());
    }

    @Override
    protected StubConnection createConnection() throws Exception {
        URI bindURI = getBindURI();

        // Note: on platforms like OS X we cannot bind to the actual hostname, so we
        // instead use the original host name (typically localhost) to bind to

        URI actualURI = connector.getServer().getConnectURI();
        URI connectURI = new URI(actualURI.getScheme(), actualURI.getUserInfo(), bindURI.getHost(), actualURI.getPort(), actualURI.getPath(), bindURI
                .getQuery(), bindURI.getFragment());

        Transport transport = TransportFactory.connect(connectURI);
        StubConnection connection = new StubConnection(transport);
        connections.add(connection);
        return connection;
    }


    protected void testSslHandshakeRenegotiation(String protocol) throws Exception {
        this.enabledProtocols = protocol;

        // Start a producer
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        destination = createDestinationInfo(connection, connectionInfo, ActiveMQDestination.QUEUE_TYPE);

        TcpTransport tcpTransport = connection.getTransport().narrow(TcpTransport.class);
        Field socketField = TcpTransport.class.getDeclaredField("socket");
        socketField.setAccessible(true);
        SSLSocket socket = (SSLSocket) socketField.get(tcpTransport);
        assertEquals(protocol, socket.getSession().getProtocol());

        // trigger a bunch of updates (either TLSv1.2 full updates or TLSv1.3 key updates)
        for (int i = 0; i < 10; i++) {
            try {
                socket.startHandshake();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // give some time for the handshake updates
        Thread.sleep(100);

        // check status advances if NIOSSL, then continue
        // below to verify transports are not stuck
        checkHandshakeStatusAdvances(socket);

        // Send a message to the broker.
        connection.send(createMessage(producerInfo, destination, deliveryMode));

        // Start the consumer
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Make sure the message was delivered.
        Message m = receiveMessage(connection);
        assertNotNull(m);
    }

    // This only applies to NIO SSL
    protected void checkHandshakeStatusAdvances(SSLSocket socket) throws Exception {
        TransportConnector connector = broker.getTransportConnectorByScheme(getBindURI().getScheme());
        NioSslTestUtil.checkHandshakeStatusAdvances(connector, socket);
    }

}
