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
package org.apache.activemq.broker.jmx;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import jakarta.jms.Connection;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Regression tests for ConnectionView JMX attribute accessors.
 */
public class ConnectionViewTest {

    private BrokerService broker;
    private URI brokerConnectURI;

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);

        TransportConnector connector = broker.addConnector(new TransportConnector());
        connector.setUri(new URI("tcp://0.0.0.0:0"));
        connector.setName("tcp");

        broker.start();
        broker.waitUntilStarted();

        brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testGetWireFormatInfoWhenPresent() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.start();
        try {
            ConnectionView view = new ConnectionView(getTransportConnection());
            String info = view.getWireFormatInfo();
            assertNotNull(info);
            assertFalse("WireFormatInfo not available".equals(info));
        } finally {
            connection.stop();
        }
    }

    @Test
    public void testGetWireFormatInfoWhenRemoteInfoNull() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.start();
        try {
            TransportConnection transportConnection = getTransportConnection();
            Field field = TransportConnection.class.getDeclaredField("wireFormatInfo");
            field.setAccessible(true);
            field.set(transportConnection, null);

            ConnectionView view = new ConnectionView(transportConnection);
            assertEquals("WireFormatInfo not available", view.getWireFormatInfo());
        } finally {
            connection.stop();
        }
    }

    private TransportConnection getTransportConnection() {
        CopyOnWriteArrayList<TransportConnection> connections =
                broker.getConnectorByName("tcp").getConnections();
        assertEquals(1, connections.size());
        return connections.get(0);
    }
}
