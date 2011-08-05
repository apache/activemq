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

import java.net.Socket;
import java.util.Set;
import javax.management.ObjectName;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.JMXSupport;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransportConnectorMBeanTest {
    private static final Logger LOG = LoggerFactory.getLogger(TransportConnectorMBeanTest.class);

    BrokerService broker;

    @Test
    public void verifyRemoteAddressInMbeanName() throws Exception {
        doVerifyRemoteAddressInMbeanName(true);
    }

    @Test
    public void verifyRemoteAddressNotInMbeanName() throws Exception {
        doVerifyRemoteAddressInMbeanName(false);
    }

    private void doVerifyRemoteAddressInMbeanName(boolean allowRemoteAddress) throws Exception {
        createBroker(allowRemoteAddress);
        ActiveMQConnection connection = createConnection();
        Set<ObjectName> registeredMbeans = getRegisteredMbeans();
        assertTrue("found mbean with clientId", match(connection.getClientID(), registeredMbeans));
        assertEquals("presence of mbean with local port", allowRemoteAddress, match(extractLocalPort(connection), registeredMbeans));
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    private boolean match(String s, Set<ObjectName> registeredMbeans) {
        String encodedName = JMXSupport.encodeObjectNamePart(s);
        for (ObjectName name : registeredMbeans) {
            LOG.info("checking for match:" + encodedName + ", with: " + name.toString());
            if (name.toString().contains(encodedName)) {
                return true;
            }
        }
        return false;
    }

    private String extractLocalPort(ActiveMQConnection connection) throws Exception {
        Socket socket = (Socket) connection.getTransport().narrow(Socket.class);
        return String.valueOf(socket.getLocalPort());
    }

    private Set<ObjectName> getRegisteredMbeans() throws Exception {
        return broker.getManagementContext().queryNames(null, null);
    }

    private ActiveMQConnection createConnection() throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection)
                new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri()).createConnection();
        connection.start();
        return connection;
    }

    private void createBroker(boolean allowRemoteAddressInMbeanNames) throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.addConnector("tcp://localhost:0");
        broker.getManagementContext().setAllowRemoteAddressInMBeanNames(allowRemoteAddressInMbeanNames);
        broker.start();
    }

}
