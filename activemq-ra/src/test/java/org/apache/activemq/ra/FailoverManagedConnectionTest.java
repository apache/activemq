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
package org.apache.activemq.ra;

import java.util.HashSet;

import javax.resource.spi.ManagedConnection;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FailoverManagedConnectionTest {

    private static final String BROKER_TRANSPORT = "tcp://localhost:61616";
    private static final String BROKER_URL = "failover://" + BROKER_TRANSPORT;
    private static final String KAHADB_DIRECTORY = "target/activemq-data/";

    private ActiveMQManagedConnectionFactory managedConnectionFactory;
    private ManagedConnection managedConnection;
    private ManagedConnectionProxy proxy;
    private BrokerService broker;
    private HashSet<ManagedConnection> connections;
    private ActiveMQConnectionRequestInfo connectionInfo;

    @Before
    public void setUp() throws Exception {

        createAndStartBroker();

        connectionInfo = new ActiveMQConnectionRequestInfo();
        connectionInfo.setServerUrl(BROKER_URL);
        connectionInfo.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
        connectionInfo.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);

        managedConnectionFactory = new ActiveMQManagedConnectionFactory();
        managedConnection = managedConnectionFactory.createManagedConnection(null, connectionInfo);

        connections = new HashSet<ManagedConnection>();
        connections.add(managedConnection);
    }

    @After
    public void tearDown() throws Exception {
        stopBroker();
    }

    private void createAndStartBroker() throws Exception {
        broker = new BrokerService();
        broker.addConnector(BROKER_TRANSPORT);
        broker.setDataDirectory(KAHADB_DIRECTORY);
        broker.start();
        broker.waitUntilStarted();
    }

    @Test(timeout = 60000)
    public void testFailoverBeforeClose() throws Exception {

        createConnectionAndProxyAndSession();

        stopBroker();

        cleanupConnectionAndProxyAndSession();

        createAndStartBroker();

        for (int i=0; i<2; i++) {
            createConnectionAndProxyAndSession();
            cleanupConnectionAndProxyAndSession();
        }
    }

    private void cleanupConnectionAndProxyAndSession() throws Exception {
        proxy.close();
        managedConnection.cleanup();
    }

    private void createConnectionAndProxyAndSession() throws Exception {
        managedConnection = managedConnectionFactory.matchManagedConnections(connections, null, connectionInfo);
        proxy = (ManagedConnectionProxy) managedConnection.getConnection(null, null);
        proxy.createSession(false, 0);
    }

    private void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }
}
