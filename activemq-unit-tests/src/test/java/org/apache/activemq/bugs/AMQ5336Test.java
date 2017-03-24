/*
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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for priority backup URI handling.
 */
public class AMQ5336Test {

    private BrokerService brokerService;
    private String connectionUri;

    @Before
    public void before() throws Exception {
        brokerService = new BrokerService();
        TransportConnector connector = brokerService.addConnector("tcp://localhost:0");
        connectionUri = connector.getPublishableConnectString();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void after() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Test
    public void test() throws Exception {
        String uri = "failover:(" + connectionUri + ")" +
                                "?randomize=false&" +
                                "nested.socket.tcpNoDelay=true&" +
                                "priorityBackup=true&" +
                                "priorityURIs=" + connectionUri + "&" +
                                "initialReconnectDelay=1000&" +
                                "useExponentialBackOff=false";

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(uri);
        ActiveMQConnection connection = (ActiveMQConnection) cf.createConnection();
        connection.start();

        FailoverTransport failover = connection.getTransport().narrow(FailoverTransport.class);
        assertNotNull(failover);
        assertTrue(failover.isConnectedToPriority());

        connection.close();
    }
}
