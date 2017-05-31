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
package org.apache.activemq.transport.tcp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test for https://issues.apache.org/jira/browse/AMQ-6561 to make sure sockets
 * are closed on all connection attempt errors
 */
@RunWith(Parameterized.class)
public class TcpTransportCloseSocketTest {

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    private String uri;
    private final String protocol;
    private BrokerService brokerService;

    @Parameters(name="protocol={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                  {"auto+nio+ssl"},
                  {"auto+ssl"},
                  {"ssl"},
                  {"tcp"}
            });
    }

    static {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
    }

    @Before
    public void before() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);

        TransportConnector connector = brokerService.addConnector(protocol + "://localhost:0");
        connector.setName("tcp");
        uri = connector.getPublishableConnectString();
        this.brokerService = brokerService;
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

    /**
     * @param isNio
     */
    public TcpTransportCloseSocketTest(String protocol) {
        this.protocol = protocol;
    }

    //We want to make sure that the socket will be closed if there as an error on broker.addConnection
    //even if the client doesn't close the connection to prevent dangling open sockets
    @Test(timeout = 60000)
    public void testDuplicateClientIdCloseConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL(uri);
        factory.setClientID("id");

        final TcpTransportServer server = (TcpTransportServer) brokerService.getTransportConnectorByName("tcp").getServer();

        //Try and create 2 connections, the second should fail because of a duplicate clientId
        int failed = 0;
        for (int i = 0; i < 2; i++) {
            try {
                factory.createConnection().start();
            } catch (Exception e) {
                e.printStackTrace();
                failed++;
            }
        }

        assertEquals(1, failed);
        //after 2 seconds the connection should be terminated by the broker because of the exception
        //on broker.addConnection
        assertTrue(Wait.waitFor(new Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return server.getCurrentTransportCount().get() == 1;
            }

        }, 10000, 500));
    }
}
