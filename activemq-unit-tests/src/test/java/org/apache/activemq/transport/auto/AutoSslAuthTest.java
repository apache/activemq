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
package org.apache.activemq.transport.auto;

import static org.junit.Assert.assertTrue;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ConnectionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AutoSslAuthTest {

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    private String uri;
    private final String protocol;
    private AtomicInteger hasCertificateCount = new AtomicInteger();
    private BrokerService brokerService;

    @Parameters(name="protocol={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"auto+nio+ssl"},
                {"auto+ssl"}
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

        TransportConnector connector = brokerService.addConnector(protocol + "://localhost:0?transport.needClientAuth=true");
        connector.setName("auto");
        uri = connector.getPublishableConnectString();

        ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();

        plugins.add(new BrokerPlugin() {

            @Override
            public Broker installPlugin(Broker broker) throws Exception {
                return new BrokerFilter(broker) {

                    @Override
                    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
                        super.addConnection(context, info);
                        if (info.getTransportContext() instanceof X509Certificate[]) {
                            hasCertificateCount.getAndIncrement();
                        }
                    }
                };
            }
        });

        if (!plugins.isEmpty()) {
            BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
            brokerService.setPlugins(plugins.toArray(array));
        }

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
    public AutoSslAuthTest(String protocol) {
        this.protocol = protocol;
    }

    @Test(timeout = 60000)
    public void testConnect() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL(uri);

        //Create 5 connections to make sure all are properly set
        for (int i = 0; i < 5; i++) {
            factory.createConnection().start();
        }

        assertTrue(hasCertificateCount.get() == 5);

        for (TransportConnection connection : brokerService.getTransportConnectorByName("auto").getConnections()) {
            assertTrue(connection.getTransport().getPeerCertificates() != null);
        }
    }
}
