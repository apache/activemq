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
package org.apache.activemq.usecases;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.spring.SpringSslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class NetworkAsyncStartSslTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkAsyncStartSslTest.class);

    private String brokerBDomain = "localhost:61617";
    private String brokerCDomain = "localhost:61618";
    int bridgeCount=0;

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    public void testSslPerConnectorConfig() throws Exception {
        String transport = "ssl";
        String brokerBUri = transport + "://" + brokerBDomain;
        String brokerCUri = transport + "://" + brokerCDomain;

        SpringSslContext brokerSslContext = new SpringSslContext();
        brokerSslContext.setKeyStore(SERVER_KEYSTORE);
        brokerSslContext.setKeyStorePassword(PASSWORD);
        brokerSslContext.setKeyStoreType(KEYSTORE_TYPE);
        brokerSslContext.setTrustStore(TRUST_KEYSTORE);
        brokerSslContext.setTrustStorePassword(PASSWORD);
        brokerSslContext.afterPropertiesSet();

        BrokerService brokerC = brokers.get("BrokerC").broker;
        brokerC.setSslContext(brokerSslContext);
        brokerC.addConnector(brokerCUri);
        brokerC.start();

        BrokerService brokerB = brokers.get("BrokerB").broker;
        brokerB.setSslContext(brokerSslContext);
        brokerB.addConnector(brokerBUri);
        brokerB.start();

        BrokerService brokerA = brokers.get("BrokerA").broker;
        brokerA.setNetworkConnectorStartAsync(true);
        NetworkConnector networkConnector = bridgeBroker(brokerA, brokerBUri);
        networkConnector.setSslContext(brokerSslContext);
        LOG.info("Added bridge to: " + brokerBUri);

        // no ssl context, will fail
        bridgeBroker(brokerA, brokerCUri);
        LOG.info("Added bridge to: " + brokerCUri);

        LOG.info("starting A..");
        brokerA.start();

        // wait for A to get bridge to B
        waitForBridgeFormation(brokerA, 1, 0);

        assertTrue("one worked", hasBridge("BrokerA", "BrokerB"));
        assertFalse("one failed", hasBridge("BrokerA", "BrokerC"));
    }

    private NetworkConnector bridgeBroker(BrokerService localBroker, String remoteURI) throws Exception {
        String uri = "static:(" + remoteURI + ")";
        NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
        connector.setName("bridge-" + bridgeCount++);
        localBroker.addNetworkConnector(connector);
        return connector;
    }

    @Override
    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        // initially with no tcp transport connector
        createBroker(new URI("broker:()BrokerA?persistent=false&useJmx=false"));
        createBroker(new URI("broker:()BrokerB?persistent=false&useJmx=false"));
        createBroker(new URI("broker:()BrokerC?persistent=false&useJmx=false"));
    }
}
