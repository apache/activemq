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

import java.net.URI;
import java.util.Arrays;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkBrokerNameColonTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkBrokerNameColonTest.class);

    public void testNetworkStartupColon() throws Exception {

        BrokerService brokerColon = new BrokerService();
        brokerColon.setBrokerName("BrokerA:Colon");
        brokerColon.setUseJmx(true);

        BrokerService brokerColonB = createBroker(new URI("broker:()BrokerB?persistent=false&useJmx=false"));
        brokerColonB.addConnector("tcp://localhost:0");
        brokerColonB.start();

        String uri = "static:(" + brokerColonB.getTransportConnectors().get(0).getPublishableConnectString() + ")";
        NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
        connector.setName("bridge-to-b");
        brokerColon.setNetworkConnectors(Arrays.asList(new NetworkConnector[]{connector}));

        LOG.info("starting broker with Colon in name");
        brokerColon.start();

        assertTrue("got bridge to B", waitForBridgeFormation(brokerColon, 1, 0));
    }
}
