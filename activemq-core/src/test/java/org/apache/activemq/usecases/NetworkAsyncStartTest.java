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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.SocketProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkAsyncStartTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkAsyncStartTest.class);

    private String brokerBUri = "tcp://localhost:61617";
    private String brokerCUri = "tcp://localhost:61618";
    int bridgeCount=0;

    public void testAsyncNetworkStartup() throws Exception {

        BrokerService brokerA = brokers.get("BrokerA").broker;
        bridgeBroker(brokerA, brokerBUri);
        bridgeBroker(brokerA, brokerCUri);

        LOG.info("starting A, no blocking on failed network connectors");
        brokerA.start();

        LOG.info("starting C transport connector");
        BrokerService brokerC = brokers.get("BrokerC").broker;
        brokerC.addConnector(brokerCUri);
        brokerC.start();

        assertTrue("got bridge to C", waitForBridgeFormation(brokerA, 1, 1));
        LOG.info("Got bridge A->C");

        LOG.info("starting B transport connector");
        BrokerService brokerB = brokers.get("BrokerB").broker;
        brokerB.addConnector(brokerBUri);
        brokerB.start();

        assertTrue("got bridge to B", waitForBridgeFormation(brokerA, 1, 0));
        assertTrue("got bridge to B&C", waitForBridgeFormation(brokerA, 1, 1));
    }

    public void testAsyncNetworkStartupWithSlowConnectionCreation() throws Exception {

        final BrokerService brokerA = brokers.get("BrokerA").broker;

        SocketProxy proxyToB = new SocketProxy();
        // don't accept any connections so limited to one connection with backlog
        proxyToB.setPauseAtStart(true);
        proxyToB.setAcceptBacklog(1);
        proxyToB.setTarget(new URI(brokerBUri));
        proxyToB.open();
        bridgeBroker(brokerA, proxyToB.getUrl().toString());
        bridgeBroker(brokerA, proxyToB.getUrl().toString());
        bridgeBroker(brokerA, proxyToB.getUrl().toString());
        bridgeBroker(brokerA, proxyToB.getUrl().toString());
        bridgeBroker(brokerA, proxyToB.getUrl().toString());
        bridgeBroker(brokerA, proxyToB.getUrl().toString());
        bridgeBroker(brokerA, proxyToB.getUrl().toString());
        bridgeBroker(brokerA, brokerCUri);

        Executor e = Executors.newCachedThreadPool();
        e.execute(new Runnable() {
            public void run() {
                LOG.info("starting A");
                try {
                    brokerA.setNetworkConnectorStartAsync(true);
                    brokerA.start();
                } catch (Exception e) {
                    LOG.error("start failed", e);
                }
            }
        });

        LOG.info("starting transport connector on C");
        BrokerService brokerC = brokers.get("BrokerC").broker;
        brokerC.addConnector(brokerCUri);
        brokerC.start();

        final long maxWaitMillis = 20*1000;
        assertTrue("got bridge to C in 10 seconds", waitForBridgeFormation(brokerA, 1, 7, maxWaitMillis));
    }

    private void bridgeBroker(BrokerService localBroker, String remoteURI) throws Exception {
        String uri = "static:(" + remoteURI + ")";
        NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
        connector.setName("bridge-" + bridgeCount++);
        localBroker.addNetworkConnector(connector);
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
