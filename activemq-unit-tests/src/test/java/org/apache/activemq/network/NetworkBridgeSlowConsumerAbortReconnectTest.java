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
package org.apache.activemq.network;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.AbortSlowAckConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbortSlowAckConsumerStrategy with ignoreNetworkConsumers=false and abortConnection=true aborts
 * a network bridge's subscription. The abort closes the bridge's local connection server-side, so
 * the bridge should fail, reconnect, and resume forwarding.
 *
 * The strategy marks the bridge's subscription slow from timeSinceLastAck, which starts at
 * subscription creation, so an idle bridge is aborted after maxTimeSinceLastAck without simulating
 * a link fault.
 */
public class NetworkBridgeSlowConsumerAbortReconnectTest {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkBridgeSlowConsumerAbortReconnectTest.class);

    private static final String QUEUE_NAME = "BRIDGE.ABORT.RECONNECT.TEST";
    private static final long MAX_TIME_SINCE_LAST_ACK = 2000;
    private static final long MAX_SLOW_DURATION = 2000;

    private BrokerService localBroker;
    private BrokerService remoteBroker;
    private Connection remoteConnection;
    private final AtomicInteger remoteReceived = new AtomicInteger();

    @Before
    public void setUp() throws Exception {
        remoteBroker = new BrokerService();
        remoteBroker.setBrokerName("remote");
        remoteBroker.setPersistent(false);
        remoteBroker.setUseJmx(false);
        remoteBroker.addConnector("tcp://127.0.0.1:0");
        remoteBroker.start();
        remoteBroker.waitUntilStarted();

        var strategy = new AbortSlowAckConsumerStrategy();
        strategy.setIgnoreNetworkConsumers(false);   // default true never touches a bridge
        strategy.setIgnoreIdleConsumers(false);      // reach the bridge even when it is idle
        strategy.setMaxTimeSinceLastAck(MAX_TIME_SINCE_LAST_ACK);
        strategy.setMaxSlowDuration(MAX_SLOW_DURATION);
        strategy.setCheckPeriod(500);
        strategy.setAbortConnection(true);           // server-side stop of the bridge's connection

        var entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setSlowConsumerStrategy(strategy);
        var policyMap = new PolicyMap();
        policyMap.setDefaultEntry(entry);

        localBroker = new BrokerService();
        localBroker.setBrokerName("local");
        localBroker.setPersistent(false);
        localBroker.setUseJmx(false);
        localBroker.setDestinationPolicy(policyMap);
        localBroker.start();
        localBroker.waitUntilStarted();

        var remoteUri = remoteBroker.getTransportConnectors().get(0).getPublishableConnectURI();
        remoteConnection = new ActiveMQConnectionFactory(remoteUri).createConnection();
        remoteConnection.start();
        remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)
                .createConsumer(new ActiveMQQueue(QUEUE_NAME))
                .setMessageListener(m -> remoteReceived.incrementAndGet());
    }

    @After
    public void tearDown() throws Exception {
        if (remoteConnection != null) {
            try { remoteConnection.close(); } catch (Exception ignored) {}
        }
        if (localBroker != null) {
            localBroker.stop();
            localBroker.waitUntilStopped();
        }
        if (remoteBroker != null) {
            remoteBroker.stop();
            remoteBroker.waitUntilStopped();
        }
    }

    @Test(timeout = 120_000)
    public void testBridgeReconnectsAfterSlowConsumerStrategyAbort() throws Exception {
        var remoteUri = remoteBroker.getTransportConnectors().get(0).getPublishableConnectURI();
        var nc = localBroker.addNetworkConnector("static:(" + remoteUri + ")");
        nc.setName("to-remote");
        nc.setDuplex(false);
        nc.addStaticallyIncludedDestination(new ActiveMQQueue(QUEUE_NAME));
        nc.start();
        assertTrue("bridge should establish",
                Wait.waitFor(() -> !nc.activeBridges().isEmpty(), 20_000, 10));
        var firstBridge = nc.activeBridges().iterator().next();

        // the strategy marks the idle bridge subscription slow after maxTimeSinceLastAck and
        // aborts its connection after maxSlowDuration; the bridge should then come back
        assertTrue("a new bridge instance must replace the aborted one (abort expected ~"
                + (MAX_TIME_SINCE_LAST_ACK + MAX_SLOW_DURATION) + "ms after establish)",
                Wait.waitFor(() -> {
                    for (var bridge : nc.activeBridges()) {
                        if (bridge != firstBridge) {
                            return true;
                        }
                    }
                    return false;
                }, 60_000, 10));
        assertTrue("the aborted bridge instance must be deregistered from activeBridges",
                Wait.waitFor(() -> !nc.activeBridges().contains(firstBridge), 20_000, 10));
        LOG.info("bridge aborted by AbortSlowAckConsumerStrategy and reconnected");

        // an idle bridge keeps being recycled, but once messages flow and are acked the active
        // bridge is no longer slow, so forwarding converges
        try (var c = new ActiveMQConnectionFactory(localBroker.getVmConnectorURI()).createConnection();
             var session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
             var producer = session.createProducer(new ActiveMQQueue(QUEUE_NAME));) {
            c.start();


            for (var i = 0; i < 5; i++) {
                producer.send(session.createTextMessage("m-" + i));
            }
        }
        assertTrue("messages must flow across a live bridge after the strategy aborts",
                Wait.waitFor(() -> remoteReceived.get() >= 5, 60_000, 10));
        LOG.info("forwarding resumed after the strategy abort");
        nc.stop();
    }
}
