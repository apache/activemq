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

import jakarta.jms.Connection;
import jakarta.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.util.SocketProxy;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InactivityMonitor detects dead links and should not conflict with soTimeout
 *
 * Before the TcpTransport receiveCounter size check a read on a dead socket throws
 * SocketTimeoutException every soTimeout ms, doRun() swallows it and retries, and every retry
 * bumps receiveCounter — so AbstractInactivityMonitor.readCheck() sees "activity" forever
 * and the dead link isn't handled for the read scenario.
 */
@Category(ParallelTest.class)
public class SoTimeoutInactivityMonitorTest {

    private static final Logger LOG = LoggerFactory.getLogger(SoTimeoutInactivityMonitorTest.class);

    private static final String QUEUE_NAME = "SOTIMEOUT.TEST";
    private static final int MAX_INACTIVITY = 2000;
    private static final long DETECT_WAIT = 30_000;   // >> several soTimeout and inactivity periods

    private BrokerService localBroker;
    private BrokerService remoteBroker;
    private SocketProxy proxy;
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

        var remoteUri = remoteBroker.getTransportConnectors().get(0).getPublishableConnectURI();
        proxy = new SocketProxy(remoteUri);

        localBroker = new BrokerService();
        localBroker.setBrokerName("local");
        localBroker.setPersistent(false);
        localBroker.setUseJmx(false);
        localBroker.start();
        localBroker.waitUntilStarted();

        // a draining remote consumer creates demand so the bridge forwards
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
        if (proxy != null) {
            proxy.close();
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
    public void testInactivityMonitorDetectsDeadLinkWithSoTimeout() throws Exception {
        var detectMs = runDeadLinkScenario("?wireFormat.maxInactivityDuration=" + MAX_INACTIVITY
                + "&soTimeout=1000&soWriteTimeout=1000&keepAlive=true");
        assertTrue("the inactivity monitor must detect a dead link and tear the bridge down even when soTimeout is "
                + "configured on the URI. It did not within " + DETECT_WAIT + "ms: reads timing out every soTimeout ms "
                + "increment receiveCounter on the attempt, so readCheck() sees activity on a dead socket forever "
                + "(took=" + detectMs + "ms, -1 = never)",
                detectMs > 0);
        LOG.info("dead link detected WITH soTimeout in {}ms", detectMs);
    }

    @Test(timeout = 120_000)
    public void testInactivityMonitorDetectsDeadLinkWithoutSoTimeout() throws Exception {
        var detectMs = runDeadLinkScenario("?wireFormat.maxInactivityDuration=" + MAX_INACTIVITY);
        assertTrue("control: without soTimeout the inactivity monitor detects the dead link "
                + "(took=" + detectMs + "ms, -1 = never)", detectMs > 0);
        LOG.info("dead link detected WITHOUT soTimeout in {}ms", detectMs);
    }

    /**
     * @return ms from dead link until the network bridge tore itself down, or -1 if it never did.
     */
    private long runDeadLinkScenario(String uriParams) throws Exception {
        var nc = localBroker.addNetworkConnector("static:(tcp://127.0.0.1:"
                + proxy.getUrl().getPort() + uriParams + ")");
        nc.setName("via-proxy");
        nc.setDuplex(false);
        nc.addStaticallyIncludedDestination(new ActiveMQQueue(QUEUE_NAME));
        nc.start();

        assertTrue("bridge should establish through the proxy",
                Wait.waitFor(() -> !nc.activeBridges().isEmpty(), 20_000, 100));

        // prove the link is alive end-to-end before freezing it
        try (var c = new ActiveMQConnectionFactory(localBroker.getVmConnectorURI()).createConnection()) {
            c.start();
            var session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            var producer = session.createProducer(new ActiveMQQueue(QUEUE_NAME));
            for (int i = 0; i < 5; i++) {
                producer.send(session.createTextMessage("m-" + i));
            }
        }
        assertTrue("bridge should forward while the link is healthy",
                Wait.waitFor(() -> remoteReceived.get() >= 5, 20_000, 10));

        proxy.pause();   // pause the link. sockets stay open, no bytes flow in either direction
        var deadAt = System.currentTimeMillis();
        var tornDown = Wait.waitFor(() -> nc.activeBridges().isEmpty(), DETECT_WAIT, 10);
        var took = System.currentTimeMillis() - deadAt;
        proxy.goOn();
        nc.stop();
        return tornDown ? took : -1;
    }
}
