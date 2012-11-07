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

package org.apache.activemq.bugs;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.transport.discovery.simple.SimpleDiscoveryAgent;
import org.junit.Assert;

/**
 * This unit test demonstrates a bug in {@link SimpleDiscoveryAgent} that
 * results from a lack of thread safety when handling bridge failures. This bug
 * allows a single discovery event (which should result in a single bridge
 * connection attempt) to multiply into several concurrent bridge connection
 * attempts, which can lead to additional bugs (see related) due to a lack of
 * thread safety in {@link DiscoveryNetworkConnector}.
 */
public class AMQ4159Test extends JmsMultipleBrokersTestSupport {
    /**
     * This test is expected to pass since the reconnect delay preserves the
     * discovery event's failed flag long enough for the multiple bridge failure
     * events to be ignored.
     */
    public void testWithReconnectDelay() throws Exception {
        doTest(1000, 5000);
    }

    /**
     * This test is expected to fail since the lack of reconnect delay allows
     * the discovery event's failed flag to be reset while there are still
     * pending failure events, thus allowing (unexpectedly) concurrent bridge
     * reconnect attempts.
     */
    public void testWithoutReconnectDelay() throws Exception {
        doTest(0, 0);
    }

    private void doTest(long reconnectDelay, long minConnectTime)
            throws Exception {
        // Start two brokers with a bridge from broker1 to broker2.
        BrokerService broker1 = createBroker(new URI(
                "broker:(vm://broker1)/broker1?persistent=false"));
        BrokerService broker2 = createBroker(new URI(
                "broker:(vm://broker2)/broker2?persistent=false"));

        // Prevent broker2 from removing its (inbound) bridge connection.
        BrokerPlugin ignoreRemoveConnectionPlugin = new BrokerPlugin() {
            @Override
            public Broker installPlugin(Broker broker) throws Exception {
                return new BrokerFilter(broker) {
                    @Override
                    public void removeConnection(ConnectionContext context,
                            ConnectionInfo info, Throwable error)
                            throws Exception {
                        // Ignore, leaving behind clientId and connection.
                    }
                };
            }
        };

        broker2.setPlugins(new BrokerPlugin[] { ignoreRemoveConnectionPlugin });

        startAllBrokers();

        // Start a bridge from broker1 to broker2 that tracks information about
        // the connection attemps.
        final AtomicInteger numAttempts = new AtomicInteger(0);
        final AtomicInteger concurrency = new AtomicInteger(0);
        final AtomicInteger concurrencyAttempt = new AtomicInteger(-1);
        final CountDownLatch attemptLatch = new CountDownLatch(10);

        DiscoveryNetworkConnector nc = new DiscoveryNetworkConnector() {
            @Override
            public void onServiceAdd(DiscoveryEvent event) {
                int attempt = numAttempts.incrementAndGet();
                if (concurrency.incrementAndGet() > 1) {
                    concurrencyAttempt.compareAndSet(-1, attempt);
                }
                try {
                    super.onServiceAdd(event);
                } finally {
                    concurrency.decrementAndGet();
                }
                attemptLatch.countDown();
            }
        };

        SimpleDiscoveryAgent da = new SimpleDiscoveryAgent();
        da.setInitialReconnectDelay(reconnectDelay);
        da.setMinConnectTime(minConnectTime);
        da.setUseExponentialBackOff(false);
        da.setServices(new URI[] { broker2.getVmConnectorURI() });

        nc.setDiscoveryAgent(da);
        broker1.addNetworkConnector(nc);
        nc.start();

        waitForBridgeFormation();

        // Verify that only one attempt and thread was used to establish the
        // bridge.
        Assert.assertEquals(1, numAttempts.get());
        Assert.assertEquals(-1, concurrencyAttempt.get());

        // Stop the bridge; this will leave the connections in the broker and
        // prevent a new bridge from being established.
        nc.stop();

        // Attempt to re-establish the bridge; this will cause repeated
        // "connection already exists" exceptions.
        numAttempts.set(0);
        nc.start();

        // Wait for several attempts and verify that only a single thread was
        // used to (attempt) to establish the bridge.
        attemptLatch.await(4, TimeUnit.MINUTES);
        Assert.assertEquals(-1, concurrencyAttempt.get());
    }
}
