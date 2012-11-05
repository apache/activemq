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

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.DiscoveryEvent;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkBridgeListener;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.transport.discovery.DiscoveryListener;
import org.apache.activemq.transport.discovery.simple.SimpleDiscoveryAgent;
import org.junit.Assert;

/**
 * This test demonstrates a number of race conditions in
 * {@link DiscoveryNetworkConnector} that can result in an active bridge no
 * longer being reported as active and vice-versa, an inactive bridge still
 * being reported as active.
 */
public class AMQ4160Test extends JmsMultipleBrokersTestSupport {
    /**
     * This test demonstrates how concurrent attempts to establish a bridge to
     * the same remote broker are allowed to occur. Connection uniqueness will
     * cause whichever bridge creation attempt is second to fail. However, this
     * failure erases the entry in
     * {@link DiscoveryNetworkConnector#activeBridges()} that represents the
     * successful first bridge creation attempt.
     */
    public void testLostActiveBridge() throws Exception {
        // Start two brokers with a bridge from broker1 to broker2.
        BrokerService broker1 = createBroker(new URI(
                "broker:(vm://broker1)/broker1?persistent=false"));
        final BrokerService broker2 = createBroker(new URI(
                "broker:(vm://broker2)/broker2?persistent=false"));

        // Allow the concurrent local bridge connections to be made even though
        // they are duplicated; this prevents both of the bridge attempts from
        // failing in the case that the local and remote bridges are established
        // out-of-order.
        BrokerPlugin ignoreAddConnectionPlugin = new BrokerPlugin() {
            @Override
            public Broker installPlugin(Broker broker) throws Exception {
                return new BrokerFilter(broker) {
                    @Override
                    public void addConnection(ConnectionContext context,
                            ConnectionInfo info) throws Exception {
                        // ignore
                    }
                };
            }
        };

        broker1.setPlugins(new BrokerPlugin[] { ignoreAddConnectionPlugin });

        startAllBrokers();

        // Start a bridge from broker1 to broker2. The discovery agent attempts
        // to create the bridge concurrently with two threads, and the
        // synchronization in createBridge ensures that both threads actually
        // attempt to start bridges.
        final CountDownLatch createLatch = new CountDownLatch(2);

        DiscoveryNetworkConnector nc = new DiscoveryNetworkConnector() {
            @Override
            protected NetworkBridge createBridge(Transport localTransport,
                    Transport remoteTransport, final DiscoveryEvent event) {
                createLatch.countDown();
                try {
                    createLatch.await();
                } catch (InterruptedException e) {
                }
                return super.createBridge(localTransport, remoteTransport,
                        event);
            }
        };

        nc.setDiscoveryAgent(new DiscoveryAgent() {
            TaskRunnerFactory taskRunner = new TaskRunnerFactory();
            DiscoveryListener listener;

            @Override
            public void start() throws Exception {
                taskRunner.init();
                taskRunner.execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onServiceAdd(new DiscoveryEvent(broker2
                                .getVmConnectorURI().toString()));
                    }
                });
                taskRunner.execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onServiceAdd(new DiscoveryEvent(broker2
                                .getVmConnectorURI().toString()));
                    }
                });
            }

            @Override
            public void stop() throws Exception {
                taskRunner.shutdown();
            }

            @Override
            public void setDiscoveryListener(DiscoveryListener listener) {
                this.listener = listener;
            }

            @Override
            public void registerService(String name) throws IOException {
            }

            @Override
            public void serviceFailed(DiscoveryEvent event) throws IOException {
                listener.onServiceRemove(event);
            }
        });

        broker1.addNetworkConnector(nc);
        nc.start();

        // The bridge should be formed by the second creation attempt, but the
        // wait will time out because the active bridge entry from the second
        // (successful) bridge creation attempt is removed by the first
        // (unsuccessful) bridge creation attempt.
        waitForBridgeFormation();

        Assert.assertFalse(nc.activeBridges().isEmpty());
    }

    /**
     * This test demonstrates a race condition where a failed bridge can be
     * removed from the list of active bridges in
     * {@link DiscoveryNetworkConnector} before it has been added. Eventually,
     * the failed bridge is added, but never removed, which prevents subsequent
     * bridge creation attempts to be ignored. The result is a network connector
     * that thinks it has an active bridge, when in fact it doesn't.
     */
    public void testInactiveBridgStillActive() throws Exception {
        // Start two brokers with a bridge from broker1 to broker2.
        BrokerService broker1 = createBroker(new URI(
                "broker:(vm://broker1)/broker1?persistent=false"));
        final BrokerService broker2 = createBroker(new URI(
                "broker:(vm://broker2)/broker2?persistent=false"));

        // Force bridge failure by having broker1 disallow connections.
        BrokerPlugin disallowAddConnectionPlugin = new BrokerPlugin() {
            @Override
            public Broker installPlugin(Broker broker) throws Exception {
                return new BrokerFilter(broker) {
                    @Override
                    public void addConnection(ConnectionContext context,
                            ConnectionInfo info) throws Exception {
                        throw new Exception(
                                "Test exception to force bridge failure");
                    }
                };
            }
        };

        broker1.setPlugins(new BrokerPlugin[] { disallowAddConnectionPlugin });

        startAllBrokers();

        // Start a bridge from broker1 to broker2. The bridge delays returning
        // from start until after the bridge failure has been processed;
        // this leaves the first bridge creation attempt recorded as active,
        // even though it failed.
        final SimpleDiscoveryAgent da = new SimpleDiscoveryAgent();
        da.setServices(new URI[] { broker2.getVmConnectorURI() });

        final CountDownLatch attemptLatch = new CountDownLatch(3);
        final CountDownLatch removedLatch = new CountDownLatch(1);

        DiscoveryNetworkConnector nc = new DiscoveryNetworkConnector() {
            @Override
            public void onServiceAdd(DiscoveryEvent event) {
                attemptLatch.countDown();
                super.onServiceAdd(event);
            }

            @Override
            public void onServiceRemove(DiscoveryEvent event) {
                super.onServiceRemove(event);
                removedLatch.countDown();
            }

            @Override
            protected NetworkBridge createBridge(Transport localTransport,
                    Transport remoteTransport, final DiscoveryEvent event) {
                final NetworkBridge next = super.createBridge(localTransport,
                        remoteTransport, event);
                return new NetworkBridge() {

                    @Override
                    public void start() throws Exception {
                        next.start();
                        // Delay returning until the failed service has been
                        // removed.
                        removedLatch.await();
                    }

                    @Override
                    public void stop() throws Exception {
                        next.stop();
                    }

                    @Override
                    public void serviceRemoteException(Throwable error) {
                        next.serviceRemoteException(error);
                    }

                    @Override
                    public void serviceLocalException(Throwable error) {
                        next.serviceLocalException(error);
                    }

                    @Override
                    public void setNetworkBridgeListener(
                            NetworkBridgeListener listener) {
                        next.setNetworkBridgeListener(listener);
                    }

                    @Override
                    public String getRemoteAddress() {
                        return next.getRemoteAddress();
                    }

                    @Override
                    public String getRemoteBrokerName() {
                        return next.getRemoteBrokerName();
                    }

                    @Override
                    public String getLocalAddress() {
                        return next.getLocalAddress();
                    }

                    @Override
                    public String getLocalBrokerName() {
                        return next.getLocalBrokerName();
                    }

                    @Override
                    public long getEnqueueCounter() {
                        return next.getEnqueueCounter();
                    }

                    @Override
                    public long getDequeueCounter() {
                        return next.getDequeueCounter();
                    }

                    @Override
                    public void setMbeanObjectName(ObjectName objectName) {
                        next.setMbeanObjectName(objectName);
                    }

                    @Override
                    public ObjectName getMbeanObjectName() {
                        return next.getMbeanObjectName();
                    }
                };
            }
        };
        nc.setDiscoveryAgent(da);

        broker1.addNetworkConnector(nc);
        nc.start();

        // All bridge attempts should fail, so the attempt latch should get
        // triggered. However, because of the race condition, the first attempt
        // is considered successful and causes further attempts to stop.
        // Therefore, this wait will time out and cause the test to fail.
        Assert.assertTrue(attemptLatch.await(30, TimeUnit.SECONDS));
    }
}
