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
package org.apache.activemq.transport.discovery;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.transport.discovery.multicast.MulticastDiscoveryAgentFactory;
import org.apache.activemq.util.SocketProxy;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.net.URI;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;


public class DiscoveryNetworkReconnectTest {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryNetworkReconnectTest.class);
    final int maxReconnects = 2;
    final String groupName = "GroupID-" + "DiscoveryNetworkReconnectTest";
    final String discoveryAddress = "multicast://default?group=" + groupName + "&initialReconnectDelay=1000";
    final Semaphore mbeanRegistered = new Semaphore(0);
    final Semaphore mbeanUnregistered = new Semaphore(0);
    BrokerService brokerA, brokerB;
    ManagementContext managementContext;
    DiscoveryAgent agent;
    SocketProxy proxy;

    // ArgumentMatcher for network bridge ObjectNames
    class NetworkBridgeObjectNameMatcher implements ArgumentMatcher<ObjectName> {
        ObjectName name;
        NetworkBridgeObjectNameMatcher(ObjectName o) {
            name = o;
        }

        @Override
        public boolean matches(ObjectName other) {
            if (other == null) return false;
            LOG.info("Match: " + name + " vs: " + other);

            if (!"networkConnectors".equals(other.getKeyProperty("connector"))) {
                return false;
            }
            return other.getKeyProperty("connector").equals(name.getKeyProperty("connector")) &&
                   other.getKeyProperty("networkBridge") != null && name.getKeyProperty("networkBridge") != null;
        }
    }

    @Before
    public void setUp() throws Exception {
        brokerA = new BrokerService();
        brokerA.setBrokerName("BrokerA");
        configure(brokerA);
        brokerA.addConnector("tcp://localhost:0");
        brokerA.start();
        brokerA.waitUntilStarted();

        proxy = new SocketProxy(brokerA.getTransportConnectors().get(0).getConnectUri());
        managementContext = mock(ManagementContext.class);

        // Default lenient stubs for ManagementContext
        lenient().when(managementContext.getJmxDomainName()).thenReturn("Test");
        lenient().doNothing().when(managementContext).setBrokerName("BrokerNC");
        lenient().doNothing().when(managementContext).start();
        lenient().when(managementContext.isCreateConnector()).thenReturn(false);
        lenient().doNothing().when(managementContext).stop();
        lenient().when(managementContext.isConnectorStarted()).thenReturn(false);

        // Expected MBean registrations
        lenient().when(managementContext.registerMBean(any(),
                eq(new ObjectName("Test:type=Broker,brokerName=BrokerNC")))).thenReturn(null);
        lenient().when(managementContext.registerMBean(any(),
                eq(new ObjectName("Test:type=Broker,brokerName=BrokerNC,service=Health")))).thenReturn(null);
        lenient().when(managementContext.registerMBean(any(),
                eq(new ObjectName("Test:type=Broker,brokerName=BrokerNC,connector=networkConnectors,networkConnectorName=NC")))).thenReturn(null);
        lenient().when(managementContext.registerMBean(any(),
                eq(new ObjectName("Test:type=Broker,brokerName=BrokerNC,service=Log4JConfiguration")))).thenReturn(null);
        lenient().when(managementContext.registerMBean(any(),
                eq(new ObjectName("Test:type=Broker,brokerName=BrokerNC,destinationType=Topic,destinationName=ActiveMQ.Advisory.Connection")))).thenReturn(null);
        lenient().when(managementContext.registerMBean(any(),
                eq(new ObjectName("Test:type=Broker,brokerName=BrokerNC,destinationType=Topic,destinationName=ActiveMQ.Advisory.NetworkBridge")))).thenReturn(null);
        lenient().when(managementContext.registerMBean(any(),
                eq(new ObjectName("Test:type=Broker,brokerName=BrokerNC,destinationType=Topic,destinationName=ActiveMQ.Advisory.MasterBroker")))).thenReturn(null);
        lenient().when(managementContext.registerMBean(any(),
                eq(new ObjectName("Test:type=Broker,brokerName=BrokerNC,service=jobScheduler,jobSchedulerName=JMS")))).thenReturn(null);
        lenient().when(managementContext.getObjectInstance(
                eq(new ObjectName("Test:type=Broker,brokerName=BrokerNC,connector=networkConnectors,networkConnectorName=NC")))).thenReturn(null);

        // Network bridge MBean register - signal semaphore
        lenient().when(managementContext.registerMBean(any(), argThat(new NetworkBridgeObjectNameMatcher(
                new ObjectName("Test:type=Broker,brokerName=BrokerNC,connector=networkConnectors,networkConnectorName=NC,networkBridge=localhost/127.0.0.1_"
                    + proxy.getUrl().getPort()))))).thenAnswer(invocation -> {
                        LOG.info("Mbean Registered: " + invocation.getArgument(0));
                        mbeanRegistered.release();
                        return new ObjectInstance((ObjectName)invocation.getArgument(1), "discription");
                    });

        // Network bridge MBean unregister - signal semaphore
        lenient().doAnswer(invocation -> {
            LOG.info("Mbean Unregistered: " + invocation.getArgument(0));
            mbeanUnregistered.release();
            return null;
        }).when(managementContext).unregisterMBean(argThat(new NetworkBridgeObjectNameMatcher(
                new ObjectName("Test:type=Broker,brokerName=BrokerNC,connector=networkConnectors,networkConnectorName=NC,networkBridge=localhost/127.0.0.1_"
                        + proxy.getUrl().getPort()))));

        brokerB = new BrokerService();
        brokerB.setManagementContext(managementContext);
        brokerB.setBrokerName("BrokerNC");
        configure(brokerB);
    }

    @After
    public void tearDown() throws Exception {
        brokerA.stop();
        brokerA.waitUntilStopped();
        brokerB.stop();
        brokerB.waitUntilStopped();
        proxy.close();
    }

    private void configure(BrokerService broker) {
        broker.setPersistent(false);
        broker.setUseJmx(true);
    }

    @Test
    public void testMulicastReconnect() throws Exception {

        brokerB.addNetworkConnector(discoveryAddress + "&discovered.trace=true&discovered.wireFormat.maxInactivityDuration=1000&discovered.wireFormat.maxInactivityDurationInitalDelay=1000");
        brokerB.start();
        brokerB.waitUntilStarted();

        // control multicast advertise agent to inject proxy
        agent = MulticastDiscoveryAgentFactory.createDiscoveryAgent(new URI(discoveryAddress));
        agent.registerService(proxy.getUrl().toString());
        agent.start();

        doReconnect();
    }

    @Test
    public void testSimpleReconnect() throws Exception {
        brokerB.addNetworkConnector("simple://(" + proxy.getUrl()
                + ")?useExponentialBackOff=false&initialReconnectDelay=500&discovered.wireFormat.maxInactivityDuration=1000&discovered.wireFormat.maxInactivityDurationInitalDelay=1000");
        brokerB.start();
        brokerB.waitUntilStarted();
        doReconnect();
    }

    private void doReconnect() throws Exception {

        for (int i=0; i<maxReconnects; i++) {
            // Wait for connection
            assertTrue("we got a network connection in a timely manner", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                   return proxy.connections.size() >= 1;
                }
            }));

            // wait for network connector
            assertTrue("network connector mbean registered within 1 minute", mbeanRegistered.tryAcquire(60, TimeUnit.SECONDS));

            // force an inactivity timeout via the proxy
            proxy.pause();

            // wait for the inactivity timeout and network shutdown
            assertTrue("network connector mbean unregistered within 1 minute", mbeanUnregistered.tryAcquire(60, TimeUnit.SECONDS));

            // whack all connections
            proxy.close();

            // let a reconnect succeed
            proxy.reopen();
        }
    }
}
