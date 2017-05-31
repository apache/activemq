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

import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectInstance;
import javax.management.ObjectName;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.transport.discovery.multicast.MulticastDiscoveryAgentFactory;
import org.apache.activemq.util.SocketProxy;
import org.apache.activemq.util.Wait;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Invocation;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.action.CustomAction;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RunWith(JMock.class)
public class DiscoveryNetworkReconnectTest {

    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryNetworkReconnectTest.class);
    final int maxReconnects = 2;
    final String groupName = "GroupID-" + "DiscoveryNetworkReconnectTest";
    final String discoveryAddress = "multicast://default?group=" + groupName + "&initialReconnectDelay=1000";
    final Semaphore mbeanRegistered = new Semaphore(0);
    final Semaphore mbeanUnregistered = new Semaphore(0);
    BrokerService brokerA, brokerB;
    Mockery context;
    ManagementContext managementContext;
    DiscoveryAgent agent;
    SocketProxy proxy;

    // ignore the hostname resolution component as this is machine dependent
    class NetworkBridgeObjectNameMatcher<T> extends BaseMatcher<T> {
        T name;
        NetworkBridgeObjectNameMatcher(T o) {
            name = o;
        }

        @Override
        public boolean matches(Object arg0) {
            ObjectName other = (ObjectName) arg0;
            ObjectName mine = (ObjectName) name;
            LOG.info("Match: " + mine + " vs: " + other);

            if (!"networkConnectors".equals(other.getKeyProperty("connector"))) {
                return false;
            }
            return other.getKeyProperty("connector").equals(mine.getKeyProperty("connector")) &&
                   other.getKeyProperty("networkBridge") != null && mine.getKeyProperty("networkBridge") != null;
        }

        @Override
        public void describeTo(Description arg0) {
            arg0.appendText(this.getClass().getName());
        }
    }

    @Before
    public void setUp() throws Exception {
        context = new JUnit4Mockery() {{
            setImposteriser(ClassImposteriser.INSTANCE);
        }};
        brokerA = new BrokerService();
        brokerA.setBrokerName("BrokerA");
        configure(brokerA);
        brokerA.addConnector("tcp://localhost:0");
        brokerA.start();
        brokerA.waitUntilStarted();

        proxy = new SocketProxy(brokerA.getTransportConnectors().get(0).getConnectUri());
        managementContext = context.mock(ManagementContext.class);

        context.checking(new Expectations(){{
            allowing(managementContext).getJmxDomainName(); will (returnValue("Test"));
            allowing(managementContext).setBrokerName("BrokerNC");
            allowing(managementContext).start();
            allowing(managementContext).isCreateConnector();
            allowing(managementContext).stop();
            allowing(managementContext).isConnectorStarted();

            // expected MBeans
            allowing(managementContext).registerMBean(with(any(Object.class)), with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC"))));
            allowing(managementContext).registerMBean(with(any(Object.class)), with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,service=Health"))));
            allowing(managementContext).registerMBean(with(any(Object.class)), with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,connector=networkConnectors,networkConnectorName=NC"))));
            allowing(managementContext).registerMBean(with(any(Object.class)), with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,service=Log4JConfiguration"))));
            allowing(managementContext).registerMBean(with(any(Object.class)), with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,destinationType=Topic,destinationName=ActiveMQ.Advisory.Connection"))));
            allowing(managementContext).registerMBean(with(any(Object.class)), with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,destinationType=Topic,destinationName=ActiveMQ.Advisory.NetworkBridge"))));
            allowing(managementContext).registerMBean(with(any(Object.class)), with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,destinationType=Topic,destinationName=ActiveMQ.Advisory.MasterBroker"))));
            allowing(managementContext).registerMBean(with(any(Object.class)), with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,service=jobScheduler,jobSchedulerName=JMS"))));
            allowing(managementContext).getObjectInstance(with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,connector=networkConnectors,networkConnectorName=NC"))));


            atLeast(maxReconnects - 1).of (managementContext).registerMBean(with(any(Object.class)), with(new NetworkBridgeObjectNameMatcher<ObjectName>(
                        new ObjectName("Test:type=Broker,brokerName=BrokerNC,connector=networkConnectors,networkConnectorName=NC,networkBridge=localhost/127.0.0.1_"
                            + proxy.getUrl().getPort())))); will(new CustomAction("signal register network mbean") {
                                @Override
                                public Object invoke(Invocation invocation) throws Throwable {
                                    LOG.info("Mbean Registered: " + invocation.getParameter(0));
                                    mbeanRegistered.release();
                                    return new ObjectInstance((ObjectName)invocation.getParameter(1), "discription");
                                }
                            });
            atLeast(maxReconnects - 1).of (managementContext).unregisterMBean(with(new NetworkBridgeObjectNameMatcher<ObjectName>(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,connector=networkConnectors,networkConnectorName=NC,networkBridge=localhost/127.0.0.1_"
                            + proxy.getUrl().getPort())))); will(new CustomAction("signal unregister network mbean") {
                                @Override
                                public Object invoke(Invocation invocation) throws Throwable {
                                    LOG.info("Mbean Unregistered: " + invocation.getParameter(0));
                                    mbeanUnregistered.release();
                                    return null;
                                }
                            });

            allowing(managementContext).unregisterMBean(with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC"))));
            allowing(managementContext).unregisterMBean(with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,service=Health"))));
            allowing(managementContext).unregisterMBean(with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,connector=networkConnectors,networkConnectorName=NC"))));
            allowing(managementContext).unregisterMBean(with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,service=Log4JConfiguration"))));
            allowing(managementContext).unregisterMBean(with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,destinationType=Topic,destinationName=ActiveMQ.Advisory.Connection"))));
            allowing(managementContext).unregisterMBean(with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,destinationType=Topic,destinationName=ActiveMQ.Advisory.NetworkBridge"))));
            allowing(managementContext).unregisterMBean(with(equal(
                    new ObjectName("Test:type=Broker,brokerName=BrokerNC,destinationType=Topic,destinationName=ActiveMQ.Advisory.MasterBroker"))));
        }});

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
