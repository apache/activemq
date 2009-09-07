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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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


@RunWith(JMock.class)
public class DiscoveryNetworkReconnectTest {

    private static final Log LOG = LogFactory.getLog(DiscoveryNetworkReconnectTest.class);
    final int maxReconnects = 5;
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
        
        public boolean matches(Object arg0) {
            ObjectName other = (ObjectName) arg0;
            ObjectName mine = (ObjectName) name;
            return other.getKeyProperty("Type").equals(mine.getKeyProperty("Type")) &&
                other.getKeyProperty("NetworkConnectorName").equals(mine.getKeyProperty("NetworkConnectorName"));
        }

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
        
        proxy = new SocketProxy(brokerA.getTransportConnectors().get(0).getConnectUri());
        managementContext = context.mock(ManagementContext.class);
        
        context.checking(new Expectations(){{
            allowing (managementContext).getJmxDomainName(); will (returnValue("Test"));
            allowing (managementContext).start();
            allowing (managementContext).stop();            
            
            // expected MBeans
            allowing (managementContext).registerMBean(with(any(Object.class)), with(equal(
                    new ObjectName("Test:BrokerName=BrokerNC,Type=Broker"))));
            allowing (managementContext).registerMBean(with(any(Object.class)), with(equal(
                    new ObjectName("Test:BrokerName=BrokerNC,Type=NetworkConnector,NetworkConnectorName=localhost"))));
            allowing (managementContext).registerMBean(with(any(Object.class)), with(equal(            
                    new ObjectName("Test:BrokerName=BrokerNC,Type=Topic,Destination=ActiveMQ.Advisory.Connection"))));
            
            atLeast(maxReconnects - 1).of (managementContext).registerMBean(with(any(Object.class)), with(new NetworkBridgeObjectNameMatcher<ObjectName>(
                        new ObjectName("Test:BrokerName=BrokerNC,Type=NetworkBridge,NetworkConnectorName=localhost,Name=localhost/127.0.0.1_" 
                            + proxy.getUrl().getPort())))); will(new CustomAction("signal register network mbean") {
                                public Object invoke(Invocation invocation) throws Throwable {
                                    LOG.info("Mbean Registered: " + invocation.getParameter(0));
                                    mbeanRegistered.release();
                                    return new ObjectInstance((ObjectName)invocation.getParameter(0), "dscription");
                                }
                            });
            atLeast(maxReconnects - 1).of (managementContext).unregisterMBean(with(new NetworkBridgeObjectNameMatcher<ObjectName>(
                    new ObjectName("Test:BrokerName=BrokerNC,Type=NetworkBridge,NetworkConnectorName=localhost,Name=localhost/127.0.0.1_" 
                            + proxy.getUrl().getPort())))); will(new CustomAction("signal unregister network mbean") {
                                public Object invoke(Invocation invocation) throws Throwable {
                                    LOG.info("Mbean Unregistered: " + invocation.getParameter(0));
                                    mbeanUnregistered.release();
                                    return null;
                                }
                            });
           
            allowing (managementContext).unregisterMBean(with(equal(
                    new ObjectName("Test:BrokerName=BrokerNC,Type=Broker"))));
            allowing (managementContext).unregisterMBean(with(equal(
                    new ObjectName("Test:BrokerName=BrokerNC,Type=NetworkConnector,NetworkConnectorName=localhost"))));
            allowing (managementContext).unregisterMBean(with(equal(            
                    new ObjectName("Test:BrokerName=BrokerNC,Type=Topic,Destination=ActiveMQ.Advisory.Connection"))));        
        }});
        
        brokerB = new BrokerService();
        brokerB.setManagementContext(managementContext);
        brokerB.setBrokerName("BrokerNC");
        configure(brokerB);
    }

    @After
    public void tearDown() throws Exception {
        brokerA.stop();
        brokerB.stop();
        proxy.close();
    }
    
    private void configure(BrokerService broker) {
        broker.setPersistent(false);
        broker.setUseJmx(true);      
    }
    
    @Test
    public void testMulicastReconnect() throws Exception {     
        
        // control multicast advertise agent to inject proxy
        agent = MulticastDiscoveryAgentFactory.createDiscoveryAgent(new URI(discoveryAddress));
        agent.registerService(proxy.getUrl().toString());
        agent.start();

        brokerB.addNetworkConnector(discoveryAddress + "&wireFormat.maxInactivityDuration=1000&wireFormat.maxInactivityDurationInitalDelay=1000");
        brokerB.start();
        doReconnect();
    }
    
    

    @Test
    public void testSimpleReconnect() throws Exception {
        brokerB.addNetworkConnector("simple://(" + proxy.getUrl() 
                + ")?useExponentialBackOff=false&initialReconnectDelay=500&wireFormat.maxInactivityDuration=1000&wireFormat.maxInactivityDurationInitalDelay=1000");
        brokerB.start();       
        doReconnect();
    }

    private void doReconnect() throws Exception {
        
        for (int i=0; i<maxReconnects; i++) {
            // Wait for connection
            assertTrue("we got a network connection in a timely manner", Wait.waitFor(new Wait.Condition() {
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
            
            // let a reconnect succeed
            proxy.goOn();       
        }
    }
}
