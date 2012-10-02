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
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import junit.framework.Test;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.transport.vm.VMTransportFactory;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.SocketProxy;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class BrokerQueueNetworkWithDisconnectTest extends JmsMultipleBrokersTestSupport {
    private static final Log LOG = LogFactory.getLog(BrokerQueueNetworkWithDisconnectTest.class);
    private static final int NETWORK_DOWN_TIME = 5000;
    protected static final int MESSAGE_COUNT = 200;
    private static final String HUB = "HubBroker";
    private static final String SPOKE = "SpokeBroker";
    private SocketProxy socketProxy;
    private long networkDownTimeStart;
    public boolean useDuplexNetworkBridge = true;
    public boolean simulateStalledNetwork;
    private long inactiveDuration = 1000;
    private boolean useSocketProxy = true;

   
    public void initCombosForTestSendOnAReceiveOnBWithTransportDisconnect() {
        addCombinationValues( "useDuplexNetworkBridge", new Object[]{ Boolean.TRUE, Boolean.FALSE} );
        addCombinationValues( "simulateStalledNetwork", new Object[]{ Boolean.TRUE } );
    }
    
    public void testSendOnAReceiveOnBWithTransportDisconnect() throws Exception {
        bridgeBrokers(SPOKE, HUB);

        startAllBrokers();

        // Setup destination
        Destination dest = createDestination("TEST.FOO", false);
        
        // Setup consumers
        MessageConsumer client = createConsumer(HUB, dest);
        
        // allow subscription information to flow back to Spoke
        sleep(600);
        
        // Send messages
        sendMessages(SPOKE, dest, MESSAGE_COUNT);

        MessageIdList msgs = getConsumerMessages(HUB, client);
        msgs.waitForMessagesToArrive(MESSAGE_COUNT);

        assertTrue("At least message " + MESSAGE_COUNT + 
                " must be recieved, duplicates are expected, count=" + msgs.getMessageCount(),
                MESSAGE_COUNT <= msgs.getMessageCount());
    }

    public void testNoStuckConnectionsWithTransportDisconnect() throws Exception {
        inactiveDuration=60000l;
        useDuplexNetworkBridge = true;

        bridgeBrokers(SPOKE, HUB);

        final BrokerItem hub = brokers.get(HUB);
        hub.broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    int sleepCount = 2;
                    @Override
                    public void removeConnection(ConnectionContext context,
                            ConnectionInfo info, Throwable error)
                            throws Exception {
                        try {
                            while(--sleepCount >= 0) {
                                LOG.info("sleeping for a bit in close impl to simulate load where reconnect fails due to a pending close");
                                TimeUnit.SECONDS.sleep(2);
                            }
                        } catch (Exception ignored) {}
                        super.removeConnection(context, info, error);
                    }
                }
        });
        startAllBrokers();
        waitForBridgeFormation();

        // kill the initiator side, leaving remote end intact
        // simulate async network breakage
        // remote side will need to spot duplicate network and stop/kill the original
        for (int i=0; i< 3;  i++) {
            socketProxy.halfClose();
            sleep(10000);
        }
        // wait for full reformation of bridge       
        // verify no extra connections
        boolean allGood = Wait.waitFor(new Wait.Condition(){ 
                    public boolean isSatisified() throws Exception {
                        long numConnections = hub.broker.getTransportConnectors().get(0).getConnections().size();
                        LOG.info("Num connetions:" + numConnections);
                        return numConnections == 1;
                    }});
        if (!allGood) {
            dumpAllThreads("ExtraHubConnection");
        }
        assertTrue("should be only one transport connection for the single duplex network connector", allGood);

        allGood = Wait.waitFor(new Wait.Condition(){
                    public boolean isSatisified() throws Exception {
                        long numVmConnections = VMTransportFactory.SERVERS.get(HUB).getConnectionCount();
                        LOG.info("Num VM connetions:" + numVmConnections);
                        return numVmConnections == 1;
                    }});
        if (!allGood) {
            dumpAllThreads("ExtraHubVMConnection");
        }
        assertTrue("should be only one vm connection for the single network duplex network connector", allGood);
    }
    
    public void testTwoDuplexNCsAreAllowed() throws Exception {
        useDuplexNetworkBridge = true;
        useSocketProxy = false;

        NetworkConnector connector = bridgeBrokers(SPOKE, HUB);
        connector.setName("FirstDuplex");
        connector = bridgeBrokers(SPOKE, HUB);
        connector.setName("SecondDuplex");

        startAllBrokers(); 
        waitForBridgeFormation();

        BrokerItem hub = brokers.get(HUB);
        assertEquals("Has two transport Connectors", 2, hub.broker.getTransportConnectors().get(0).getConnections().size());
    }
    
    @Override
    protected void startAllBrokers() throws Exception {
        // Ensure HUB is started first so bridge will be active from the get go
        BrokerItem brokerItem = brokers.get(HUB);
        brokerItem.broker.start();
        brokerItem = brokers.get(SPOKE);
        brokerItem.broker.start();
        sleep(600);
    }

    public void setUp() throws Exception {
        networkDownTimeStart = 0;
        inactiveDuration = 1000;
        useSocketProxy = true;
        super.setAutoFail(true);
        super.setUp();
        final String options = "?persistent=true&useJmx=false&deleteAllMessagesOnStartup=true";
        createBroker(new URI("broker:(tcp://localhost:61617)/" + HUB + options));
        createBroker(new URI("broker:(tcp://localhost:61616)/" + SPOKE + options));
    }
    
    public void tearDown() throws Exception {
        super.tearDown();
        if (socketProxy != null) {
            socketProxy.close();
        }
    }
    
    public static Test suite() {
        return suite(BrokerQueueNetworkWithDisconnectTest.class);
    }
       
    @Override
    protected void onSend(int i, TextMessage msg) {
        sleep(50);
        if (i == 50 || i == 150) {
            if (simulateStalledNetwork) {
                socketProxy.pause();
            } else {
                socketProxy.close();
            }
            networkDownTimeStart = System.currentTimeMillis();
        } else if (networkDownTimeStart > 0) {
             // restart after NETWORK_DOWN_TIME seconds
             if (networkDownTimeStart + NETWORK_DOWN_TIME < System.currentTimeMillis()) {
                 if (simulateStalledNetwork) {
                     socketProxy.goOn();
                 } else {
                     socketProxy.reopen();
                 }
                 networkDownTimeStart = 0;
             } else {
                 // slow message production to allow bridge to recover and limit message duplication
                 sleep(500);
            }
        }
        super.onSend(i, msg);
    }

    private void sleep(int milliSecondTime) {
        try {
            Thread.sleep(milliSecondTime);
        } catch (InterruptedException igonred) {
        }    
    }

    @Override
    protected NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker, boolean dynamicOnly, int networkTTL, boolean conduit, boolean failover) throws Exception {
        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI;
        if (!transportConnectors.isEmpty()) {
            remoteURI = ((TransportConnector)transportConnectors.get(0)).getConnectUri();
            if (useSocketProxy) {
                socketProxy = new SocketProxy(remoteURI);
                remoteURI = socketProxy.getUrl();
            }
            DiscoveryNetworkConnector connector = new DiscoveryNetworkConnector(new URI("static:(" + remoteURI 
                    + "?wireFormat.maxInactivityDuration=" + inactiveDuration + "&wireFormat.maxInactivityDurationInitalDelay=" + inactiveDuration + ")?useExponentialBackOff=false"));
            connector.setDynamicOnly(dynamicOnly);
            connector.setNetworkTTL(networkTTL);
            localBroker.addNetworkConnector(connector);
            maxSetupTime = 2000;
            if (useDuplexNetworkBridge) {
                connector.setDuplex(true);
            }
            return connector;
        } else {
            throw new Exception("Remote broker has no registered connectors.");
        }
    }
}
