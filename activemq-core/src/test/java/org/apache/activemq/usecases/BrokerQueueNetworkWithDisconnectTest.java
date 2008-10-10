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

import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import junit.framework.Test;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.JmsMultipleBrokersTestSupport.BrokerItem;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.util.SocketProxy;


public class BrokerQueueNetworkWithDisconnectTest extends JmsMultipleBrokersTestSupport {
    private static final int NETWORK_DOWN_TIME = 5000;
    protected static final int MESSAGE_COUNT = 200;
    private static final String HUB = "HubBroker";
    private static final String SPOKE = "SpokeBroker";
    private SocketProxy socketProxy;
    private long networkDownTimeStart;
    public boolean useDuplexNetworkBridge;
    public boolean sumulateStalledNetwork;

   
    public void initCombosForTestSendOnAReceiveOnBWithTransportDisconnect() {
        addCombinationValues( "useDuplexNetworkBridge", new Object[]{ Boolean.TRUE, Boolean.FALSE} );
        addCombinationValues( "sumulateStalledNetwork", new Object[]{ Boolean.TRUE } );
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
        super.setAutoFail(true);
        super.setUp();
        final String options = "?persistent=true&useJmx=false&deleteAllMessagesOnStartup=true";
        createBroker(new URI("broker:(tcp://localhost:61617)/" + HUB + options));
        createBroker(new URI("broker:(tcp://localhost:61616)/" + SPOKE + options));
    }
    
    public static Test suite() {
        return suite(BrokerQueueNetworkWithDisconnectTest.class);
    }
       
    @Override
    protected void onSend(int i, TextMessage msg) {
        sleep(50);
        if (i == 50 || i == 150) {
            if (sumulateStalledNetwork) {
                socketProxy.pause();
            } else {
                socketProxy.close();
            }
            networkDownTimeStart = System.currentTimeMillis();
        } else if (networkDownTimeStart > 0) {
             // restart after NETWORK_DOWN_TIME seconds
             if (networkDownTimeStart + NETWORK_DOWN_TIME < System.currentTimeMillis()) {
                 if (sumulateStalledNetwork) {
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
    protected NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker, boolean dynamicOnly, int networkTTL) throws Exception {
        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI;
        if (!transportConnectors.isEmpty()) {
            remoteURI = ((TransportConnector)transportConnectors.get(0)).getConnectUri();
            socketProxy = new SocketProxy(remoteURI);
            DiscoveryNetworkConnector connector = new DiscoveryNetworkConnector(new URI("static:(" + socketProxy.getUrl() 
                    + "?wireFormat.maxInactivityDuration=1000&wireFormat.maxInactivityDurationInitalDelay=1000)?useExponentialBackOff=false"));
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
