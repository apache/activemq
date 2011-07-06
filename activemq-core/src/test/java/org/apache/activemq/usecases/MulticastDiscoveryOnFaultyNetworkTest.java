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
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.MessageIdList;


public class MulticastDiscoveryOnFaultyNetworkTest extends JmsMultipleBrokersTestSupport {
    protected static final int MESSAGE_COUNT = 200;
    private static final String HUB = "HubBroker";
    private static final String SPOKE = "SpokeBroker";
    public boolean useDuplexNetworkBridge;

   private TransportConnector mCastTrpConnector;
   
    public void initCombosForTestSendOnAFaultyTransport() {
        addCombinationValues( "useDuplexNetworkBridge", new Object[]{ Boolean.TRUE , Boolean.FALSE } );
        addCombinationValues( "sumulateStalledNetwork", new Object[]{ Boolean.TRUE } );
    }
    
    public void testSendOnAFaultyTransport() throws Exception {
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
	msgs.setMaximumDuration(200000L);
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
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        final String options = "?persistent=false&useJmx=false&deleteAllMessagesOnStartup=true";
        createBroker(new URI("broker:(tcpfaulty://localhost:61617)/" + HUB + options));
        createBroker(new URI("broker:(tcpfaulty://localhost:61616)/" + SPOKE + options));
    }
    
    public static Test suite() {
        return suite(MulticastDiscoveryOnFaultyNetworkTest.class);
    }
       
    @Override
    protected void onSend(int i, TextMessage msg) {
        sleep(50);
    }

    private void sleep(int milliSecondTime) {
        try {
            Thread.sleep(milliSecondTime);
        } catch (InterruptedException igonred) {
        }    
    }


    @Override
    protected NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker, boolean dynamicOnly, int networkTTL, boolean conduit, boolean failover) throws Exception {
        DiscoveryNetworkConnector connector = new DiscoveryNetworkConnector(new URI("multicast://default?group=TESTERIC&useLocalHost=false"));
        connector.setDynamicOnly(dynamicOnly);
        connector.setNetworkTTL(networkTTL);
        localBroker.addNetworkConnector(connector);
        maxSetupTime = 2000;
        if (useDuplexNetworkBridge) {
            connector.setDuplex(true);
        }

        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        if (!transportConnectors.isEmpty()) {
		    mCastTrpConnector = ((TransportConnector)transportConnectors.get(0));
		    mCastTrpConnector.setDiscoveryUri(new URI("multicast://default?group=TESTERIC"));
	    }
	    return connector;
    }
}
