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

import java.io.File;
import java.net.URI;

import javax.jms.Destination;
import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.MessageIdList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ThreeBrokerVirtualTopicNetworkTest extends JmsMultipleBrokersTestSupport {
    private static final Log LOG = LogFactory.getLog(ThreeBrokerVirtualTopicNetworkTest.class);
    protected static final int MESSAGE_COUNT = 1;
    public boolean dynamicOnly = false;

    
    public void testNetworkVirtualTopic() throws Exception {
        int networkTTL = 6;
        boolean conduitSubs = true;
        // Setup broker networks
        bridgeAndConfigureBrokers("BrokerA", "BrokerB", dynamicOnly, networkTTL, conduitSubs);
        bridgeAndConfigureBrokers("BrokerA", "BrokerC", dynamicOnly, networkTTL, conduitSubs);
        bridgeAndConfigureBrokers("BrokerB", "BrokerA", dynamicOnly, networkTTL, conduitSubs);
        bridgeAndConfigureBrokers("BrokerB", "BrokerC", dynamicOnly, networkTTL, conduitSubs);
        bridgeAndConfigureBrokers("BrokerC", "BrokerA", dynamicOnly, networkTTL, conduitSubs);
        bridgeAndConfigureBrokers("BrokerC", "BrokerB", dynamicOnly, networkTTL, conduitSubs);

        startAllBrokers();      
        waitForBridgeFormation();
        
        // Setup destination
        Destination dest = createDestination("TEST.FOO", true);

        // Setup consumers
        MessageConsumer clientA = createConsumer("BrokerA", createDestination("Consumer.A.TEST.FOO", false));
        MessageConsumer clientB = createConsumer("BrokerB", createDestination("Consumer.B.TEST.FOO", false));
        MessageConsumer clientC = createConsumer("BrokerC", createDestination("Consumer.C.TEST.FOO", false));
        
        Thread.sleep(2000);

        // Send messages
        sendMessages("BrokerA", dest, 1);

        // Get message count
        MessageIdList msgsA = getConsumerMessages("BrokerA", clientA);
        MessageIdList msgsB = getConsumerMessages("BrokerB", clientB);
        MessageIdList msgsC = getConsumerMessages("BrokerC", clientC);

        msgsA.waitForMessagesToArrive(1);
        msgsB.waitForMessagesToArrive(1);
        msgsC.waitForMessagesToArrive(1);

        // ensure we don't get any more messages
        Thread.sleep(2000);
        
        assertEquals(1, msgsA.getMessageCount());
        assertEquals(1, msgsB.getMessageCount());
        assertEquals(1, msgsC.getMessageCount());
        
        // restart to ensure no hanging messages
        LOG.info("Restarting brokerA");
        BrokerItem brokerItem = brokers.remove("BrokerA");
        if (brokerItem != null) {
            brokerItem.destroy();
        }
        
        BrokerService restartedBroker = createAndConfigureBroker(new URI("broker:(tcp://localhost:61616)/BrokerA?useJmx=false"));
        bridgeAndConfigureBrokers("BrokerA", "BrokerB", dynamicOnly, networkTTL, conduitSubs);
        bridgeAndConfigureBrokers("BrokerA", "BrokerC", dynamicOnly, networkTTL, conduitSubs);
        restartedBroker.start();
        waitForBridgeFormation();
        
        clientA = createConsumer("BrokerA", createDestination("Consumer.A.TEST.FOO", false));
        
        Thread.sleep(2000);

        sendMessages("BrokerA", dest, 1);

        msgsA = getConsumerMessages("BrokerA", clientA);

        msgsA.waitForMessagesToArrive(1);
        msgsB.waitForMessagesToArrive(2);
        msgsC.waitForMessagesToArrive(2);

        // ensure we don't get any more messages
        Thread.sleep(2000);
        
        assertEquals(1, msgsA.getMessageCount());
        assertEquals(2, msgsB.getMessageCount());
        assertEquals(2, msgsC.getMessageCount());        
    }
    

    private void bridgeAndConfigureBrokers(String local, String remote, boolean dynamicOnly, int networkTTL, boolean conduitSubs) throws Exception {
        bridgeBrokers(local, remote, dynamicOnly, networkTTL, conduitSubs);
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        String options = new String("?useJmx=false&deleteAllMessagesOnStartup=true"); 
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61616)/BrokerA" + options));
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61617)/BrokerB" + options));
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61618)/BrokerC" + options));
    }
    
    private BrokerService createAndConfigureBroker(URI uri) throws Exception {
        BrokerService broker = createBroker(uri);
        
        File dataFileDir = new File("target/test-amq-data/kahadb/" + broker.getBrokerName());
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir);
        broker.setPersistenceAdapter(kaha);
        
        // make all topics virtual and consumers use the default prefix
        VirtualDestinationInterceptor virtualDestinationInterceptor = new VirtualDestinationInterceptor();
        virtualDestinationInterceptor.setVirtualDestinations(new VirtualDestination[]{new VirtualTopic()});
        DestinationInterceptor[] destinationInterceptors = new DestinationInterceptor[]{virtualDestinationInterceptor};
        broker.setDestinationInterceptors(destinationInterceptors);
        return broker;
    }
}
