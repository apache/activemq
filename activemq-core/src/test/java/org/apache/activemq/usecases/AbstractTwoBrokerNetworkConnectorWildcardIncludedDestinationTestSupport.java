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
import java.io.IOException;
import java.net.URI;

import javax.jms.MessageConsumer;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.MessageIdList;

public abstract class AbstractTwoBrokerNetworkConnectorWildcardIncludedDestinationTestSupport extends JmsMultipleBrokersTestSupport {
	
    protected static final int MESSAGE_COUNT = 1;
    boolean dynamicOnly = true;
    int networkTTL = 1;
    boolean conduit = true;
    boolean suppressDuplicateQueueSubscriptions = true;
    boolean decreaseNetworkConsumerPriority = true;
    
    /**
     * simple nwob
     */
    public void testSimpleNWOB() throws Exception {
       
        sendReceive("BrokerA", "local.test",  false, "BrokerB", "local.test",  false, 1, 0);
        sendReceive("BrokerA", "local.test",  true,  "BrokerB", "local.test",  true,  1, 0);
        sendReceive("BrokerA", "global.test", false, "BrokerB", "global.test", false, 1, 1);
        sendReceive("BrokerA", "global.test", true,  "BrokerB", "global.test", true,  1, 1);
        
    }
    
    /**
     * nwob with wild-card subscriptions
     */
    public void testSimpleNWOBWithWildcardSubscriptions() throws Exception {

        sendReceive("BrokerA", "local.test.1", false, "BrokerB", "local.test.>", false, 1, 0);
        sendReceive("BrokerA", "local.test.2", true, "BrokerB", "local.test.>", true, 1, 0);
        sendReceive("BrokerA", "global.test.1", false, "BrokerB", "global.test.>", false, 1, 1);
        sendReceive("BrokerA", "global.test.2", true, "BrokerB", "global.test.>", true, 1, 1);

    }
    
    /**
     * nwob with virtual destinations
     */
    public void testSimpleNWOBWithVirtualDestinations() throws Exception {
        
        sendReceive("BrokerA", "local.test",  true, "BrokerB", "Consumer.a.local.test",  false, 1, 0);
        sendReceive("BrokerA", "global.test", true, "BrokerB", "Consumer.a.global.test", false, 1, 1);
        
    }
    
    /**
     * nwob with virtual destinations and wild-card subscriptions
     */
    public void testSimpleNWOBWithVirtualDestinationsAndWildcardSubscriptions() throws Exception {
        
        sendReceive("BrokerA", "local.test.1",  true, "BrokerB", "Consumer.a.local.test.>",  false, 1, 0);
        sendReceive("BrokerA", "global.test.1", true, "BrokerB", "Consumer.a.global.test.>", false, 1, 1);
        
    }
    
    public void sendReceive(String broker1, String dest1, boolean topic1, String broker2, String dest2, boolean topic2, int send, int expected) throws Exception{
        MessageConsumer client = createConsumer(broker2, createDestination(dest2, topic2));
        Thread.sleep(1000);
        sendMessages(broker1, createDestination(dest1, topic1), send);
        MessageIdList msgs = getConsumerMessages(broker2, client);
        msgs.setMaximumDuration(1000);
        msgs.waitForMessagesToArrive(send);
        assertEquals(expected, msgs.getMessageCount());
        client.close();
        Thread.sleep(500);
    }

    protected abstract void addIncludedDestination(NetworkConnector networkConnector);

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        String options = new String("?useJmx=false&deleteAllMessagesOnStartup=true"); 
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61616)/BrokerA" + options));
        createAndConfigureBroker(new URI("broker:(tcp://localhost:61617)/BrokerB" + options));
        
        // Setup broker networks
        NetworkConnector nc = bridgeBrokers("BrokerA", "BrokerB", dynamicOnly, networkTTL, conduit);
        nc.setDecreaseNetworkConsumerPriority(decreaseNetworkConsumerPriority);
        nc.setSuppressDuplicateQueueSubscriptions(suppressDuplicateQueueSubscriptions);
        
        addIncludedDestination(nc);
        
        nc = bridgeBrokers("BrokerB", "BrokerA", dynamicOnly, networkTTL, conduit);
        nc.setDecreaseNetworkConsumerPriority(decreaseNetworkConsumerPriority);
        nc.setSuppressDuplicateQueueSubscriptions(suppressDuplicateQueueSubscriptions);
        
        addIncludedDestination(nc);
        
        startAllBrokers();
        
    }
    
    private BrokerService createAndConfigureBroker(URI uri) throws Exception {
        BrokerService broker = createBroker(uri);
        
        configurePersistenceAdapter(broker);
        
        // make all topics virtual and consumers use the default prefix
        VirtualDestinationInterceptor virtualDestinationInterceptor = new VirtualDestinationInterceptor();
        virtualDestinationInterceptor.setVirtualDestinations(new VirtualDestination[]{new VirtualTopic()});
        DestinationInterceptor[] destinationInterceptors = new DestinationInterceptor[]{virtualDestinationInterceptor};
        broker.setDestinationInterceptors(destinationInterceptors);
        return broker;
    }
    
    protected void configurePersistenceAdapter(BrokerService broker) throws IOException {
        File dataFileDir = new File("target/test-amq-data/kahadb/" + broker.getBrokerName());
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir);
        broker.setPersistenceAdapter(kaha);
    }
    
}
