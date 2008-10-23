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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Destination;

import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.usage.SystemUsage;

public class QueueMemoryFullMultiBrokersTest extends JmsMultipleBrokersTestSupport {
    public static final int BROKER_COUNT = 2;
    public static final int MESSAGE_COUNT = 2000;
   
    public void testQueueNetworkWithConsumerFull() throws Exception {
        
        bridgeAllBrokers();
        startAllBrokers();

        Destination dest = createDestination("TEST.FOO", false);

        sendMessages("Broker1", dest, 50);

        CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);
        createConsumer("Broker2", dest, latch);
        assertConsumersConnect("Broker1", dest, 1, 30000);
        sendMessages("Broker1", dest, MESSAGE_COUNT - 50);

        // Wait for messages to be delivered
        assertTrue("Missing " + latch.getCount() + " messages", latch.await(45, TimeUnit.SECONDS));
        
        // verify stats, all messages acked
        BrokerService broker1 = brokers.get("Broker1").broker;
        RegionBroker regionBroker = (RegionBroker) broker1.getRegionBroker();
        // give the acks a chance to flow
        Thread.sleep(2000);
        Queue internalQueue = (Queue) regionBroker.getDestinations(ActiveMQDestination.transform(dest)).iterator().next(); 
        
        assertTrue("All messages are consumed and acked from source:" + internalQueue, internalQueue.getMessages().isEmpty());
        assertEquals("messages source:" + internalQueue, 0, internalQueue.getDestinationStatistics().getMessages().getCount());
        assertEquals("inflight source:" + internalQueue, 0, internalQueue.getDestinationStatistics().getInflight().getCount());
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
        messageSize = 1024;
        
        // Setup n brokers
        for (int i = 1; i <= BROKER_COUNT; i++) {
            createBroker(new URI("broker:()/Broker" + i + "?persistent=false&useJmx=false"));
        }
        BrokerService broker2 = brokers.get("Broker2").broker;
        applyMemoryLimitPolicy(broker2);
    }

    private void applyMemoryLimitPolicy(BrokerService broker) {
        final SystemUsage memoryManager = new SystemUsage();
        memoryManager.getMemoryUsage().setLimit(1024 * 50); // 50 MB
        broker.setSystemUsage(memoryManager);

        final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
        final PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024 * 4); // Set to 2 kb
        entry.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        policyEntries.add(entry);

        final PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(policyEntries);
        broker.setDestinationPolicy(policyMap);
        
    }
}
