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
package org.apache.activemq.advisory;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.RegionBroker;

public class MasterSlaveTempQueueMemoryTest extends TempQueueMemoryTest {
    String masterBindAddress = "tcp://localhost:61616";
    String slaveBindAddress = "tcp://localhost:62616";
    BrokerService slave;
    
    /*
     * add a slave broker
     * @see org.apache.activemq.EmbeddedBrokerTestSupport#createBroker()
     */
    @Override
    protected BrokerService createBroker() throws Exception {
        // bindAddress is used by super.createBroker
        bindAddress = masterBindAddress;
        BrokerService master = super.createBroker();
        master.setBrokerName("master");
        bindAddress = slaveBindAddress;
        slave = super.createBroker();
        slave.setBrokerName("slave");
        slave.setMasterConnectorURI(masterBindAddress);
        bindAddress = masterBindAddress;
        return master;
    }

    @Override
    protected void startBroker() throws Exception {
        super.startBroker();
        slave.start();
    }

    @Override
    protected void tearDown() throws Exception {
        slave.stop();
        super.tearDown();
        
    }

    @Override
    public void testLoadRequestReply() throws Exception {
        super.testLoadRequestReply();
        
        // some checks on the slave
        AdvisoryBroker ab = (AdvisoryBroker) slave.getBroker().getAdaptor(
                AdvisoryBroker.class);
        
        assertEquals("the temp queues should not be visible as they are removed", 1, ab.getAdvisoryDestinations().size());
                       
        RegionBroker rb = (RegionBroker) slave.getBroker().getAdaptor(
                RegionBroker.class); 
               
        //serverDestination + 
        assertEquals(6, rb.getDestinationMap().size());     
        
        RegionBroker masterRb = (RegionBroker) broker.getBroker().getAdaptor(
                RegionBroker.class);

        // REVISIT the following two are not dependable at the moment, off by a small number
        // for some reason? The work for a COUNT < ~500
        //
        //assertEquals("inflight match", rb.getDestinationStatistics().getInflight().getCount(), masterRb.getDestinationStatistics().getInflight().getCount());
        //assertEquals("enqueues match", rb.getDestinationStatistics().getEnqueues().getCount(), masterRb.getDestinationStatistics().getEnqueues().getCount());
        
        assertEquals("dequeues match",
                rb.getDestinationStatistics().getDequeues().getCount(),
                masterRb.getDestinationStatistics().getDequeues().getCount());
        
        // slave does not actually dispatch any messages, so no request/reply(2) pair per iteration(COUNT)
        assertEquals("dispatched match",
                rb.getDestinationStatistics().getDispatched().getCount() + 2*COUNT, 
                masterRb.getDestinationStatistics().getDispatched().getCount());
    }
    
}
