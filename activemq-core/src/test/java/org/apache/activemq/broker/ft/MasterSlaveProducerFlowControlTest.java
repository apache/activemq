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
package org.apache.activemq.broker.ft;

import org.apache.activemq.ProducerFlowControlTest;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.VMPendingSubscriberMessageStoragePolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MasterSlaveProducerFlowControlTest extends ProducerFlowControlTest {
    static final Log LOG = LogFactory.getLog(MasterSlaveProducerFlowControlTest.class);

    protected BrokerService createBroker() throws Exception {
        BrokerService service = super.createBroker();
        service.start();
        
        BrokerService slave = new BrokerService();
        slave.setBrokerName("Slave");
        slave.setPersistent(false);
        slave.setUseJmx(false);
        
        // Setup a destination policy where it takes lots of message at a time.
        // so that slave does not block first as there is no producer flow control
        // on the master connector
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        // don't apply the same memory limit as the master in this case
        //policy.setMemoryLimit(10);
        policy.setPendingSubscriberPolicy(new VMPendingSubscriberMessageStoragePolicy());
        policy.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        policy.setProducerFlowControl(true);
        policyMap.setDefaultEntry(policy);
        
        slave.setDestinationPolicy(policyMap);
        slave.setMasterConnectorURI(connector.getConnectUri().toString());
        slave.start();
        return service;
    }
}
