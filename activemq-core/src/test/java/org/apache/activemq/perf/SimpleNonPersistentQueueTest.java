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
package org.apache.activemq.perf;

import java.util.ArrayList;
import java.util.List;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.NoSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.VMPendingSubscriberMessageStoragePolicy;

/**
 * @version $Revision: 1.3 $
 */
public class SimpleNonPersistentQueueTest extends SimpleQueueTest {

    protected void setUp() throws Exception {
        numberOfConsumers = 10;
        numberofProducers = 10;
        //this.consumerSleepDuration=100;
        super.setUp();
    }
    protected PerfProducer createProducer(ConnectionFactory fac, Destination dest, int number, byte[] payload) throws JMSException {
        PerfProducer pp = new PerfProducer(fac, dest, payload);
        pp.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        //pp.setTimeToLive(100);
        return pp;
    }
    
    protected void configureBroker(BrokerService answer,String uri) throws Exception {
        answer.setPersistent(false);
        final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
        final PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024 * 1024 * 1); // Set to 1 MB
        entry.setOptimizedDispatch(true);
        entry.setLazyDispatch(true);
        policyEntries.add(entry);

        
        final PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(policyEntries);
        answer.setDestinationPolicy(policyMap);
        super.configureBroker(answer, uri);
    }
}
