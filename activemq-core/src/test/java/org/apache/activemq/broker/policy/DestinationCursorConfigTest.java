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
package org.apache.activemq.broker.policy;

import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PendingSubscriberMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.VMPendingSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 * 
 */
public class DestinationCursorConfigTest extends TestSupport {
    protected BrokerService broker;

    @Override
    protected void setUp() throws Exception {
        broker = createBroker();
        super.setUp();  
    }

    @Override
    protected void tearDown() throws Exception {
        broker.stop();
        super.tearDown();    
    }

    protected BrokerService createBroker() throws Exception {
        BrokerFactoryBean factory = new BrokerFactoryBean(new ClassPathResource("org/apache/activemq/broker/policy/cursor.xml"));
        factory.afterPropertiesSet();
        BrokerService answer = factory.getBroker();
        return answer;
    }

    public void testQueueConfiguration() throws Exception {
        super.topic = false;
        ActiveMQDestination destination = (ActiveMQDestination) createDestination("org.apache.foo");
        PolicyEntry entry = broker.getDestinationPolicy().getEntryFor(destination);
        PendingQueueMessageStoragePolicy policy = entry.getPendingQueuePolicy();
        assertNotNull(policy);
        assertTrue("Policy is: " + policy, policy instanceof VMPendingQueueMessageStoragePolicy);
    }

    public void testTopicConfiguration() throws Exception {
        super.topic = true;
        ActiveMQDestination destination = (ActiveMQDestination) createDestination("org.apache.foo");
        PolicyEntry entry = broker.getDestinationPolicy().getEntryFor(destination);
        PendingSubscriberMessageStoragePolicy policy = entry.getPendingSubscriberPolicy();
        assertNotNull(policy);
        assertFalse(entry.isProducerFlowControl());
        assertTrue(entry.getMemoryLimit()==(1024*1024));
        assertTrue("subscriberPolicy is: " + policy, policy instanceof VMPendingSubscriberMessageStoragePolicy);
    }
}
