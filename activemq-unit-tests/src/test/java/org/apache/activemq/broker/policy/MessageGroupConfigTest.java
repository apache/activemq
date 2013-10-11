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
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.group.CachedMessageGroupMap;
import org.apache.activemq.broker.region.group.MessageGroupHashBucket;
import org.apache.activemq.broker.region.group.MessageGroupMap;
import org.apache.activemq.broker.region.group.SimpleMessageGroupMap;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * 
 */
public class MessageGroupConfigTest extends TestSupport {
    protected BrokerService broker;

    @Override
    protected void setUp() throws Exception {
        super.setUp();  
    }

    @Override
    protected void tearDown() throws Exception {
        broker.stop();
        super.tearDown();    
    }



    public void testCachedGroupConfiguration() throws Exception {
        doTestGroupConfiguration("cached",CachedMessageGroupMap.class);
    }

    public void testCachedGroupConfigurationWithCacheSize() throws Exception {
        CachedMessageGroupMap result = (CachedMessageGroupMap) doTestGroupConfiguration("cached?cacheSize=10",CachedMessageGroupMap.class);
        assertEquals(10,result.getMaximumCacheSize());

    }

    public void testSimpleGroupConfiguration() throws Exception {
        doTestGroupConfiguration("simple", SimpleMessageGroupMap.class);
    }

    public void testBucketGroupConfiguration() throws Exception {
        doTestGroupConfiguration("bucket", MessageGroupHashBucket.class);
    }

    public void testBucketGroupConfigurationWithBucketCount() throws Exception {
        MessageGroupHashBucket result = (MessageGroupHashBucket) doTestGroupConfiguration("bucket?bucketCount=2", MessageGroupHashBucket.class);
        assertEquals(2,result.getBucketCount());
    }

    public MessageGroupMap doTestGroupConfiguration(String type, Class classType) throws Exception {
        broker = new BrokerService();

        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setMessageGroupMapFactoryType(type);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        super.topic = false;
        ActiveMQDestination destination = (ActiveMQDestination) createDestination("org.apache.foo");
        Queue brokerDestination = (Queue) broker.getDestination(destination);

        assertNotNull(brokerDestination);
        MessageGroupMap messageGroupMap = brokerDestination.getMessageGroupOwners();
        assertNotNull(messageGroupMap);
        assertTrue(messageGroupMap.getClass().isAssignableFrom(classType));
        return messageGroupMap;
    }


}
