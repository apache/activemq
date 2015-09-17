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


import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DestinationWildcardTest {
    protected final static String DESTNAME="DomainA.DomainB.TestMeA.TestMeB.Prioritised.Queue";
    protected final static int QUEUE_LIMIT = 5000000;
    protected static Logger LOG = LoggerFactory.getLogger(DestinationWildcardTest.class);
    private BrokerService broker = null;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setDeleteAllMessagesOnStartup(true);
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("Shutting down");
        if (broker != null && broker.isStarted()) {
            LOG.info("Broker still running, stopping it now.");
            broker.stop();
        }
        else {
            LOG.info("Broker not running, nothing to shutdown.");
        }
    }

    /**
     * Configures broker for two wildcard policies and one specific destination policy, creates a destination
     * and checks if the right policy is applied to the destination.
     */
    @Test
    public void testDestinationWildcardThreeEntries() throws Exception {
        LOG.info("testDestinationWildcard() called.");

        // configure broker for policyEntries
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();

        PolicyEntry e1 = new PolicyEntry();
        e1.setDestination(new ActiveMQQueue("DomainA.DomainB.TestMeA.TestMeB.Prioritised.Queue"));
        e1.setMemoryLimit(QUEUE_LIMIT);
        e1.setPrioritizedMessages(true);
        entries.add(e1);

        PolicyEntry e2 = new PolicyEntry();
        e2.setDestination(new ActiveMQQueue("DomainA.DomainB.*.*.Prioritised.Queue"));
        e2.setMemoryLimit(3000000);
        e2.setPrioritizedMessages(false);
        entries.add(e2);

        PolicyEntry e3 = new PolicyEntry();
        e3.setDestination(new ActiveMQQueue("DomainA.DomainB.>"));
        e3.setMemoryLimit(3000000);
        e3.setPrioritizedMessages(false);
        entries.add(e3);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();

        // verify broker isn't null
        Assert.assertNotNull(broker);

        // verify effective policy is in place.
        ManagedRegionBroker rb = (ManagedRegionBroker) broker.getRegionBroker();
        org.apache.activemq.broker.region.Queue queue = (Queue) rb.addDestination(new ConnectionContext(), new ActiveMQQueue(DESTNAME), true);
        Assert.assertTrue("PolicyEntry should have priorityMessages enabled for destination " + DESTNAME, queue.isPrioritizedMessages());
        long limit = queue.getMemoryUsage().getLimit();
        LOG.info("MemoryLimit of {}: expected: 5242880, actual: {}", DESTNAME, limit);
        Assert.assertEquals("Memory limit is expected to be " + QUEUE_LIMIT + " for this destination, but does not match.", QUEUE_LIMIT, limit);
    }


    /**
     * Configures broker for two wildcard policies, creates a destination
     * and checks if the policy is applied to the destination.
     */
    @Test
    public void testDestinationWildcardTwoEntries() throws Exception {
        LOG.info("testDestinationWildcard() called.");

        // configure broker for policyEntries
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();

        PolicyEntry e1 = new PolicyEntry();
        e1.setDestination(new ActiveMQQueue("DomainA.DomainB.*.*.Prioritised.Queue"));
        e1.setMemoryLimit(QUEUE_LIMIT);
        e1.setPrioritizedMessages(true);
        entries.add(e1);

        PolicyEntry e2 = new PolicyEntry();
        e2.setDestination(new ActiveMQQueue("DomainA.DomainB.>"));
        e2.setMemoryLimit(3000000);
        e2.setPrioritizedMessages(false);
        entries.add(e2);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();

        // verify broker isn't null
        Assert.assertNotNull(broker);

        // verify effective policy is in place.
        ManagedRegionBroker rb = (ManagedRegionBroker) broker.getRegionBroker();
        org.apache.activemq.broker.region.Queue queue = (Queue) rb.addDestination(new ConnectionContext(), new ActiveMQQueue(DESTNAME), true);
        Assert.assertTrue("PolicyEntry should have priorityMessages enabled for destination " + DESTNAME, queue.isPrioritizedMessages());
        long limit = queue.getMemoryUsage().getLimit();
        LOG.info("MemoryLimit of {}: expected: 5242880, actual: {}", DESTNAME, limit);
        Assert.assertEquals("Memory limit is expected to be " + QUEUE_LIMIT + " for this destination, but does not match.", QUEUE_LIMIT, limit);
    }


    @Test
    public void testDestinationWildcardOneEntry() throws Exception {
        LOG.info("testDestinationWildcard2() called.");
        // verify broker isn't null
        Assert.assertNotNull(broker);

        // configure broker for policyEntries
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();

        PolicyEntry e1 = new PolicyEntry();
        e1.setDestination(new ActiveMQQueue("DomainA.DomainB.*.*.Prioritised.Queue"));
        e1.setMemoryLimit(QUEUE_LIMIT);
        e1.setPrioritizedMessages(true);
        entries.add(e1);
        PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);
        broker.start();
        broker.waitUntilStarted();

        // verify effective policy is in place.
        ManagedRegionBroker rb = (ManagedRegionBroker) broker.getRegionBroker();
        org.apache.activemq.broker.region.Queue queue = (Queue) rb.addDestination(new ConnectionContext(), new ActiveMQQueue(DESTNAME), true);
        Assert.assertTrue("PolicyEntry should have priorityMessages enabled for destination " + DESTNAME, queue.isPrioritizedMessages());
        long limit = queue.getMemoryUsage().getLimit();
        LOG.info("MemoryLimit of {}: expected: 5000000, actual: {}", "DomainA", limit);
        Assert.assertEquals("Memory limit is expected to be " + 5000000 + " for this destination, but does not match.", 5000000, limit);
    }
}

