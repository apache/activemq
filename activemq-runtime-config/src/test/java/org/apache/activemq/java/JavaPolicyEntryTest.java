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
package org.apache.activemq.java;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RuntimeConfigTestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.BaseDestination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.plugin.java.JavaRuntimeConfigurationBroker;
import org.apache.activemq.plugin.java.JavaRuntimeConfigurationPlugin;
import org.junit.Test;

public class JavaPolicyEntryTest extends RuntimeConfigTestSupport {

    public static final int SLEEP = 2; // seconds
    private JavaRuntimeConfigurationBroker javaConfigBroker;

    public void startBroker(BrokerService brokerService) throws Exception {
        this.brokerService = brokerService;
        brokerService.setPlugins(new BrokerPlugin[]{new JavaRuntimeConfigurationPlugin()});
        brokerService.setPersistent(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        javaConfigBroker =
                (JavaRuntimeConfigurationBroker) brokerService.getBroker().getAdaptor(JavaRuntimeConfigurationBroker.class);
    }

    /**
     * Test modifying a policy
     *
     * @throws Exception
     */
    @Test
    public void testMod() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024);
        policyMap.setPolicyEntries(Arrays.asList(entry));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());
        verifyQueueLimit("Before", 1024);

        //Reapply new limit
        entry.setMemoryLimit(4194304);
        javaConfigBroker.modifyPolicyEntry(entry);
        TimeUnit.SECONDS.sleep(SLEEP);

        verifyQueueLimit("After", 4194304);

      // change to existing dest
        verifyQueueLimit("Before", 4194304);
    }

    /**
     * Test modifying a policy but only applying a subset o
     * properties retroactively to existing destinations
     *
     * @throws Exception
     */
    @Test
    public void testModFilterProperties() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024);
        entry.setMaxPageSize(500);
        entry.setMaxBrowsePageSize(100);
        policyMap.setPolicyEntries(Arrays.asList(entry));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());
        verifyQueueLimit("Before", 1024);
        assertEquals(500, getQueue("Before").getMaxPageSize());
        assertEquals(100, getQueue("Before").getMaxBrowsePageSize());

        //Reapply new limit, add the property to the list of included properties
        entry.setMemoryLimit(4194304);
        entry.setMaxPageSize(300);
        entry.setMaxBrowsePageSize(200);
        Set<String> properties = new HashSet<>();
        properties.add("memoryLimit");
        properties.add("maxPageSize");
        javaConfigBroker.modifyPolicyEntry(entry, false, properties);
        TimeUnit.SECONDS.sleep(SLEEP);

        verifyQueueLimit("After", 4194304);
        assertEquals(300, getQueue("After").getMaxPageSize());
        assertEquals(200, getQueue("After").getMaxBrowsePageSize());

       // change to existing dest, maxBrowsePageSize was not included
        //in the property list so it should not have changed
        verifyQueueLimit("Before", 4194304);
        assertEquals(300, getQueue("Before").getMaxPageSize());
        assertEquals(100, getQueue("Before").getMaxBrowsePageSize());
    }

    @Test
    public void testModQueueAndTopic() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry qEntry = new PolicyEntry();
        qEntry.setQueue(">");
        qEntry.setPersistJMSRedelivered(true);
        PolicyEntry tEntry = new PolicyEntry();
        tEntry.setTopic(">");
        tEntry.setLazyDispatch(true);
        policyMap.setPolicyEntries(Arrays.asList(qEntry, tEntry));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());
        assertEquals(true, getQueue("queueBefore").isPersistJMSRedelivered());
        assertEquals(true, getTopic("topicBefore").isLazyDispatch());

        //Reapply new limit, add the property to the list of included properties
        qEntry.setPersistJMSRedelivered(false);
        tEntry.setLazyDispatch(false);
        Set<String> queueProperties = new HashSet<>();
        queueProperties.add("persistJMSRedelivered");
        Set<String> topicProperties = new HashSet<>();
        topicProperties.add("lazyDispatch");
        javaConfigBroker.modifyPolicyEntry(qEntry, false, queueProperties);
        javaConfigBroker.modifyPolicyEntry(tEntry, false, topicProperties);
        TimeUnit.SECONDS.sleep(SLEEP);

        assertEquals(false, getQueue("queueBefore").isPersistJMSRedelivered());
        assertEquals(false, getTopic("topicBefore").isLazyDispatch());

        assertEquals(false, getQueue("queueAfter").isPersistJMSRedelivered());
        assertEquals(false, getTopic("topicAfter").isLazyDispatch());
    }

    /**
     * Test that a property that is not part of the update methods (can't be changed after creation)
     * will not be applied to existing destinations but will be applied to new destinations
     *
     * @throws Exception
     */
    @Test
    public void testModFilterExcludedProperty() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setEnableAudit(true);
        policyMap.setPolicyEntries(Arrays.asList(entry));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());
        assertTrue(getQueue("Before").isEnableAudit());

        //Reapply new limit, add the property to the list of included properties
        entry.setEnableAudit(false);
        Set<String> properties = new HashSet<>();
        properties.add("enableAudit");
        javaConfigBroker.modifyPolicyEntry(entry, false, properties);
        TimeUnit.SECONDS.sleep(SLEEP);

        //no change because enableAudit is excluded
        assertTrue(getQueue("Before").isEnableAudit());

        //A new destination should have the property changed
        assertFalse(getQueue("After").isEnableAudit());
    }

    @Test
    public void testModFilterPropertiesInvalid() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024);
        policyMap.setPolicyEntries(Arrays.asList(entry));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());
        verifyQueueLimit("Before", 1024);

        //use a property that doesn't exist, so nothing should be updated
        entry.setMemoryLimit(4194304);
        Set<String> properties = new HashSet<>();
        properties.add("invalid");
        javaConfigBroker.modifyPolicyEntry(entry, false, properties);
        TimeUnit.SECONDS.sleep(SLEEP);

        //This should be unchanged as the list of properties only
        //has an invalid property so nothing will be re-applied retrospectively
        verifyQueueLimit("Before", 1024);

        //A new destination should be updated because the policy was changed
        verifyQueueLimit("After", 4194304);
    }

    @Test
    public void testModNewPolicyObject() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024);
        policyMap.setPolicyEntries(Arrays.asList(entry));
        brokerService.setDestinationPolicy(policyMap);


        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        verifyQueueLimit("Before", 1024);

        //Reapply new limit with new object that matches
        //the same destination, so it should still apply
        PolicyEntry entry2 = new PolicyEntry();
        entry2.setQueue(">");
        entry2.setMemoryLimit(4194304);
        javaConfigBroker.modifyPolicyEntry(entry2, true);
        TimeUnit.SECONDS.sleep(SLEEP);

        // These should change because the policy entry passed in
        //matched an existing entry but was not the same reference.
        //Since createOrReplace is true, we replace the entry with
        //this new entry and apply
        verifyQueueLimit("Before", 4194304);
        verifyQueueLimit("After", 4194304);
    }

    /**
     * Test that a new policy is added and applied
     * Test that a new policy will be added when setting createOrReplace to true
     * when calling modifyPolicyEntry
     *
     * @throws Exception
     */
    @Test
    public void testCreate() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(Arrays.asList());
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());
        verifyQueueLimit("Before", (int)brokerService.getSystemUsage().getMemoryUsage().getLimit());

        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024);

        //The true flag should add the new policy
        javaConfigBroker.modifyPolicyEntry(entry, true);
        TimeUnit.SECONDS.sleep(SLEEP);

        //Make sure the new policy is added and applied
        verifyQueueLimit("Before", 1024);
        verifyQueueLimit("After", 1024);
    }

    /**
     * Test that a new policy is not added
     * Pass a new policy to modifyPolicyEntry which should throw an exception
     * because the policy didn't already exist
     *
     * @throws Exception
     */
    @Test
    public void testCreateFalse() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(Arrays.asList());
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());
        verifyQueueLimit("Before", (int)brokerService.getSystemUsage().getMemoryUsage().getLimit());

        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024);
        //The default should NOT add this policy since it won't match an existing policy to modify
        boolean caughtException = false;
        try {
            javaConfigBroker.modifyPolicyEntry(entry);
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
        TimeUnit.SECONDS.sleep(SLEEP);

        //Make sure there was no change
        verifyQueueLimit("Before", (int)brokerService.getSystemUsage().getMemoryUsage().getLimit());
        verifyQueueLimit("After", (int)brokerService.getSystemUsage().getMemoryUsage().getLimit());
    }


    @Test
    public void testModNewPolicyObjectCreateOrReplaceFalse() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024);
        policyMap.setPolicyEntries(Arrays.asList(entry));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());
        verifyQueueLimit("Before", 1024);

        //Reapply new limit with new object that matches
        //the same destination, but createOrReplace is false
        PolicyEntry entry2 = new PolicyEntry();
        entry2.setQueue(">");
        entry2.setMemoryLimit(4194304);
        boolean caughtException = false;
        try {
            javaConfigBroker.modifyPolicyEntry(entry2, false);
        } catch (IllegalArgumentException e) {
            caughtException = true;
        }
        assertTrue(caughtException);
        TimeUnit.SECONDS.sleep(SLEEP);

        // These should not change because the policy entry passed in
        //matched an existing entry but was not the same reference.
        //Since createOrReplace is false, it should noo be updated
        verifyQueueLimit("Before", 1024);
        verifyQueueLimit("After", 1024);
    }

    @Test
    public void testModWithChildPolicy() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue("queue.>");
        entry.setMemoryLimit(1024);
        PolicyEntry entry2 = new PolicyEntry();
        entry2.setQueue("queue.child.>");
        entry2.setMemoryLimit(2048);
        policyMap.setPolicyEntries(Arrays.asList(entry, entry2));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.test"), false);
        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.child.test"), false);

        //check destinations before policy updates
        verifyQueueLimit("queue.test", 1024);
        verifyQueueLimit("queue.child.test", 2048);

        //Reapply new limit to policy 2
        entry2.setMemoryLimit(4194304);
        javaConfigBroker.modifyPolicyEntry(entry2);
        TimeUnit.SECONDS.sleep(SLEEP);

        //verify new dest and existing are changed
        verifyQueueLimit("queue.child.test", 4194304);
        verifyQueueLimit("queue.child.test2", 4194304);

        //verify that destination at a higher level policy is not affected
        verifyQueueLimit("queue.test", 1024);
    }

    @Test
    public void testModWithMultipleChildPolicies() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue("queue.>");
        entry.setMemoryLimit(1024);
        PolicyEntry entry2 = new PolicyEntry();
        entry2.setQueue("queue.child.>");
        entry2.setMemoryLimit(2048);
        PolicyEntry entry3 = new PolicyEntry();
        entry3.setQueue("queue.child.test");
        entry3.setMemoryLimit(5000);
        PolicyEntry entry4 = new PolicyEntry();
        entry4.setQueue("queue.child.test.test");
        entry4.setMemoryLimit(5100);
        PolicyEntry entry5 = new PolicyEntry();
        entry5.setQueue("queue.child.a");
        entry5.setMemoryLimit(5200);
        policyMap.setPolicyEntries(Arrays.asList(entry, entry2, entry3, entry4, entry5));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.child.>"), false);
        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.test"), false);
        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.child.test2"), false);

        //check destinations before policy updates
        verifyQueueLimit("queue.test", 1024);
        verifyQueueLimit("queue.child.test2", 2048);

        //Reapply new limit to policy 2
        entry3.setMemoryLimit(4194304);
        javaConfigBroker.modifyPolicyEntry(entry);
        TimeUnit.SECONDS.sleep(SLEEP);

        //should be unchanged
        verifyQueueLimit("queue.child.>", 2048);

        //verify new dest and existing are changed
        verifyQueueLimit("queue.child.test", 4194304);

        //verify that destination at a higher level policy is not affected
        verifyQueueLimit("queue.test", 1024);
    }

    @Test
    public void testModWithChildWildcardPolicies() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024);
        PolicyEntry entry2 = new PolicyEntry();
        entry2.setQueue("queue.child.>");
        entry2.setMemoryLimit(2048);
        PolicyEntry entry3 = new PolicyEntry();
        entry3.setQueue("queue.child.one.>");
        entry3.setMemoryLimit(4096);

        policyMap.setPolicyEntries(Arrays.asList(entry, entry2, entry3));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.>"), false);
        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.child.>"), false);
        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.child.one.>"), false);
        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.child.one"), false);

        //check destinations before policy updates
        verifyQueueLimit("queue.>", 1024);
        verifyQueueLimit("queue.child.>", 2048);
        verifyQueueLimit("queue.child.one", 4096);

        //Reapply new limit to policy 2
        entry2.setMemoryLimit(4194304);
        javaConfigBroker.modifyPolicyEntry(entry2);
        TimeUnit.SECONDS.sleep(SLEEP);

        //verify that destination at a higher level policy is not affected
        verifyQueueLimit("queue.>", 1024);

        verifyQueueLimit("queue.child.>", 4194304);

        verifyQueueLimit("queue.child.one.>", 4096);
        verifyQueueLimit("queue.child.one", 4096);
    }

    @Test
    public void testModParentPolicy() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();

        PolicyEntry entry = new PolicyEntry();
        entry.setQueue("queue.>");
        entry.setMemoryLimit(1024);
        PolicyEntry entry2 = new PolicyEntry();
        entry2.setQueue("queue.child.>");
        entry2.setMemoryLimit(2048);
        policyMap.setPolicyEntries(Arrays.asList(entry, entry2));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.test"), false);
        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue("queue.child.test"), false);

        //check destinations before policy updates
        verifyQueueLimit("queue.test", 1024);
        verifyQueueLimit("queue.child.test", 2048);

        //Reapply new limit to policy
        entry.setMemoryLimit(4194304);
        javaConfigBroker.modifyPolicyEntry(entry);
        TimeUnit.SECONDS.sleep(SLEEP);

        //verify new dest and existing are not changed
        verifyQueueLimit("queue.child.test", 2048);
        verifyQueueLimit("queue.child.test2", 2048);

        //verify that destination at a higher level policy is changed
        verifyQueueLimit("queue.test", 4194304);
    }

    @Test
    public void testAddNdMod() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024);
        policyMap.setPolicyEntries(Arrays.asList(entry));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        verifyQueueLimit("Before", 1024);
        verifyTopicLimit("Before", brokerService.getSystemUsage().getMemoryUsage().getLimit());

        entry.setMemoryLimit(2048);
        javaConfigBroker.modifyPolicyEntry(entry);
        TimeUnit.SECONDS.sleep(SLEEP);

        PolicyEntry newEntry = new PolicyEntry();
        newEntry.setTopic(">");
        newEntry.setMemoryLimit(2048);
        javaConfigBroker.addNewPolicyEntry(newEntry);
        TimeUnit.SECONDS.sleep(SLEEP);

        verifyTopicLimit("After", 2048l);
        verifyQueueLimit("After", 2048);

        // change to existing dest
        verifyTopicLimit("Before", 2048l);
    }

    @Test
    public void testAddNdModWithMultiplePolicies() throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        entry.setMemoryLimit(1024);
        policyMap.setPolicyEntries(Arrays.asList(entry));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        verifyQueueLimit("Before", 1024);
        verifyTopicLimit("Before", brokerService.getSystemUsage().getMemoryUsage().getLimit());

        entry.setMemoryLimit(2048);
        javaConfigBroker.modifyPolicyEntry(entry);
        TimeUnit.SECONDS.sleep(SLEEP);

        PolicyEntry newEntry = new PolicyEntry();
        newEntry.setTopic("test2.>");
        newEntry.setMemoryLimit(2048);
        PolicyEntry newEntry2 = new PolicyEntry();
        newEntry2.setTopic("test2.test.>");
        newEntry2.setMemoryLimit(4000);
        javaConfigBroker.addNewPolicyEntry(newEntry);
        javaConfigBroker.addNewPolicyEntry(newEntry2);
        TimeUnit.SECONDS.sleep(SLEEP);

        verifyTopicLimit("test2.after", 2048l);
        verifyTopicLimit("test2.test.after", 4000l);
        //check existing modified entry
        verifyQueueLimit("After", 2048);

        // change to existing dest
        PolicyEntry newEntry3 = new PolicyEntry();
        newEntry3.setTopic(">");
        newEntry3.setMemoryLimit(5000);
        javaConfigBroker.addNewPolicyEntry(newEntry3);
        verifyTopicLimit("Before", 5000l);

        //reverify children
        verifyTopicLimit("test2.after", 2048l);
        verifyTopicLimit("test2.test.after", 4000l);
    }

    @Test
    public void testAllQueuePropertiesApplied() throws Exception {
        testAllQueuePropertiesAppliedFilter(null);
    }

    /**
     * Make sure all properties set on the filter Set are applied
     *
     * @throws Exception
     */
    @Test
    public void testAllQueuePropertiesAppliedFilter() throws Exception {
        testAllQueuePropertiesAppliedFilter(getQueuePropertySet());
    }

    /**
     * Make sure all properties set on the filter Set are applied
     *
     * @throws Exception
     */
    @Test
    public void testAllTopicPropertiesAppliedFilter() throws Exception {
        testAllTopicPropertiesAppliedFilter(getTopicPropertySet());
    }

    @Test
    public void testAllTopicPropertiesApplied() throws Exception {
        testAllTopicPropertiesAppliedFilter(null);
    }

    private void testAllQueuePropertiesAppliedFilter(Set<String> properties) throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");

        //initial config
        setAllDestPolicyProperties(entry, true, true, 10,
                100, 200, 1000, 400, 40, 30, true, true, 1000, true, true,
                30, true, true, true, true, true, true, true, true);
        setAllQueuePolicyProperties(entry, 10000, true, true, true, true, 100,
                100, true, true);

        policyMap.setPolicyEntries(Arrays.asList(entry));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        //validate config
        assertAllDestPolicyProperties(getQueue("Before"), true, true, 10,
                100, 200, 1000, 400, 40, 30, true, true, 1000, true, true,
                30, true, true, true, true, true, true, true, true);
        assertAllQueuePolicyProperties(getQueue("Before"), 10000, true, true, true, true, 100,
                100, true, true);


        //change config
        setAllDestPolicyProperties(entry, false, false, 100,
                1000, 2000, 10000, 4000, 400, 300, false, false, 1000, false, false,
                300, false, false, false, false, false, false, false, false);
        setAllQueuePolicyProperties(entry, 100000, false, false, false, false, 1000,
                1000, false, false);

        javaConfigBroker.modifyPolicyEntry(entry, false, properties);
        TimeUnit.SECONDS.sleep(SLEEP);

        assertAllDestPolicyProperties(getQueue("Before"), false, false, 100,
                1000, 2000, 10000, 4000, 400, 300, false, false, 1000, false, false,
                300, false, false, false, false, false, false, false, false);
        assertAllQueuePolicyProperties(getQueue("Before"), 100000, false, false, false, false, 1000,
                1000, false, false);

        //check new dest
        assertAllDestPolicyProperties(getQueue("After"), false, false, 100,
                1000, 2000, 10000, 4000, 400, 300, false, false, 1000, false, false,
                300, false, false, false, false, false, false, false, false);
        assertAllQueuePolicyProperties(getQueue("After"), 100000, false, false, false, false, 1000,
                1000, false, false);
    }

    private void testAllTopicPropertiesAppliedFilter(Set<String> properties) throws Exception {
        BrokerService brokerService = new BrokerService();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setTopic(">");

        //initial config
        setAllDestPolicyProperties(entry, true, true, 10,
                100, 200, 1000, 400, 40, 30, true, true, 1000, true, true,
                30, true, true, true, true, true, true, true, true);
        setAllTopicPolicyProperties(entry, 10000, true);

        policyMap.setPolicyEntries(Arrays.asList(entry));
        brokerService.setDestinationPolicy(policyMap);

        startBroker(brokerService);
        assertTrue("broker alive", brokerService.isStarted());

        //validate config
        assertAllDestPolicyProperties(getTopic("Before"), true, true, 10,
                100, 200, 1000, 400, 40, 30, true, true, 1000, true, true,
                30, true, true, true, true, true, true, true, true);
        assertAllTopicPolicyProperties(getTopic("Before"), 10000, true);


        //change config
        setAllDestPolicyProperties(entry, false, false, 100,
                1000, 2000, 10000, 4000, 400, 300, false, false, 1000, false, false,
                300, false, false, false, false, false, false, false, false);
        setAllTopicPolicyProperties(entry, 100000, false);

        javaConfigBroker.modifyPolicyEntry(entry, false, properties);
        TimeUnit.SECONDS.sleep(SLEEP);

        assertAllDestPolicyProperties(getTopic("Before"), false, false, 100,
                1000, 2000, 10000, 4000, 400, 300, false, false, 1000, false, false,
                300, false, false, false, false, false, false, false, false);
        assertAllTopicPolicyProperties(getTopic("Before"), 100000, false);

        //check new dest
        assertAllDestPolicyProperties(getTopic("After"), false, false, 100,
                1000, 2000, 10000, 4000, 400, 300, false, false, 1000, false, false,
                300, false, false, false, false, false, false, false, false);
        assertAllTopicPolicyProperties(getTopic("After"), 100000, false);
    }

    private void verifyQueueLimit(String dest, int memoryLimit) throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection) new ActiveMQConnectionFactory("vm://localhost").createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue(dest));

            assertEquals(memoryLimit, getQueue(dest).getMemoryUsage().getLimit());
        } finally {
            connection.close();
        }
    }

    private void verifyTopicLimit(String dest, long memoryLimit) throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection) new ActiveMQConnectionFactory("vm://localhost").createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createTopic(dest));

            assertEquals(memoryLimit, getTopic(dest).getMemoryUsage().getLimit());
        } finally {
            connection.close();
        }
    }

    private Queue getQueue(String queue) throws Exception {
        return (Queue) brokerService.getRegionBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQQueue(queue), false);
    }

    private Topic getTopic(String topic) throws Exception {
        return (Topic) brokerService.getRegionBroker().addDestination(
                brokerService.getAdminConnectionContext(), new ActiveMQTopic(topic), false);
    }

    private Set<String> getQueuePropertySet() {
        Set<String> properties = new HashSet<>(getDestPropertySet());
        properties.add("memoryLimit");
        properties.add("useConsumerPriority");
        properties.add("strictOrderDispatch");
        properties.add("optimizedDispatch");
        properties.add("lazyDispatch");
        properties.add("timeBeforeDispatchStarts");
        properties.add("consumersBeforeDispatchStarts");
        properties.add("allConsumersExclusiveByDefault");
        properties.add("persistJMSRedelivered");
        return properties;
    }

    private Set<String> getTopicPropertySet() {
        Set<String> properties = new HashSet<>(getDestPropertySet());
        properties.add("memoryLimit");
        properties.add("lazyDispatch");
        return properties;
    }

    private Set<String> getDestPropertySet() {
        Set<String> properties = new HashSet<>();
        properties.add("producerFlowControl");
        properties.add("alwaysRetroactive");
        properties.add("blockedProducerWarningInterval");
        properties.add("maxPageSize");
        properties.add("maxBrowsePageSize");
        properties.add("minimumMessageSize");
        properties.add("maxExpirePageSize");
        properties.add("cursorMemoryHighWaterMark");
        properties.add("storeUsageHighWaterMark");
        properties.add("gcInactiveDestinations");
        properties.add("gcWithNetworkConsumers");
        properties.add("inactiveTimeoutBeforeGC");
        properties.add("reduceMemoryFootprint");
        properties.add("doOptimizeMessageStore");
        properties.add("optimizeMessageStoreInFlightLimit");
        properties.add("advisoryForConsumed");
        properties.add("advisoryForDelivery");
        properties.add("advisoryForDiscardingMessages");
        properties.add("advisoryForSlowConsumers");
        properties.add("advisoryForFastProducers");
        properties.add("advisoryWhenFull");
        properties.add("includeBodyForAdvisory");
        properties.add("sendAdvisoryIfNoConsumers");
        return properties;

    }

    private void setAllQueuePolicyProperties(PolicyEntry entry, long memoryLimit, boolean useConsumerPriority,
            boolean strictOrderDispatch, boolean optimizedDispatch, boolean lazyDispatch,
            int timeBeforeDispatchStarts, int consumersBeforeDispatchStarts, boolean allConsumersExclusiveByDefault,
            boolean persistJMSRedelivered) {

        entry.setMemoryLimit(memoryLimit);
        entry.setUseConsumerPriority(useConsumerPriority);
        entry.setStrictOrderDispatch(strictOrderDispatch);
        entry.setOptimizedDispatch(optimizedDispatch);
        entry.setLazyDispatch(lazyDispatch);
        entry.setTimeBeforeDispatchStarts(timeBeforeDispatchStarts);
        entry.setConsumersBeforeDispatchStarts(consumersBeforeDispatchStarts);
        entry.setAllConsumersExclusiveByDefault(allConsumersExclusiveByDefault);
        entry.setPersistJMSRedelivered(persistJMSRedelivered);
    }

    private void setAllTopicPolicyProperties(PolicyEntry entry, long memoryLimit, boolean lazyDispatch) {
        entry.setMemoryLimit(memoryLimit);
        entry.setLazyDispatch(lazyDispatch);
    }

    private void setAllDestPolicyProperties(PolicyEntry entry, boolean producerFlowControl,
            boolean alwaysRetroactive, long blockedProducerWarningInterval, int maxPageSize,
            int maxBrowsePageSize, long minimumMessageSize, int maxExpirePageSize, int cursorMemoryHighWaterMark,
            int storeUsageHighWaterMark, boolean gcInactiveDestinations, boolean gcWithNetworkConsumers,
            long inactiveTimeoutBeforeGC,boolean reduceMemoryFootprint, boolean doOptimizeMessageStore,
            int optimizeMessageStoreInFlightLimit, boolean advisoryForConsumed, boolean advisoryForDelivery,
            boolean advisoryForDiscardingMessages, boolean advisoryForSlowConsumers, boolean advisoryForFastProducers,
            boolean advisoryWhenFull, boolean includeBodyForAdvisory, boolean sendAdvisoryIfNoConsumers) {

        entry.setProducerFlowControl(producerFlowControl);
        entry.setAlwaysRetroactive(alwaysRetroactive);
        entry.setBlockedProducerWarningInterval(blockedProducerWarningInterval);
        entry.setMaxPageSize(maxPageSize);
        entry.setMaxBrowsePageSize(maxBrowsePageSize);
        entry.setMinimumMessageSize(minimumMessageSize);
        entry.setMaxExpirePageSize(maxExpirePageSize);
        entry.setCursorMemoryHighWaterMark(cursorMemoryHighWaterMark);
        entry.setStoreUsageHighWaterMark(storeUsageHighWaterMark);
        entry.setGcInactiveDestinations(gcInactiveDestinations);
        entry.setGcWithNetworkConsumers(gcWithNetworkConsumers);
        entry.setInactiveTimeoutBeforeGC(inactiveTimeoutBeforeGC);
        entry.setReduceMemoryFootprint(reduceMemoryFootprint);
        entry.setDoOptimzeMessageStorage(doOptimizeMessageStore);
        entry.setOptimizeMessageStoreInFlightLimit(optimizeMessageStoreInFlightLimit);
        entry.setAdvisoryForConsumed(advisoryForConsumed);
        entry.setAdvisoryForDelivery(advisoryForDelivery);
        entry.setAdvisoryForDiscardingMessages(advisoryForDiscardingMessages);
        entry.setAdvisoryForSlowConsumers(advisoryForSlowConsumers);
        entry.setAdvisoryForFastProducers(advisoryForFastProducers);
        entry.setAdvisoryWhenFull(advisoryWhenFull);
        entry.setIncludeBodyForAdvisory(includeBodyForAdvisory);
        entry.setSendAdvisoryIfNoConsumers(sendAdvisoryIfNoConsumers);
    }

    private void assertAllQueuePolicyProperties(Queue queue, long memoryLimit, boolean useConsumerPriority,
            boolean strictOrderDispatch, boolean optimizedDispatch, boolean lazyDispatch,
            int timeBeforeDispatchStarts, int consumersBeforeDispatchStarts, boolean allConsumersExclusiveByDefault,
            boolean persistJMSRedelivered) {

        assertEquals(memoryLimit, queue.getMemoryUsage().getLimit());
        assertEquals(useConsumerPriority, queue.isUseConsumerPriority());
        assertEquals(strictOrderDispatch, queue.isStrictOrderDispatch());
        assertEquals(optimizedDispatch, queue.isOptimizedDispatch());
        assertEquals(lazyDispatch, queue.isLazyDispatch());
        assertEquals(timeBeforeDispatchStarts, queue.getTimeBeforeDispatchStarts());
        assertEquals(consumersBeforeDispatchStarts, queue.getConsumersBeforeDispatchStarts());
        assertEquals(allConsumersExclusiveByDefault, queue.isAllConsumersExclusiveByDefault());
        assertEquals(persistJMSRedelivered, queue.isPersistJMSRedelivered());

    }

    private void assertAllTopicPolicyProperties(Topic topic, long memoryLimit, boolean lazyDispatch) {
        assertEquals(memoryLimit, topic.getMemoryUsage().getLimit());
        assertEquals(lazyDispatch, topic.isLazyDispatch());
    }

    private void assertAllDestPolicyProperties(BaseDestination dest, boolean producerFlowControl,
            boolean alwaysRetroactive, long blockedProducerWarningInterval, int maxPageSize,
            int maxBrowsePageSize, long minimumMessageSize, int maxExpirePageSize, int cursorMemoryHighWaterMark,
            int storeUsageHighWaterMark, boolean gcInactiveDestinations, boolean gcWithNetworkConsumers,
            long inactiveTimeoutBeforeGC,boolean reduceMemoryFootprint, boolean doOptimizeMessageStore,
            int optimizeMessageStoreInFlightLimit, boolean advisoryForConsumed, boolean advisoryForDelivery,
            boolean advisoryForDiscardingMessages, boolean advisoryForSlowConsumers, boolean advisoryForFastProducers,
            boolean advisoryWhenFull, boolean includeBodyForAdvisory, boolean sendAdvisoryIfNoConsumers) {


        assertEquals(producerFlowControl, dest.isProducerFlowControl());
        assertEquals(alwaysRetroactive, dest.isAlwaysRetroactive());
        assertEquals(blockedProducerWarningInterval, dest.getBlockedProducerWarningInterval());
        assertEquals(maxPageSize, dest.getMaxPageSize());
        assertEquals(maxBrowsePageSize, dest.getMaxBrowsePageSize());
        assertEquals(minimumMessageSize, dest.getMinimumMessageSize());
        assertEquals(maxExpirePageSize, dest.getMaxExpirePageSize());
        assertEquals(cursorMemoryHighWaterMark, dest.getCursorMemoryHighWaterMark());
        assertEquals(storeUsageHighWaterMark, dest.getStoreUsageHighWaterMark());
        assertEquals(gcInactiveDestinations, dest.isGcIfInactive());
        assertEquals(gcWithNetworkConsumers, dest.isGcWithNetworkConsumers());
        assertEquals(inactiveTimeoutBeforeGC, dest.getInactiveTimeoutBeforeGC());
        assertEquals(reduceMemoryFootprint, dest.isReduceMemoryFootprint());
        assertEquals(doOptimizeMessageStore, dest.isDoOptimzeMessageStorage());
        assertEquals(optimizeMessageStoreInFlightLimit, dest.getOptimizeMessageStoreInFlightLimit());
        assertEquals(advisoryForConsumed, dest.isAdvisoryForConsumed());
        assertEquals(advisoryForDelivery, dest.isAdvisoryForDelivery());
        assertEquals(advisoryForDiscardingMessages, dest.isAdvisoryForDiscardingMessages());
        assertEquals(advisoryForSlowConsumers, dest.isAdvisoryForSlowConsumers());
        assertEquals(advisoryForFastProducers, dest.isAdvisoryForFastProducers());
        assertEquals(advisoryWhenFull, dest.isAdvisoryWhenFull());
        assertEquals(includeBodyForAdvisory, dest.isIncludeBodyForAdvisory());
        assertEquals(sendAdvisoryIfNoConsumers, dest.isSendAdvisoryIfNoConsumers());

    }
}
