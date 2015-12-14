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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RuntimeConfigTestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
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

    private void verifyQueueLimit(String dest, int memoryLimit) throws Exception {
        ActiveMQConnection connection = (ActiveMQConnection) new ActiveMQConnectionFactory("vm://localhost").createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue(dest));

            assertEquals(memoryLimit, brokerService.getRegionBroker().getDestinationMap().get(new ActiveMQQueue(dest)).getMemoryUsage().getLimit());
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

            assertEquals(memoryLimit, brokerService.getRegionBroker().getDestinationMap().get(new ActiveMQTopic(dest)).getMemoryUsage().getLimit());
        } finally {
            connection.close();
        }
    }
}
