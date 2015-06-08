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

import static org.junit.Assert.assertTrue;

import java.io.File;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * This unit test is to test that setting the property "maxDestinations" on
 * PolicyEntry works correctly. If this property is set, it will limit the
 * number of destinations that can be created. Advisory topics will be ignored
 * during calculations.
 *
 */
public class MaxDestinationsPolicyTest {
    BrokerService broker;
    ConnectionFactory factory;
    Connection connection;
    Session session;
    MessageProducer producer;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();

        File testDataDir = new File("target/activemq-data/AMQ-5751");
        broker.setDataDirectoryFile(testDataDir);
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(1024l * 1024 * 64);
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(new File(testDataDir, "kahadb"));
        broker.setPersistenceAdapter(persistenceAdapter);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        factory = new ActiveMQConnectionFactory(broker.getTransportConnectors()
                .get(0).getConnectUri().toString());
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception {
        session.close();
        connection.stop();
        connection.close();
        broker.stop();
    }

    /**
     * Test that 10 queues can be created when default policy allows it.
     */
    @Test
    public void testMaxDestinationDefaultPolicySuccess() throws Exception {
        applyDefaultMaximumDestinationPolicy(10);

        for (int i = 0; i < 10; i++) {
            createQueue("queue." + i);
        }
    }

    /**
     * Test that default policy prevents going beyond max
     */
    @Test(expected = javax.jms.IllegalStateException.class)
    public void testMaxDestinationDefaultPolicyFail() throws Exception {
        applyDefaultMaximumDestinationPolicy(10);

        for (int i = 0; i < 11; i++) {
            createQueue("queue." + i);
        }
    }

    /**
     * Test that a queue policy overrides the default policy
     */
    @Test(expected = javax.jms.IllegalStateException.class)
    public void testMaxDestinationOnQueuePolicy() throws Exception {
        PolicyMap policyMap = applyDefaultMaximumDestinationPolicy(10);
        applyMaximumDestinationPolicy(policyMap, new ActiveMQQueue("queue.>"),
                5);

        // This should fail even though the default policy is set to a limit of
        // 10 because the
        // queue policy overrides it
        for (int i = 0; i < 6; i++) {
            createQueue("queue." + i);
        }
    }

    /**
     * Test that 10 topics can be created when default policy allows it.
     */
    @Test
    public void testTopicMaxDestinationDefaultPolicySuccess() throws Exception {
        applyDefaultMaximumDestinationPolicy(10);

        for (int i = 0; i < 10; i++) {
            createTopic("topic." + i);
        }
    }

    /**
     * Test that topic creation will faill when exceeding the limit
     */
    @Test(expected = javax.jms.IllegalStateException.class)
    public void testTopicMaxDestinationDefaultPolicyFail() throws Exception {
        applyDefaultMaximumDestinationPolicy(20);

        for (int i = 0; i < 21; i++) {
            createTopic("topic." + i);
        }
    }

    /**
     * Test that no limit is enforced
     */
    @Test
    public void testTopicDefaultPolicyNoMaxDestinations() throws Exception {
        // -1 is the default and signals no max destinations
        applyDefaultMaximumDestinationPolicy(-1);
        for (int i = 0; i < 100; i++) {
            createTopic("topic." + i);
        }
    }

    /**
     * Test a mixture of queue and topic policies
     */
    @Test
    public void testComplexMaxDestinationPolicy() throws Exception {
        PolicyMap policyMap = applyMaximumDestinationPolicy(new PolicyMap(),
                new ActiveMQQueue("queue.>"), 5);
        applyMaximumDestinationPolicy(policyMap, new ActiveMQTopic("topic.>"),
                10);

        for (int i = 0; i < 5; i++) {
            createQueue("queue." + i);
        }

        for (int i = 0; i < 10; i++) {
            createTopic("topic." + i);
        }

        // Make sure that adding one more of either a topic or a queue fails
        boolean fail = false;
        try {
            createTopic("topic.test");
        } catch (javax.jms.IllegalStateException e) {
            fail = true;
        }
        assertTrue(fail);

        fail = false;
        try {
            createQueue("queue.test");
        } catch (javax.jms.IllegalStateException e) {
            fail = true;
        }
        assertTrue(fail);
    }

    /**
     * Test child destinations of a policy
     */
    @Test
    public void testMaxDestinationPolicyOnChildDests() throws Exception {
        applyMaximumDestinationPolicy(new PolicyMap(), new ActiveMQTopic(
                "topic.>"), 10);

        for (int i = 0; i < 10; i++) {
            createTopic("topic.test" + i);
        }

        // Make sure that adding one more fails
        boolean fail = false;
        try {
            createTopic("topic.abc.test");
        } catch (javax.jms.IllegalStateException e) {
            fail = true;
        }
        assertTrue(fail);

    }

    /**
     * Test a topic policy overrides the default
     */
    @Test(expected = javax.jms.IllegalStateException.class)
    public void testMaxDestinationOnTopicPolicy() throws Exception {
        PolicyMap policyMap = applyDefaultMaximumDestinationPolicy(10);
        applyMaximumDestinationPolicy(policyMap, new ActiveMQTopic("topic.>"),
                5);

        // This should fail even though the default policy is set to a limit of
        // 10 because the
        // queue policy overrides it
        for (int i = 0; i < 6; i++) {
            createTopic("topic." + i);
        }
    }

    private PolicyMap applyMaximumDestinationPolicy(PolicyMap policyMap,
            ActiveMQDestination destination, int maxDestinations) {
        PolicyEntry entry = new PolicyEntry();
        entry.setDestination(destination);
        entry.setMaxDestinations(maxDestinations);
        policyMap.setPolicyEntries(Lists.newArrayList(entry));
        broker.setDestinationPolicy(policyMap);
        return policyMap;
    }

    private PolicyMap applyDefaultMaximumDestinationPolicy(int maxDestinations) {
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        if (maxDestinations >= 0) {
            defaultEntry.setMaxDestinations(maxDestinations);
        }
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
        return policyMap;
    }

    private void createQueue(String queueName) throws Exception {
        Queue queue = session.createQueue(queueName);
        producer = session.createProducer(queue);
    }

    private void createTopic(String topicName) throws Exception {
        Topic topic = session.createTopic(topicName);
        producer = session.createProducer(topic);
    }

}
