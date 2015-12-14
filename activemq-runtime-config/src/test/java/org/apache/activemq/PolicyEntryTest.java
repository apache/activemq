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
package org.apache.activemq;

import javax.jms.Session;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PolicyEntryTest extends RuntimeConfigTestSupport {

    String configurationSeed = "policyEntryTest";

    @Test
    public void testMod() throws Exception {
        final String brokerConfig = configurationSeed + "-policy-ml-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-policy-ml");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        verifyQueueLimit("Before", 1024);
        applyNewConfig(brokerConfig, configurationSeed + "-policy-ml-mod", SLEEP);
        verifyQueueLimit("After", 4194304);

        // change to existing dest
        verifyQueueLimit("Before", 4194304);
    }

    @Test
    public void testAddNdMod() throws Exception {
        final String brokerConfig = configurationSeed + "-policy-ml-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-policy-ml");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        verifyQueueLimit("Before", 1024);
        verifyTopicLimit("Before", brokerService.getSystemUsage().getMemoryUsage().getLimit());

        applyNewConfig(brokerConfig, configurationSeed + "-policy-ml-add", SLEEP);

        verifyTopicLimit("After", 2048l);
        verifyQueueLimit("After", 2048);

        // change to existing dest
        verifyTopicLimit("Before", 2048l);
    }

    @Test
    public void testModParentPolicy() throws Exception {
        final String brokerConfig = configurationSeed + "-policy-ml-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-policy-ml-parent");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        verifyQueueLimit("queue.test", 1024);
        verifyQueueLimit("queue.child.test", 2048);
        applyNewConfig(brokerConfig, configurationSeed + "-policy-ml-parent-mod", SLEEP);
        verifyQueueLimit("queue.test2", 4194304);

        // change to existing dest
        verifyQueueLimit("queue.test", 4194304);
        //verify no change
        verifyQueueLimit("queue.child.test", 2048);
    }

    @Test
    public void testModChildPolicy() throws Exception {
        final String brokerConfig = configurationSeed + "-policy-ml-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-policy-ml-parent");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        verifyQueueLimit("queue.test", 1024);
        verifyQueueLimit("queue.child.test", 2048);
        applyNewConfig(brokerConfig, configurationSeed + "-policy-ml-child-mod", SLEEP);
        //verify no change
        verifyQueueLimit("queue.test", 1024);

        // change to existing dest
        verifyQueueLimit("queue.child.test", 4194304);
        //new dest change
        verifyQueueLimit("queue.child.test2", 4194304);
    }

    private void verifyQueueLimit(String dest, int memoryLimit) throws Exception {
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection();
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
        ActiveMQConnection connection = new ActiveMQConnectionFactory("vm://localhost").createActiveMQConnection();
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
