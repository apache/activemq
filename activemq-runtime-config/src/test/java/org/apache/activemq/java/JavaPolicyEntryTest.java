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
