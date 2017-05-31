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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.FilePendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class AMQ2616Test {

    @Rule
    public TestName test = new TestName();

    private static final int NUMBER = 2000;
    private BrokerService brokerService;
    private final String ACTIVEMQ_BROKER_BIND = "tcp://0.0.0.0:0";

    private String connectionUri;

    @Test(timeout = 90000)
    public void testQueueResourcesReleased() throws Exception {
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(connectionUri);
        Connection tempConnection = fac.createConnection();
        tempConnection.start();
        Session tempSession = tempConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue tempQueue = tempSession.createTemporaryQueue();

        Connection testConnection = fac.createConnection();
        final long startUsage = brokerService.getSystemUsage().getMemoryUsage().getUsage();
        Session testSession = testConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer testProducer = testSession.createProducer(tempQueue);
        byte[] payload = new byte[1024 * 4];

        for (int i = 0; i < NUMBER; i++) {
            BytesMessage msg = testSession.createBytesMessage();
            msg.writeBytes(payload);
            testProducer.send(msg);
        }

        long endUsage = brokerService.getSystemUsage().getMemoryUsage().getUsage();
        assertFalse(startUsage == endUsage);
        tempConnection.close();

        assertTrue("Usage should return to original", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return brokerService.getSystemUsage().getMemoryUsage().getUsage() == startUsage;
            }
        }));
    }

    @Before
    public void setUp() throws Exception {
        // Start an embedded broker up.
        brokerService = new BrokerService();

        KahaDBPersistenceAdapter adaptor = new KahaDBPersistenceAdapter();
        adaptor.setEnableJournalDiskSyncs(false);
        File file = new File("target/AMQ2616Test");
        IOHelper.mkdirs(file);
        IOHelper.deleteChildren(file);
        adaptor.setDirectory(file);
        brokerService.setPersistenceAdapter(adaptor);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry pe = new PolicyEntry();
        pe.setMemoryLimit(10 * 1024 * 1024);
        pe.setOptimizedDispatch(true);
        pe.setProducerFlowControl(false);
        pe.setExpireMessagesPeriod(1000);
        pe.setPendingQueuePolicy(new FilePendingQueueMessageStoragePolicy());
        policyMap.put(new ActiveMQQueue(">"), pe);

        brokerService.setDestinationPolicy(policyMap);
        brokerService.getSystemUsage().getMemoryUsage().setLimit(20 * 1024 * 1024);
        brokerService.getSystemUsage().getTempUsage().setLimit(200 * 1024 * 1024);
        brokerService.addConnector(ACTIVEMQ_BROKER_BIND);
        brokerService.start();
        brokerService.waitUntilStarted();

        connectionUri = brokerService.getTransportConnectors().get(0).getPublishableConnectString();

        new ActiveMQQueue(test.getMethodName());
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService = null;
    }
}
