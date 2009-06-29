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
package org.apache.activemq.broker.ft;

import java.net.URISyntaxException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTopicTransactionTest;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.test.JmsResourceProvider;

/**
 * Test failover for Topics
 */
public class TransactedTopicMasterSlaveTest extends JmsTopicTransactionTest {
    protected BrokerService slave;
    protected int inflightMessageCount;
    protected int failureCount = 50;
    protected String uriString = "failover://(tcp://localhost:62001?soWriteTimeout=15000,tcp://localhost:62002?soWriteTimeout=15000)?randomize=false";
    private boolean stopMaster = false;

    protected void setUp() throws Exception {
        failureCount = super.batchCount / 2;
        // this will create the main (or master broker)
        broker = createBroker();
        broker.start();
        slave = new BrokerService();
        slave.setBrokerName("slave");
        slave.setDeleteAllMessagesOnStartup(true);
        slave.setMasterConnectorURI("tcp://localhost:62001");
        slave.addConnector("tcp://localhost:62002");
        slave.start();
        // wait for thing to connect
        Thread.sleep(1000);
        resourceProvider = getJmsResourceProvider();
        topic = resourceProvider.isTopic();
        // We will be using transacted sessions.
        resourceProvider.setTransacted(true);
        connectionFactory = resourceProvider.createConnectionFactory();
        reconnect();
    }

    protected void tearDown() throws Exception {
        slave.stop();
        slave = null;
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception, URISyntaxException {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("master");
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("tcp://localhost:62001");
        return broker;
    }

    protected JmsResourceProvider getJmsResourceProvider() {
        JmsResourceProvider p = super.getJmsResourceProvider();
        p.setServerUri(uriString);
        return p;
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(uriString);
    }

    public void testSendReceiveTransactedBatchesWithMasterStop() throws Exception {
        try {
            stopMaster = true;
            testSendReceiveTransactedBatches();
        } finally {
            stopMaster = false;
        }
    }
    
    protected void messageSent() throws Exception {
        if (stopMaster) {
            if (++inflightMessageCount >= failureCount) {
                inflightMessageCount = 0;
                Thread.sleep(1000);
                broker.stop();
            }
        }
    }
}
