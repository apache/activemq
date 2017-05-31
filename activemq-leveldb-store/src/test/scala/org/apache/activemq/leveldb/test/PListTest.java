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
package org.apache.activemq.leveldb.test;


import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.leveldb.LevelDBStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.tools.ant.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.File;

public class PListTest {

    protected BrokerService brokerService;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.addConnector("tcp://localhost:0");

        LevelDBStore store = new LevelDBStore();
        store.setDirectory(new File("target/activemq-data/haleveldb"));
        store.deleteAllMessages();
        brokerService.setPersistenceAdapter(store);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setMemoryLimit(1);
        policyMap.setDefaultEntry(policy);
        brokerService.setDestinationPolicy(policyMap);

        brokerService.start();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null && brokerService.isStopped()) {
            brokerService.stop();
        }
        FileUtils.delete(new File("target/activemq-data/haleveldb"));
    }

    @Test
    public void testBrokerStop() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerService.getTransportConnectors().get(0).getServer().getConnectURI().toString());
        Connection conn = factory.createConnection();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = sess.createProducer(sess.createQueue("TEST"));
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        for (int i = 0; i < 10000; i++) {
            producer.send(sess.createTextMessage(i + " message"));
        }
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

}
