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
package org.apache.activemq.usecases;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

import java.io.File;
import java.net.URI;

/**
 * @author Carlo Dapor
 */
public class TwoBrokersStopSlaveTest extends TestCase {
    private final static File KahaDbDirectory = new File("target/TwoBrokersStopSlaveTest");

    public void testStartMasterAndSlaveShutdownSlaveFirst() throws Exception {
        BrokerService masterBroker = createBroker("masterBroker", 9100);
        BrokerService slaveBroker = createBroker("slaveBroker", 9101);

        Thread.sleep(1_000L);

        assertTrue(masterBroker.isPersistent());
        assertTrue(slaveBroker.isPersistent());
        assertFalse(masterBroker.isSlave());
        assertTrue(slaveBroker.isSlave());

        // stop slave broker
        slaveBroker.stop();
        slaveBroker.waitUntilStopped();

        masterBroker.stop();
    }

    protected BrokerService createBroker(final String brokerName, final int port) throws Exception {
        String connectorUrl = "tcp://localhost:" + port;

        BrokerService broker = BrokerFactory.createBroker(new URI("broker://()/localhost"));
        broker.setBrokerName(brokerName);
        broker.addConnector(connectorUrl);
        broker.setUseShutdownHook(false);
        broker.setPersistent(true);

        PersistenceAdapter adapter = new KahaDBPersistenceAdapter();
        // brokers must acquire exclusive lock KahaDB
        adapter.setDirectory(KahaDbDirectory);
        broker.setPersistenceAdapter(adapter);
        broker.setStartAsync(true);
        broker.start();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectorUrl);
        factory.createConnection();

        // If the slave broker also is waiting to fully start up, it achieves that by acquiring the exclusive lock.
        // But then, it can never be shut down before becoming master.
        // This behaviour is filed as issue 6601, cf. https://issues.apache.org/jira/browse/AMQ-6601
        if (!broker.isSlave())
            broker.waitUntilStarted();

        return broker;
    }

}
