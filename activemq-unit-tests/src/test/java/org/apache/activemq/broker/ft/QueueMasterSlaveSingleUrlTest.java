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

import java.io.File;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.leveldb.LevelDBStore;
import org.apache.activemq.util.Wait;
import org.junit.Ignore;


public class QueueMasterSlaveSingleUrlTest extends QueueMasterSlaveTestSupport {
    private final String brokerUrl = "tcp://localhost:62001";
    private final String singleUriString = "failover://(" + brokerUrl +")?randomize=false&useExponentialBackOff=false";

    @Override
    protected void setUp() throws Exception {
        setAutoFail(true);
        super.setUp();
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(singleUriString);
    }

    @Override
    protected void createMaster() throws Exception {
        master = new BrokerService();
        master.setBrokerName("shared-master");
        configureSharedPersistenceAdapter(master);
        master.addConnector(brokerUrl);
        master.start();
    }

    private void configureSharedPersistenceAdapter(BrokerService broker) throws Exception {
       LevelDBStore adapter = new LevelDBStore();
       adapter.setDirectory(new File("shared"));
       broker.setPersistenceAdapter(adapter);
    }

    @Override
    protected void createSlave() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    BrokerService broker = new BrokerService();
                    broker.setBrokerName("shared-slave");
                    configureSharedPersistenceAdapter(broker);
                    // add transport as a service so that it is bound on start, after store started
                    final TransportConnector tConnector = new TransportConnector();
                    tConnector.setUri(new URI(brokerUrl));
                    broker.addConnector(tConnector);

                    broker.start();
                    slave.set(broker);
                    slaveStarted.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }).start();
    }

    public void testNetworkMasterSlave() throws Exception {

        final BrokerService client = new BrokerService();
        client.setBrokerName("client");
        client.setPersistent(false);
        client.getManagementContext().setCreateConnector(false);
        client.addNetworkConnector("masterslave:(tcp://localhost:62001,tcp://localhost:62002)");
        client.start();
        try {
            Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return client.getRegionBroker().getPeerBrokerInfos().length == 1;
                }
            });

            assertTrue(!master.isSlave());
            master.stop();
            assertTrue("slave started", slaveStarted.await(60, TimeUnit.SECONDS));
            assertTrue(!slave.get().isSlave());

            Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return client.getRegionBroker().getPeerBrokerInfos().length == 1;
                }
            });
        } finally {
            client.stop();
        }

    }
}
