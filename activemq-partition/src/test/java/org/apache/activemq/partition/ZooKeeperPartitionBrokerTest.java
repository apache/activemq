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
package org.apache.activemq.partition;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.leveldb.replicated.groups.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.After;
import org.junit.Before;
import org.linkedin.util.clock.Timespan;

import java.io.File;
import java.net.InetSocketAddress;

/**
 */
public class ZooKeeperPartitionBrokerTest extends PartitionBrokerTest {

    NIOServerCnxnFactory connector;

    @Before
    public void setUp() throws Exception {
        System.out.println("Starting ZooKeeper");
        ZooKeeperServer zk_server = new ZooKeeperServer();
        zk_server.setTickTime(500);
        zk_server.setTxnLogFactory(new FileTxnSnapLog(new File("target/test-data/zk-log"), new File("target/test-data/zk-data")));
        connector = new NIOServerCnxnFactory();
        connector.configure(new InetSocketAddress(0), 100);
        connector.startup(zk_server);
        System.out.println("ZooKeeper started");
        super.setUp();
    }


    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if( connector!=null ) {
          connector.shutdown();
          connector = null;
        }
    }

    String zkPath = "/partition-config";

    @Override
    protected void createBrokerCluster(int brokerCount) throws Exception {
        // Store the partitioning in ZK.
        ZKClient zk_client = new ZKClient("localhost:" + connector.getLocalPort(), Timespan.parse("10s"), null);
        try {
            zk_client.start();
            zk_client.waitForConnected(Timespan.parse("30s"));
            try {
                zk_client.delete(zkPath);
            } catch (Throwable e) {
            }
            zk_client.create(zkPath, partitioning.toString(), CreateMode.PERSISTENT);
        } finally {
            zk_client.close();
        }
        super.createBrokerCluster(brokerCount);
    }

    @Override
    protected void addPartitionBrokerPlugin(BrokerService broker) {
        // Have the borker plugin get the partition config via ZK.
        ZooKeeperPartitionBrokerPlugin plugin = new ZooKeeperPartitionBrokerPlugin(){
            @Override
            public String getBrokerURL(PartitionBroker partitionBroker, String id) {
                try {
                    return getConnectURL(id);
                } catch (Exception e) {
                    return null;
                }
            }
        };
        plugin.setZkAddress("localhost:" + connector.getLocalPort());
        plugin.setZkPath(zkPath);
        broker.setPlugins(new BrokerPlugin[]{plugin});
    }
}
