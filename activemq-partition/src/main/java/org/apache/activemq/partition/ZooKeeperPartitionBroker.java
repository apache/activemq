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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.leveldb.replicated.groups.ZKClient;
import org.apache.activemq.partition.dto.Partitioning;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.linkedin.util.clock.Timespan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 */
public class ZooKeeperPartitionBroker extends PartitionBroker {

    protected static final Logger LOG = LoggerFactory.getLogger(ZooKeeperPartitionBroker.class);

    protected volatile ZKClient zk_client = null;
    protected volatile Partitioning config;
    protected final CountDownLatch configAcquired = new CountDownLatch(1);

    public ZooKeeperPartitionBroker(Broker broker, ZooKeeperPartitionBrokerPlugin plugin) {
        super(broker, plugin);
    }

    @Override
    public void start() throws Exception {
        super.start();
        // Lets block a bit until we get our config.. Otherwise just keep
        // on going.. not a big deal if we get our config later.  Perhaps
        // ZK service is not having a good day.
        configAcquired.await(5, TimeUnit.SECONDS);
    }

    @Override
    protected void onMonitorStop() {
        zkDisconnect();
    }

    @Override
    protected Partitioning getConfig() {
        return config;
    }

    protected ZooKeeperPartitionBrokerPlugin plugin() {
        return (ZooKeeperPartitionBrokerPlugin)plugin;
    }

    protected void zkConnect() throws Exception {
        zk_client = new ZKClient(plugin().getZkAddress(), Timespan.parse(plugin().getZkSessionTmeout()), null);
        if( plugin().getZkPassword()!=null ) {
            zk_client.setPassword(plugin().getZkPassword());
        }
        zk_client.start();
        zk_client.waitForConnected(Timespan.parse("30s"));
    }

    protected void zkDisconnect() {
        if( zk_client!=null ) {
            zk_client.close();
            zk_client = null;
        }
    }

    protected void reloadConfiguration() throws Exception {
        if( zk_client==null )  {
            LOG.debug("Connecting to ZooKeeper");
            try {
                zkConnect();
                LOG.debug("Connected to ZooKeeper");
            } catch (Exception e) {
                LOG.debug("Connection to ZooKeeper failed: "+e);
                zkDisconnect();
                throw e;
            }
        }

        byte[] data = null;
        try {
            Stat stat = new Stat();
            data = zk_client.getData(plugin().getZkPath(), new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    try {
                        reloadConfiguration();
                    } catch (Exception e) {
                    }
                    monitorWakeup();
                }
            }, stat);
            configAcquired.countDown();
            reloadConfigOnPoll = false;
        } catch (Exception e) {
            LOG.warn("Could load partitioning configuration: " + e, e);
            reloadConfigOnPoll = true;
        }

        try {
            config = Partitioning.MAPPER.readValue(data, Partitioning.class);
        } catch (Exception e) {
            LOG.warn("Invalid partitioning configuration: " + e, e);
        }
    }

}
