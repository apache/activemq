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
import org.apache.activemq.broker.BrokerPlugin;

/**
 * A PartitionBrokerPlugin which gets it's configuration from ZooKeeper.
 */
public class ZooKeeperPartitionBrokerPlugin extends PartitionBrokerPlugin {

    String zkAddress = "127.0.0.1:2181";
    String zkPassword;
    String zkPath = "/broker-assignments";
    String zkSessionTmeout = "10s";

    @Override
    public Broker installPlugin(Broker broker) throws Exception {
        return new ZooKeeperPartitionBroker(broker, this);
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public String getZkPassword() {
        return zkPassword;
    }

    public void setZkPassword(String zkPassword) {
        this.zkPassword = zkPassword;
    }

    public String getZkPath() {
        return zkPath;
    }

    public void setZkPath(String zkPath) {
        this.zkPath = zkPath;
    }

    public String getZkSessionTmeout() {
        return zkSessionTmeout;
    }

    public void setZkSessionTmeout(String zkSessionTmeout) {
        this.zkSessionTmeout = zkSessionTmeout;
    }
}
