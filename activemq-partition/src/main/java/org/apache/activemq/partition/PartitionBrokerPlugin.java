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
import org.apache.activemq.partition.dto.Partitioning;

import java.io.IOException;

/**
 * A BrokerPlugin which partitions client connections over a cluster of brokers.
 *
 * @org.apache.xbean.XBean element="partitionBrokerPlugin"
 */
public class PartitionBrokerPlugin implements BrokerPlugin {

    protected int minTransferCount;
    protected Partitioning config;

    @Override
    public Broker installPlugin(Broker broker) throws Exception {
        return new PartitionBroker(broker, this);
    }

    public int getMinTransferCount() {
        return minTransferCount;
    }

    public void setMinTransferCount(int minTransferCount) {
        this.minTransferCount = minTransferCount;
    }

    public Partitioning getConfig() {
        return config;
    }

    public void setConfig(Partitioning config) {
        this.config = config;
    }

    public void setConfigAsJson(String config) throws IOException {
        this.config = Partitioning.MAPPER.readValue(config, Partitioning.class);
    }

    public String getBrokerURL(PartitionBroker partitionBroker, String id) {
        if( config!=null && config.brokers!=null ) {
            return config.brokers.get(id);
        }
        return null;
    }
}
