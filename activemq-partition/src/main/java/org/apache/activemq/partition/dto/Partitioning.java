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
package org.apache.activemq.partition.dto;

import java.util.Map;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;
import javax.json.bind.annotation.JsonbProperty;

/**
 * The main Configuration class for the PartitionBroker plugin
 */
public class Partitioning {

    static final public Jsonb MAPPER = JsonbBuilder.create();
    static final public Jsonb TO_STRING_MAPPER = JsonbBuilder.create(new JsonbConfig()
            .setProperty("johnzon.cdi.activated", false).withFormatting(true));

    /**
     * If a client connects with a clientId which is listed in the
     * map, then he will be immediately reconnected
     * to the partition target immediately.
     */
    @JsonbProperty("by_client_id")
    public Map<String, Target> byClientId;

    /**
     * If a client connects with a user priciple which is listed in the
     * map, then he will be immediately reconnected
     * to the partition target immediately.
     */
    @JsonbProperty("by_user_name")
    public Map<String, Target> byUserName;

    /**
     * If a client connects with source ip which is listed in the
     * map, then he will be immediately reconnected
     * to the partition target immediately.
     */
    @JsonbProperty("by_source_ip")
    public Map<String, Target> bySourceIp;

    /**
     * Used to map the preferred partitioning of queues across
     * a set of brokers.  Once a it is deemed that a connection mostly
     * works with a set of targets configured in this map, the client
     * will be reconnected to the appropriate target.
     */
    @JsonbProperty("by_queue")
    public Map<String, Target> byQueue;

    /**
     * Used to map the preferred partitioning of topics across
     * a set of brokers.  Once a it is deemed that a connection mostly
     * works with a set of targets configured in this map, the client
     * will be reconnected to the appropriate target.
     */
    @JsonbProperty("by_topic")
    public Map<String, Target> byTopic;

    /**
     * Maps broker names to broker URLs.
     */
    @JsonbProperty("brokers")
    public Map<String, String> brokers;


    @Override
    public String toString() {
        return TO_STRING_MAPPER.toJson(this);
    }

    public Map<String, String> getBrokers() {
        return brokers;
    }

    public void setBrokers(Map<String, String> brokers) {
        this.brokers = brokers;
    }

    public Map<String, Target> getByClientId() {
        return byClientId;
    }

    public void setByClientId(Map<String, Target> byClientId) {
        this.byClientId = byClientId;
    }

    public Map<String, Target> getByQueue() {
        return byQueue;
    }

    public void setByQueue(Map<String, Target> byQueue) {
        this.byQueue = byQueue;
    }

    public Map<String, Target> getBySourceIp() {
        return bySourceIp;
    }

    public void setBySourceIp(Map<String, Target> bySourceIp) {
        this.bySourceIp = bySourceIp;
    }

    public Map<String, Target> getByTopic() {
        return byTopic;
    }

    public void setByTopic(Map<String, Target> byTopic) {
        this.byTopic = byTopic;
    }

    public Map<String, Target> getByUserName() {
        return byUserName;
    }

    public void setByUserName(Map<String, Target> byUserName) {
        this.byUserName = byUserName;
    }
}
