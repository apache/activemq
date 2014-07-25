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



import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.HashMap;

/**
 * The main Configuration class for the PartitionBroker plugin
 */
public class Partitioning {

    static final public ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    static final public ObjectMapper TO_STRING_MAPPER = new ObjectMapper();
    static {
        TO_STRING_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        TO_STRING_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * If a client connects with a clientId which is listed in the
     * map, then he will be immediately reconnected
     * to the partition target immediately.
     */
    @JsonProperty("by_client_id")
    @JsonDeserialize(contentAs = Target.class)
    public HashMap<String, Target> byClientId;

    /**
     * If a client connects with a user priciple which is listed in the
     * map, then he will be immediately reconnected
     * to the partition target immediately.
     */
    @JsonProperty("by_user_name")
    @JsonDeserialize(contentAs = Target.class)
    public HashMap<String, Target> byUserName;

    /**
     * If a client connects with source ip which is listed in the
     * map, then he will be immediately reconnected
     * to the partition target immediately.
     */
    @JsonProperty("by_source_ip")
    @JsonDeserialize(contentAs = Target.class)
    public HashMap<String, Target> bySourceIp;

    /**
     * Used to map the preferred partitioning of queues across
     * a set of brokers.  Once a it is deemed that a connection mostly
     * works with a set of targets configured in this map, the client
     * will be reconnected to the appropriate target.
     */
    @JsonProperty("by_queue")
    @JsonDeserialize(contentAs = Target.class)
    public HashMap<String, Target> byQueue;

    /**
     * Used to map the preferred partitioning of topics across
     * a set of brokers.  Once a it is deemed that a connection mostly
     * works with a set of targets configured in this map, the client
     * will be reconnected to the appropriate target.
     */
    @JsonProperty("by_topic")
    @JsonDeserialize(contentAs = Target.class)
    public HashMap<String, Target> byTopic;

    /**
     * Maps broker names to broker URLs.
     */
    @JsonProperty("brokers")
    @JsonDeserialize(contentAs = String.class)
    public HashMap<String, String> brokers;


    @Override
    public String toString() {
        try {
            return TO_STRING_MAPPER.writeValueAsString(this);
        } catch (IOException e) {
            return super.toString();
        }
    }

    public HashMap<String, String> getBrokers() {
        return brokers;
    }

    public void setBrokers(HashMap<String, String> brokers) {
        this.brokers = brokers;
    }

    public HashMap<String, Target> getByClientId() {
        return byClientId;
    }

    public void setByClientId(HashMap<String, Target> byClientId) {
        this.byClientId = byClientId;
    }

    public HashMap<String, Target> getByQueue() {
        return byQueue;
    }

    public void setByQueue(HashMap<String, Target> byQueue) {
        this.byQueue = byQueue;
    }

    public HashMap<String, Target> getBySourceIp() {
        return bySourceIp;
    }

    public void setBySourceIp(HashMap<String, Target> bySourceIp) {
        this.bySourceIp = bySourceIp;
    }

    public HashMap<String, Target> getByTopic() {
        return byTopic;
    }

    public void setByTopic(HashMap<String, Target> byTopic) {
        this.byTopic = byTopic;
    }

    public HashMap<String, Target> getByUserName() {
        return byUserName;
    }

    public void setByUserName(HashMap<String, Target> byUserName) {
        this.byUserName = byUserName;
    }
}
