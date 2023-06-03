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
package org.apache.activemq.replica;

import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.ByteSequence;

import java.util.HashMap;
import java.util.Map;

import static java.text.MessageFormat.format;
import static java.util.Objects.requireNonNull;

public class ReplicaEvent {

    private TransactionId transactionId;
    private ReplicaEventType eventType;
    private byte[] eventData;
    private Map<String, Object> replicationProperties = new HashMap<>();

    private Integer version;

    ReplicaEvent setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public ReplicaEvent setEventType(final ReplicaEventType eventType) {
        this.eventType = requireNonNull(eventType);
        return this;
    }

    public ReplicaEvent setEventData(final byte[] eventData) {
        this.eventData = requireNonNull(eventData);
        return this;
    }

    ReplicaEvent setReplicationProperty(String propertyKey, Object propertyValue) {
        requireNonNull(propertyKey);
        requireNonNull(propertyValue);
        if (replicationProperties.containsKey(propertyKey)) {
            throw new IllegalStateException(format("replication property ''{0}'' already has value ''{1}''", propertyKey, propertyValue));
        }
        replicationProperties.put(propertyKey, propertyValue);
        return this;
    }

    ReplicaEvent setVersion(int version) {
        this.version = version;
        return this;
    }

    TransactionId getTransactionId() {
        return transactionId;
    }

    public ByteSequence getEventData() {
        return new ByteSequence(eventData);
    }

    public ReplicaEventType getEventType() {
        return eventType;
    }

    public Map<String, Object> getReplicationProperties() {
        return replicationProperties;
    }

    public Integer getVersion() {
        return version;
    }
}
