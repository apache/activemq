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

    ReplicaEvent setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    ReplicaEvent setEventType(final ReplicaEventType eventType) {
        this.eventType = requireNonNull(eventType);
        return this;
    }

    ReplicaEvent setEventData(final byte[] eventData) {
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

    TransactionId getTransactionId() {
        return transactionId;
    }

    ByteSequence getEventData() {
        return new ByteSequence(eventData);
    }

    ReplicaEventType getEventType() {
        return eventType;
    }

    public Map<String, Object> getReplicationProperties() {
        return replicationProperties;
    }
}
