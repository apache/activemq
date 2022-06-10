package org.apache.activemq.replica;

public enum ReplicaEventType {
    DESTINATION_UPSERT,
    DESTINATION_DELETE,
    MESSAGE_SEND;

    static final String EVENT_TYPE_PROPERTY = "ActiveMQ.Replication.EventType";
}
