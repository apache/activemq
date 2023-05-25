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

public enum ReplicaEventType {
    DESTINATION_UPSERT,
    DESTINATION_DELETE,
    MESSAGE_SEND,
    MESSAGE_ACK,
    QUEUE_PURGED,
    TRANSACTION_BEGIN,
    TRANSACTION_PREPARE,
    TRANSACTION_ROLLBACK,
    TRANSACTION_COMMIT,
    TRANSACTION_FORGET,
    ADD_DURABLE_CONSUMER,
    REMOVE_DURABLE_CONSUMER,
    MESSAGE_EXPIRED,
    BATCH,
    REMOVE_DURABLE_CONSUMER_SUBSCRIPTION,
    FAIL_OVER,
    HEART_BEAT,
    ;

    public static final String EVENT_TYPE_PROPERTY = "ActiveMQReplicationEventType";
}
