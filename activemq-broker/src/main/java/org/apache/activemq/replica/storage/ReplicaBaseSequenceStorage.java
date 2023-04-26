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
package org.apache.activemq.replica.storage;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerStoppedException;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.replica.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public abstract class ReplicaBaseSequenceStorage extends ReplicaBaseStorage {

    private final Logger logger = LoggerFactory.getLogger(ReplicaBaseSequenceStorage.class);

    static final String SEQUENCE_NAME_PROPERTY = "SequenceName";
    private final String sequenceName;

    public ReplicaBaseSequenceStorage(Broker broker, ReplicaReplicationQueueSupplier queueProvider,
            ReplicaInternalMessageProducer replicaInternalMessageProducer, String sequenceName) {
        super(broker, replicaInternalMessageProducer, queueProvider.getSequenceQueue(),
                "ReplicationPlugin.ReplicaSequenceStorage",
                String.format("%s LIKE '%s'", SEQUENCE_NAME_PROPERTY, sequenceName));
        this.sequenceName = requireNonNull(sequenceName);
    }

    public void deinitialize(ConnectionContext connectionContext) throws Exception {
        queue = null;

        if (subscription != null) {
            try {
                broker.removeConsumer(connectionContext, subscription.getConsumerInfo());
            } catch (BrokerStoppedException ignored) {}
            subscription = null;
        }
    }

    @Override
    public void send(ConnectionContext connectionContext, ActiveMQTextMessage seqMessage) throws Exception {
        seqMessage.setStringProperty(SEQUENCE_NAME_PROPERTY, sequenceName);
        super.send(connectionContext, seqMessage);
    }
}
