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
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;

import java.util.ArrayList;
import java.util.List;

public class ReplicaRecoverySequenceStorage extends ReplicaBaseSequenceStorage {

    public ReplicaRecoverySequenceStorage(Broker broker, ConnectionContext connectionContext,
            ReplicaReplicationQueueSupplier queueProvider, ReplicaInternalMessageProducer replicaInternalMessageProducer,
            String sequenceName) {
        super(broker, connectionContext, queueProvider, replicaInternalMessageProducer, sequenceName);
    }

    public List<String> initialize() throws Exception {
        List<String> result = new ArrayList<>();
        for (ActiveMQTextMessage message : super.initializeBase()) {
            result.add(message.getText());
        }
        return result;
    }

    public void acknowledge(ConnectionContext connectionContext, TransactionId tid, List<String> messageIds) throws Exception {
        for (String messageId : messageIds) {
            MessageAck ack = new MessageAck();
            ack.setMessageID(new MessageId(messageId));
            ack.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);
            ack.setDestination(queueProvider.getSequenceQueue());
            ack.setTransactionId(tid);
            acknowledge(connectionContext, ack);
        }
    }
}
