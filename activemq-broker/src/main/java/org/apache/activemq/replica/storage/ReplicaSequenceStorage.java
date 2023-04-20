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
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.replica.ReplicaInternalMessageProducer;
import org.apache.activemq.replica.ReplicaReplicationQueueSupplier;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ReplicaSequenceStorage extends ReplicaBaseSequenceStorage {

    private final Logger logger = LoggerFactory.getLogger(ReplicaSequenceStorage.class);
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();

    public ReplicaSequenceStorage(Broker broker, ReplicaReplicationQueueSupplier queueProvider,
            ReplicaInternalMessageProducer replicaInternalMessageProducer, String sequenceName) {
        super(broker, queueProvider, replicaInternalMessageProducer, sequenceName);
    }

    public String initialize(ConnectionContext connectionContext) throws Exception {
        List<ActiveMQTextMessage> allMessages = super.initializeBase(connectionContext);

        if (allMessages.size() == 0) {
            return null;
        }

        if (allMessages.size() > 1) {
            for (int i = 0; i < allMessages.size() - 1; i++) {
                queue.removeMessage(allMessages.get(i).getMessageId().toString());
            }
        }

        return allMessages.get(0).getText();
    }

    public void enqueue(ConnectionContext connectionContext, TransactionId tid, String message) throws Exception {
        // before enqueue message, we acknowledge all messages currently in queue.
        acknowledgeAll(connectionContext, tid);

        send(connectionContext, tid, message,
                new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
    }
}
