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

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class ReplicationMessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationMessageProducer.class);

    private final IdGenerator idGenerator = new IdGenerator();
    private final ProducerId replicationProducerId = new ProducerId();
    private final ReplicaInternalMessageProducer replicaInternalMessageProducer;
    private final ReplicaReplicationQueueSupplier queueProvider;
    private final ReplicaEventSerializer eventSerializer = new ReplicaEventSerializer();
    private final LongSequenceGenerator eventMessageIdGenerator = new LongSequenceGenerator();

    ReplicationMessageProducer(ReplicaInternalMessageProducer replicaInternalMessageProducer,
            ReplicaReplicationQueueSupplier queueProvider) {
        this.replicaInternalMessageProducer = replicaInternalMessageProducer;
        this.queueProvider = queueProvider;
        replicationProducerId.setConnectionId(idGenerator.generateId());
    }

    void enqueueIntermediateReplicaEvent(ConnectionContext connectionContext, ReplicaEvent event) throws Exception {
        synchronized (ReplicaSupport.INTERMEDIATE_QUEUE_MUTEX) {
            logger.debug("Replicating {} event", event.getEventType());
            logger.trace("Replicating {} event: data:\n{}\nproperties:{}", event.getEventType(), new Object() {
                @Override
                public String toString() {
                    try {
                        return eventSerializer.deserializeMessageData(event.getEventData()).toString();
                    } catch (IOException e) {
                        return "<some event data>";
                    }
                }
            }, event.getReplicationProperties()); // FIXME: remove
            enqueueReplicaEvent(connectionContext, event, true, queueProvider.getIntermediateQueue());
        }
    }

    void enqueueMainReplicaEvent(ConnectionContext connectionContext, ReplicaEvent event) throws Exception {
        enqueueReplicaEvent(connectionContext, event, false, queueProvider.getMainQueue());
    }

    private void enqueueReplicaEvent(ConnectionContext connectionContext, ReplicaEvent event,
            boolean persistent, ActiveMQQueue mainQueue) throws Exception {
        ActiveMQMessage eventMessage = new ActiveMQMessage();
        eventMessage.setPersistent(persistent);
        eventMessage.setType("ReplicaEvent");
        eventMessage.setStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY, event.getEventType().name());
        eventMessage.setMessageId(new MessageId(replicationProducerId, eventMessageIdGenerator.getNextSequenceId()));
        eventMessage.setDestination(mainQueue);
        eventMessage.setProducerId(replicationProducerId);
        eventMessage.setResponseRequired(false);
        eventMessage.setContent(event.getEventData());
        eventMessage.setProperties(event.getReplicationProperties());
        eventMessage.setTransactionId(event.getTransactionId());
        replicaInternalMessageProducer.sendIgnoringFlowControl(connectionContext, eventMessage);
    }
}

