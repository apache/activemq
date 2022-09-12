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

import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaSchedulerSourceBroker extends ReplicaSourceBaseBroker {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaSchedulerSourceBroker.class);

    public ReplicaSchedulerSourceBroker(Broker next) {
        super(next);
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        ActiveMQDestination destination = messageSend.getDestination();
        final String jobId = (String) messageSend.getProperty(ScheduledMessage.AMQ_SCHEDULED_ID);
        if (jobId != null) {
            replicateSend(producerExchange.getConnectionContext(), messageSend, destination);
        }
        super.send(producerExchange, messageSend);
    }


    private void replicateSend(ConnectionContext connectionContext, Message message, ActiveMQDestination destination) {
        try {
            enqueueReplicaEvent(
                    connectionContext,
                    new ReplicaEvent()
                            .setEventType(ReplicaEventType.MESSAGE_SEND)
                            .setEventData(eventSerializer.serializeMessageData(message))
            );
        } catch (Exception e) {
            logger.error("Failed to replicate scheduled message {} for destination {}", message.getMessageId(), destination.getPhysicalName());
        }
    }

}
