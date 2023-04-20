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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ReplicaAckHelper {

    private final Broker broker;

    public ReplicaAckHelper(Broker broker) {
        this.broker = broker;
    }

    public List<MessageReference> getMessagesToAck(MessageAck ack, Destination destination) {
        PrefetchSubscription prefetchSubscription = getPrefetchSubscription(destination, ack.getConsumerId());
        if (prefetchSubscription == null) {
            return null;
        }

        return getMessagesToAck(ack, prefetchSubscription);
    }

    public List<MessageReference> getMessagesToAck(MessageAck ack, PrefetchSubscription subscription) {
        List<MessageReference> dispatched = subscription.getDispatched();
        if (ack.isStandardAck() || ack.isExpiredAck() || ack.isPoisonAck()) {
            boolean inAckRange = false;
            List<MessageReference> removeList = new ArrayList<>();
            for (final MessageReference node : dispatched) {
                MessageId messageId = node.getMessageId();
                if (ack.getFirstMessageId() == null || ack.getFirstMessageId().equals(messageId)) {
                    inAckRange = true;
                }
                if (inAckRange) {
                    removeList.add(node);
                    if (ack.getLastMessageId().equals(messageId)) {
                        break;
                    }
                }
            }

            return removeList;
        }

        if (ack.isIndividualAck()) {
            return dispatched.stream()
                .filter(mr -> mr.getMessageId().equals(ack.getLastMessageId()))
                .collect(Collectors.toList());
        }

        return null;
    }

    private PrefetchSubscription getPrefetchSubscription(Destination destination, ConsumerId consumerId) {
        return destination.getConsumers().stream()
            .filter(c -> c.getConsumerInfo().getConsumerId().equals(consumerId))
            .findFirst().filter(PrefetchSubscription.class::isInstance).map(PrefetchSubscription.class::cast)
            .orElse(null);
    }
}