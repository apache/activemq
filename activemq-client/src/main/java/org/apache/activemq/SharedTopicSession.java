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
package org.apache.activemq;

import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQMessageTransformation;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SharedConsumerInfo;

/**
 * Session extension that implements JMS 3.1 shared topic subscriptions.
 *
 * <p>Overrides the four {@code createSharedConsumer} / {@code createSharedDurableConsumer}
 * methods (which upstream throws {@code UnsupportedOperationException} for) and
 * intercepts {@code syncSendPacket} to swap the {@link ConsumerInfo} command with
 * a {@link SharedConsumerInfo} before it reaches the broker.
 */
public class SharedTopicSession extends ActiveMQSession {

    private Boolean pendingSharedDurable;

    protected SharedTopicSession(ActiveMQConnection connection, SessionId sessionId,
            int acknowledgeMode, boolean asyncDispatch, boolean sessionAsyncDispatch)
            throws JMSException {
        super(connection, sessionId, acknowledgeMode, asyncDispatch, sessionAsyncDispatch);
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName)
            throws JMSException {
        return createSharedConsumer(topic, sharedSubscriptionName, null);
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName,
            String messageSelector) throws JMSException {
        checkClosed();
        validateSharedArgs(topic, sharedSubscriptionName);
        return createSharedMessageConsumer(topic, sharedSubscriptionName, messageSelector, false);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name)
            throws JMSException {
        return createSharedDurableConsumer(topic, name, null);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name,
            String messageSelector) throws JMSException {
        checkClosed();
        validateSharedArgs(topic, name);
        return createSharedMessageConsumer(topic, name, messageSelector, true);
    }

    private void validateSharedArgs(Topic topic, String subscriptionName)
            throws JMSException {
        if (topic == null) {
            throw new InvalidDestinationException("Topic cannot be null",
                    ActiveMQErrorCode.INVALID_DESTINATION);
        }
        if (subscriptionName == null || subscriptionName.isEmpty()) {
            throw new JMSException("Shared subscription name cannot be null or empty",
                    ActiveMQErrorCode.INVALID_SUBSCRIPTION_NAME);
        }
    }

    private MessageConsumer createSharedMessageConsumer(Topic topic, String name,
            String messageSelector, boolean durable) throws JMSException {
        ActiveMQPrefetchPolicy prefetchPolicy = connection.getPrefetchPolicy();
        int prefetch;
        if (durable) {
            prefetch = isAutoAcknowledge() && connection.isOptimizedMessageDispatch()
                    ? prefetchPolicy.getOptimizeDurableTopicPrefetch()
                    : prefetchPolicy.getDurableTopicPrefetch();
        } else {
            prefetch = prefetchPolicy.getTopicPrefetch();
        }
        int maxPendingLimit = prefetchPolicy.getMaximumPendingMessageLimit();
        ActiveMQDestination dest = ActiveMQMessageTransformation.transformDestination(topic);

        pendingSharedDurable = durable;
        try {
            return new ActiveMQMessageConsumer(this, getNextConsumerId(), dest, name,
                    messageSelector, prefetch, maxPendingLimit, false, false,
                    isAsyncDispatch(), null);
        } finally {
            pendingSharedDurable = null;
        }
    }

    @Override
    public Response syncSendPacket(Command command) throws JMSException {
        if (pendingSharedDurable != null && command instanceof ConsumerInfo
                && !(command instanceof SharedConsumerInfo)) {
            SharedConsumerInfo shared = toSharedConsumerInfo(
                    (ConsumerInfo) command, pendingSharedDurable);
            shared.setUserSpecifiedClientId(connection.isUserSpecifiedClientID());
            return super.syncSendPacket(shared);
        }
        return super.syncSendPacket(command);
    }

    static SharedConsumerInfo toSharedConsumerInfo(ConsumerInfo original, boolean durable) {
        SharedConsumerInfo shared = new SharedConsumerInfo();
        original.copy(shared);
        shared.setShared(true);
        shared.setDurable(durable);
        return shared;
    }
}
