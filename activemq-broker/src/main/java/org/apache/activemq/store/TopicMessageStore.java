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
package org.apache.activemq.store;

import java.io.IOException;

import javax.jms.JMSException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;

/**
 * A MessageStore for durable topic subscriptions
 */
public interface TopicMessageStore extends MessageStore {

    /**
     * Stores the last acknowledged messgeID for the given subscription so that
     * we can recover and commence dispatching messages from the last checkpoint
     *
     * @param context
     * @param clientId
     * @param subscriptionName
     * @param messageId
     * @param ack
     *
     * @throws IOException
     */
    void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId, MessageAck ack) throws IOException;

    /**
     * @param clientId
     * @param subscriptionName
     *
     * @throws IOException
     * @throws JMSException
     */
    void deleteSubscription(String clientId, String subscriptionName) throws IOException;

    /**
     * For the new subscription find the last acknowledged message ID and then
     * find any new messages since then and dispatch them to the subscription.
     * <p/> e.g. if we dispatched some messages to a new durable topic
     * subscriber, then went down before acknowledging any messages, we need to
     * know the correct point from which to recover from.
     *
     * @param clientId
     * @param subscriptionName
     * @param listener
     *
     * @throws Exception
     */
    void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener) throws Exception;

    /**
     * For an active subscription - retrieve messages from the store for the
     * subscriber after the lastMessageId messageId <p/>
     *
     * @param clientId
     * @param subscriptionName
     * @param maxReturned
     * @param listener
     *
     * @throws Exception
     */
    void recoverNextMessages(String clientId, String subscriptionName, int maxReturned, MessageRecoveryListener listener) throws Exception;

    /**
     * A hint to the Store to reset any batching state for a durable subscriber
     *
     * @param clientId
     * @param subscriptionName
     */
    void resetBatching(String clientId, String subscriptionName);

    /**
     * Get the number of messages ready to deliver from the store to a durable
     * subscriber
     *
     * @param clientId
     * @param subscriberName
     *
     * @return the outstanding message count
     *
     * @throws IOException
     */
    int getMessageCount(String clientId, String subscriberName) throws IOException;

    /**
     * Get the total size of the messages ready to deliver from the store to the
     * durable subscriber
     *
     * @param clientId
     * @param subscriberName
     * @return
     * @throws IOException
     */
    long getMessageSize(String clientId, String subscriberName) throws IOException;

    /**
     * The subscription metrics contained in this store
     *
     * @param subscriptionKey
     * @return
     */
    MessageStoreSubscriptionStatistics getMessageStoreSubStatistics();

    /**
     * Finds the subscriber entry for the given consumer info
     *
     * @param clientId
     * @param subscriptionName
     *
     * @return the SubscriptionInfo
     *
     * @throws IOException
     */
    SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException;

    /**
     * Lists all the durable subscriptions for a given destination.
     *
     * @return an array SubscriptionInfos
     * @throws IOException
     */
    SubscriptionInfo[] getAllSubscriptions() throws IOException;

    /**
     * Inserts the subscriber info due to a subscription change <p/> If this is
     * a new subscription and the retroactive is false, then the last message
     * sent to the topic should be set as the last message acknowledged by they
     * new subscription. Otherwise, if retroactive is true, then create the
     * subscription without it having an acknowledged message so that on
     * recovery, all message recorded for the topic get replayed.
     *
     * @param subscriptionInfo
     * @param retroactive
     *
     * @throws IOException
     */
    void addSubscription(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException;
}
