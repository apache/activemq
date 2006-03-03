/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;

/**
 * A MessageStore for durable topic subscriptions
 *
 * @version $Revision: 1.4 $
 */
public interface TopicMessageStore extends MessageStore {

    /**
     * Stores the last acknowledged messgeID for the given subscription
     * so that we can recover and commence dispatching messages from the last
     * checkpoint
     * @param context TODO
     * @param messageId
     * @param subscriptionPersistentId
     */
    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId) throws IOException;

    /**
     * @param sub
     * @throws JMSException 
     */
    public void deleteSubscription(String clientId, String subscriptionName) throws IOException;
    
    /**
     * For the new subscription find the last acknowledged message ID
     * and then find any new messages since then and dispatch them
     * to the subscription.
     * <p/>
     * e.g. if we dispatched some messages to a new durable topic subscriber, then went down before
     * acknowledging any messages, we need to know the correct point from which to recover from.
     * @param subscription
     *
     * @throws Exception 
     */
    public void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener) throws Exception;

    /**
     * Finds the subscriber entry for the given consumer info
     * 
     * @param clientId TODO
     * @param subscriptionName TODO
     * @return
     */
    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException;

    /**
     * Lists all the durable subscirptions for a given destination.
     * 
     * @param clientId TODO
     * @param subscriptionName TODO
     * @return
     */
    public SubscriptionInfo[] getAllSubscriptions() throws IOException;

    /**
     * Inserts the subscriber info due to a subscription change
     * <p/>
     * If this is a new subscription and the retroactive is false, then the last
     * message sent to the topic should be set as the last message acknowledged by they new
     * subscription.  Otherwise, if retroactive is true, then create the subscription without 
     * it having an acknowledged message so that on recovery, all message recorded for the 
     * topic get replayed.
     * @param retroactive TODO
     *
     */
    public void addSubsciption(String clientId, String subscriptionName, String selector, boolean retroactive) throws IOException;

}
