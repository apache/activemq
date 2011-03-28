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
package org.apache.activemq.broker.region;

import java.io.IOException;
import java.util.List;

import javax.jms.InvalidSelectorException;
import javax.management.ObjectName;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.Response;
import org.apache.activemq.filter.MessageEvaluationContext;

/**
 * 
 */
public interface Subscription extends SubscriptionRecovery {

    /**
     * Used to add messages that match the subscription.
     * @param node
     * @throws Exception 
     * @throws InterruptedException 
     * @throws IOException 
     */
    void add(MessageReference node) throws Exception;
    
    /**
     * Used when client acknowledge receipt of dispatched message. 
     * @param node
     * @throws IOException 
     * @throws Exception 
     */
    void acknowledge(ConnectionContext context, final MessageAck ack) throws Exception;
    

    /**
     * Allows a consumer to pull a message on demand
     */
    Response pullMessage(ConnectionContext context, MessagePull pull) throws Exception;

    /**
     * Is the subscription interested in the message?
     * @param node 
     * @param context
     * @return
     * @throws IOException 
     */
    boolean matches(MessageReference node, MessageEvaluationContext context) throws IOException;
    
    /**
     * Is the subscription interested in messages in the destination?
     * @param context
     * @return
     */
    boolean matches(ActiveMQDestination destination);
    
    /**
     * The subscription will be receiving messages from the destination.
     * @param context 
     * @param destination
     * @throws Exception 
     */
    void add(ConnectionContext context, Destination destination) throws Exception;
    
    /**
     * The subscription will be no longer be receiving messages from the destination.
     * @param context 
     * @param destination
     * @return a list of un-acked messages that were added to the subscription.
     */
    List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception;
    
    /**
     * The ConsumerInfo object that created the subscription.
     * @param destination
     */
    ConsumerInfo getConsumerInfo();

    /**
     * The subscription should release as may references as it can to help the garbage collector
     * reclaim memory.
     */
    void gc();
    
    /**
     * Used by a Slave Broker to update dispatch infomation
     * @param mdn
     * @throws Exception 
     */
    void processMessageDispatchNotification(MessageDispatchNotification  mdn) throws Exception;
    
    /**
     * @return true if the broker is currently in slave mode
     */
    boolean isSlave();
    
    /**
     * @return number of messages pending delivery
     */
    int getPendingQueueSize();
    
    /**
     * @return number of messages dispatched to the client
     */
    int getDispatchedQueueSize();
        
    /**
     * @return number of messages dispatched to the client
     */
    long getDispatchedCounter();
    
    /**
     * @return number of messages that matched the subscription
     */
    long getEnqueueCounter();

    /**
     * @return number of messages queued by the client
     */
    long getDequeueCounter();

    /**
     * @return the JMS selector on the current subscription
     */
    String getSelector();
    
    /**
     * Attempts to change the current active selector on the subscription.
     * This operation is not supported for persistent topics.
     */
    void setSelector(String selector) throws InvalidSelectorException, UnsupportedOperationException;

    /**
     * @return the JMX object name that this subscription was registered as if applicable
     */
    ObjectName getObjectName();

    /**
     * Set when the subscription is registered in JMX
     */
    void setObjectName(ObjectName objectName);
    
    /**
     * @return true when 60% or more room is left for dispatching messages
     */
    boolean isLowWaterMark();
    
    /**
     * @return true when 10% or less room is left for dispatching messages
     */
    boolean isHighWaterMark();
    
    /**
     * @return true if there is no space to dispatch messages
     */
    boolean isFull();
    
    /**
     * inform the MessageConsumer on the client to change it's prefetch
     * @param newPrefetch
     */
    void updateConsumerPrefetch(int newPrefetch);
    
        
    /**
     * Called when the subscription is destroyed.
     */
    void destroy();

    /**
     * @return the prefetch size that is configured for the subscription
     */
    int getPrefetchSize();
    
    /**
     * @return the number of messages awaiting acknowledgement
     */
    int getInFlightSize();
    
    /**
     * @return the in flight messages as a percentage of the prefetch size
     */
    int getInFlightUsage();
    
    /**
     * Informs the Broker if the subscription needs to intervention to recover it's state
     * e.g. DurableTopicSubscriber may do
     * @see org.apache.activemq.region.cursors.PendingMessageCursor
     * @return true if recovery required
     */
    boolean isRecoveryRequired();
    
    
    /**
     * @return true if a browser
     */
    boolean isBrowser();
    
    /**
     * @return the number of messages this subscription can accept before its full
     */
    int countBeforeFull();

    ConnectionContext getContext();
    
    public int getCursorMemoryHighWaterMark();

	public void setCursorMemoryHighWaterMark(int cursorMemoryHighWaterMark);

    boolean isSlowConsumer();

    void unmatched(MessageReference node) throws IOException;
}
