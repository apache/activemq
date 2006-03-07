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
package org.apache.activemq.broker.region;

import java.io.IOException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.filter.MessageEvaluationContext;

/**
 * @version $Revision: 1.5 $
 */
public interface Subscription {

    /**
     * Used to add messages that match the subscription.
     * @param node
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
     */
    void remove(ConnectionContext context, Destination destination) throws Exception;
    
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
     */
    void processMessageDispatchNotification(MessageDispatchNotification  mdn);
    
    /**
     * @return true if the broker is currently in slave mode
     */
    boolean isSlaveBroker();
    
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

}