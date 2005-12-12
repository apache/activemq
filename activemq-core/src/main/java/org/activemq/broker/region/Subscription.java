/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
*
**/
package org.activemq.broker.region;

import java.io.IOException;

import org.activemq.broker.ConnectionContext;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.MessageAck;
import org.activemq.filter.MessageEvaluationContext;

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
    void add(MessageReference node) throws Throwable;
    
    /**
     * Used when client acknowledge receipt of dispatched message. 
     * @param node
     * @throws IOException 
     * @throws Throwable 
     */
    void acknowledge(ConnectionContext context, final MessageAck ack) throws Throwable;
    
    /**
     * Is the subscription interested in the message?
     * @param node 
     * @param context
     * @return
     */
    boolean matches(MessageReference node, MessageEvaluationContext context);
    
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
     * @throws Throwable 
     */
    void add(ConnectionContext context, Destination destination) throws Throwable;
    
    /**
     * The subscription will be no longer be receiving messages from the destination.
     * @param context 
     * @param destination
     */
    void remove(ConnectionContext context, Destination destination) throws Throwable;
    
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
    
}