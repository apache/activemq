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

import org.activemq.broker.ConnectionContext;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.RemoveSubscriptionInfo;

/**
 * A Region is used to implement the different QOS options available to 
 * a broker.  A Broker is composed of multiple mesasge processing Regions that
 * provide different QOS options.
 * 
 * @version $Revision$
 */
public interface Region {

    /**
     * Used to create a destination.  Usually, this method is invoked as a side-effect of sending
     * a message to a destiantion that does not exist yet.
     * 
     * @param context
     * @param destination the destination to create.
     * @return TODO
     */
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Throwable;
    
    /**
     * Used to destory a destination.  
     * This shoud try to quiesce use of the destination up to the timeout alotted time before removing the destination.
     * This will remove all persistent messages associated with the destination.
     * 
     * @param context the enviorment the operation is being executed under.
     * @param destination what is being removed from the broker.
     * @param timeout the max amount of time to wait for the destination to quiesce
     */
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Throwable;

    /**
     * Adds a consumer.
     * @param context the enviorment the operation is being executed under.
     */
    public void addConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable;

    /**
     * Removes a consumer.
     * @param context the enviorment the operation is being executed under.
     */
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Throwable;

    /**
     * Deletes a durable subscription.
     * @param context the enviorment the operation is being executed under.
     * @param info TODO
     */
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Throwable;
    
    /**
     * Send a message to the broker to using the specified destination.  The destination specified
     * in the message does not need to match the destination the message is sent to.  This is 
     * handy in case the message is being sent to a dead letter destination.
     * @param context the enviorment the operation is being executed under.
     */
    public void send(ConnectionContext context, Message message) throws Throwable;
    
    /**
     * Used to acknowledge the receipt of a message by a client.
     * @param context the enviorment the operation is being executed under.
     */
    public void acknowledge(ConnectionContext context, MessageAck ack) throws Throwable;

    public void gc();
    
}
