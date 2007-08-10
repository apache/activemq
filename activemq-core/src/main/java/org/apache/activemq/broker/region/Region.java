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

import java.util.Map;
import java.util.Set;

import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;

/**
 * A Region is used to implement the different QOS options available to 
 * a broker.  A Broker is composed of multiple message processing Regions that
 * provide different QOS options.
 * 
 * @version $Revision$
 */
public interface Region extends Service {

    /**
     * Used to create a destination.  Usually, this method is invoked as a side-effect of sending
     * a message to a destination that does not exist yet.
     * 
     * @param context
     * @param destination the destination to create.
     * @return TODO
     * @throws Exception TODO
     */
    Destination addDestination(ConnectionContext context, ActiveMQDestination destination) throws Exception;
    
    /**
     * Used to destroy a destination.  
     * This should try to quiesce use of the destination up to the timeout allotted time before removing the destination.
     * This will remove all persistent messages associated with the destination.
     * 
     * @param context the environment the operation is being executed under.
     * @param destination what is being removed from the broker.
     * @param timeout the max amount of time to wait for the destination to quiesce
     * @throws Exception TODO
     */
    void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception;

    /**
     * Returns a copy of the current destinations available in the region
     * 
     * @return a copy of the regions currently active at the time of the call with the key the destination and the value the Destination.
     */
    Map<ActiveMQDestination, Destination> getDestinationMap();
    

    /**
     * Adds a consumer.
     * @param context the environment the operation is being executed under.
     * @return TODO
     * @throws Exception TODO
     */
    Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception;

    /**
     * Removes a consumer.
     * @param context the environment the operation is being executed under.
     * @throws Exception TODO
     */
    void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception;

    /**
     * Deletes a durable subscription.
     * @param context the environment the operation is being executed under.
     * @param info TODO
     * @throws Exception TODO
     */
    void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception;
    
    /**
     * Send a message to the broker to using the specified destination.  The destination specified
     * in the message does not need to match the destination the message is sent to.  This is 
     * handy in case the message is being sent to a dead letter destination.
     * @param producerExchange the environment the operation is being executed under.
     * @param message 
     * @throws Exception TODO
     */
    void send(ProducerBrokerExchange producerExchange, Message message) throws Exception;
    
    /**
     * Used to acknowledge the receipt of a message by a client.
     * @param consumerExchange the environment the operation is being executed under.
     * @throws Exception TODO
     */
    void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception;
    
    /**
     * Allows a consumer to pull a message from a queue
     */
    Response messagePull(ConnectionContext context, MessagePull pull) throws Exception;

    /**
     * Process a notification of a dispatch - used by a Slave Broker
     * @param messageDispatchNotification
     * @throws Exception TODO
     */
    void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception;

    void gc();

    /**
     * Provide an exact or wildcard lookup of destinations in the region
     * 
     * @return a set of matching destination objects.
     */
    Set getDestinations(ActiveMQDestination destination);
    
}
