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
package org.apache.activemq.broker.region.policy;


import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.SubscriptionRecovery;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;

/**
 * Abstraction to allow different recovery policies to be plugged
 * into the region implementations.  This is used by a topic to retroactively recover
 * messages that the subscription missed.
 * 
 * @version $Revision$
 */
public interface SubscriptionRecoveryPolicy extends Service {
    
    /**
     * A message was sent to the destination.
     * 
     * @param context
     * @param message 
     * @param node
     * @return true if successful
     * @throws Exception
     */
    boolean add(ConnectionContext context, MessageReference message) throws Exception;
    
    /**
     * Let a subscription recover message held by the policy.
     * 
     * @param context
     * @param topic
     * @param sub 
     * @param node
     * @throws Exception
     */
    void recover(ConnectionContext context, Topic topic, SubscriptionRecovery sub) throws Exception;
    
    
    /**
     * @param dest 
     * @return messages
     * @throws Exception 
     */
    Message[] browse(ActiveMQDestination dest) throws Exception;

    /**
     * Used to copy the policy object.
     * @return the copy
     */
    SubscriptionRecoveryPolicy copy();
}
