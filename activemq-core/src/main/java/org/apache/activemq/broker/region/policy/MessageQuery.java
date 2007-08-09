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

import javax.jms.MessageListener;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;

/**
 * Represents some kind of query which will load initial messages from some source for a new topic subscriber.
 * 
 * @version $Revision$
 */
public interface MessageQuery {
    
    /**
     * Executes the query for messages; each message is passed into the listener
     * 
     * @param destination the destination on which the query is to be performed
     * @param listener is the listener to notify as each message is created or loaded
     */
    public void execute(ActiveMQDestination destination, MessageListener listener) throws Exception;

    /**
     * Returns true if the given update is valid and does not overlap with the initial message query.
     * When performing an initial load from some source, there is a chance that an update may occur which is logically before
     * the message sent on the initial load - so this method provides a hook where the query instance can keep track of the version IDs
     * of the messages sent so that if an older version is sent as an update it can be excluded to avoid going backwards in time.
     * 
     * e.g. if the execute() method creates version 2 of an object and then an update message is sent for version 1, this method should return false to 
     * hide the old update message.
     * 
     * @param message the update message which may have been sent before the query actually completed
     * @return true if the update message is valid otherwise false in which case the update message will be discarded.
     */
    public boolean validateUpdate(Message message);

}
