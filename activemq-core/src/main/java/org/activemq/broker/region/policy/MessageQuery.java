/**
 * 
 * Copyright 2005 LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.broker.region.policy;

import org.activemq.command.ActiveMQDestination;

import javax.jms.MessageListener;

/**
 * Represents some kind of query which will load messages from some source.
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
    public void execute(ActiveMQDestination destination, MessageListener listener);

}
