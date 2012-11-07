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
package org.apache.activemq.memory.list;

import java.util.List;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;

/**
 * A container of messages which is used to store messages and then 
 * replay them later for a given subscription.
 * 
 * 
 */
public interface MessageList {

    void add(MessageReference node);

    /**
     * Returns the current list of MessageReference objects for the given subscription
     */
    List getMessages(ActiveMQDestination destination);
    
    /**
     * @param destination
     * @return an array of Messages that match the destination
     */
    Message[] browse(ActiveMQDestination destination);

    void clear();
    
    
}
