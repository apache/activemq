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

import org.activemq.Service;
import org.activemq.broker.ConnectionContext;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.MessageId;
import org.activemq.memory.UsageManager;
import org.activemq.store.MessageStore;

/**
 * 
 * @version $Revision: 1.12 $
 */
public interface Destination extends Service {

    void addSubscription(ConnectionContext context, Subscription sub) throws Throwable;
    void removeSubscription(ConnectionContext context, Subscription sub) throws Throwable;
    
    void send(ConnectionContext context, Message messageSend) throws Throwable;
    boolean lock(MessageReference node, Subscription subscription);
    void acknowledge(ConnectionContext context, Subscription sub, final MessageAck ack, final MessageReference node) throws IOException;
    
    void gc();
    Message loadMessage(MessageId messageId) throws IOException;
 
    ActiveMQDestination getActiveMQDestination();
    UsageManager getUsageManager();

    void dispose(ConnectionContext context) throws IOException;
    
    DestinationStatistics getDestinationStatistics();
    MessageStore getMessageStore();
}