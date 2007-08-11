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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.memory.buffer.MessageBuffer;
import org.apache.activemq.memory.buffer.MessageQueue;
import org.apache.activemq.memory.buffer.OrderBasedMessageBuffer;

/**
 * An implementation of {@link MessageList} which maintains a separate message
 * list for each destination to reduce contention on the list and to speed up
 * recovery times by only recovering the interested topics.
 * 
 * @version $Revision: 1.1 $
 */
public class DestinationBasedMessageList implements MessageList {

    private MessageBuffer messageBuffer;
    private Map<ActiveMQDestination, MessageQueue> queueIndex = new HashMap<ActiveMQDestination, MessageQueue>();
    private DestinationMap subscriptionIndex = new DestinationMap();
    private Object lock = new Object();

    public DestinationBasedMessageList(int maximumSize) {
        this(new OrderBasedMessageBuffer(maximumSize));
    }
    
    public DestinationBasedMessageList(MessageBuffer buffer) {
        messageBuffer = buffer;
    }

    public void add(MessageReference node) {
        ActiveMQMessage message = (ActiveMQMessage) node.getMessageHardRef();
        ActiveMQDestination destination = message.getDestination();
        MessageQueue queue = null;
        synchronized (lock) {
            queue = queueIndex.get(destination);
            if (queue == null) {
                queue = messageBuffer.createMessageQueue();
                queueIndex.put(destination, queue);
                subscriptionIndex.put(destination, queue);
            }
        }
        queue.add(node);
    }

    public List<MessageReference> getMessages(Subscription sub) {
        return getMessages(sub.getConsumerInfo().getDestination());
    }
    
    public  List<MessageReference> getMessages(ActiveMQDestination destination) {
        Set set = null;
        synchronized (lock) {
            set = subscriptionIndex.get(destination);
        }
        List<MessageReference> answer = new ArrayList<MessageReference>();
        for (Iterator iter = set.iterator(); iter.hasNext();) {
            MessageQueue queue = (MessageQueue) iter.next();
            queue.appendMessages(answer);
        }
        return answer;
    }
    
    public Message[] browse(ActiveMQDestination destination) {
        List<MessageReference> result = getMessages(destination);
        return result.toArray(new Message[result.size()]);
    }


    public void clear() {
        messageBuffer.clear();
    }
}
