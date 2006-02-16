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
    private Map queueIndex = new HashMap();
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
            queue = (MessageQueue) queueIndex.get(destination);
            if (queue == null) {
                queue = messageBuffer.createMessageQueue();
                queueIndex.put(destination, queue);
                subscriptionIndex.put(destination, queue);
            }
        }
        queue.add(node);
    }

    public List getMessages(Subscription sub) {
        Set set = null;
        synchronized (lock) {
            set = subscriptionIndex.get(sub.getConsumerInfo().getDestination());
        }
        List answer = new ArrayList();
        for (Iterator iter = set.iterator(); iter.hasNext();) {
            MessageQueue queue = (MessageQueue) iter.next();
            queue.appendMessages(answer);
        }
        return answer;
    }

    public void clear() {
        messageBuffer.clear();
    }
}
