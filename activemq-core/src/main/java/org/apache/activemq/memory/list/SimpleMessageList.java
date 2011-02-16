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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.DestinationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple fixed size {@link MessageList} where there is a single, fixed size
 * list that all messages are added to for simplicity. Though this will lead to
 * possibly slow recovery times as many more messages than is necessary will
 * have to be iterated through for each subscription.
 * 
 * 
 */
public class SimpleMessageList implements MessageList {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleMessageList.class);
    private final LinkedList<MessageReference> list = new LinkedList<MessageReference>();
    private int maximumSize = 100 * 64 * 1024;
    private int size;
    private final Object lock = new Object();

    public SimpleMessageList() {
    }

    public SimpleMessageList(int maximumSize) {
        this.maximumSize = maximumSize;
    }

    public void add(MessageReference node) {
        int delta = node.getMessageHardRef().getSize();
        synchronized (lock) {
            list.add(node);
            size += delta;
            while (size > maximumSize) {
                MessageReference evicted = list.removeFirst();
                size -= evicted.getMessageHardRef().getSize();
            }
        }
    }

    public List<MessageReference> getMessages(ActiveMQDestination destination) {
        return getList();
    }

    public Message[] browse(ActiveMQDestination destination) {
        List<Message> result = new ArrayList<Message>();
        DestinationFilter filter = DestinationFilter.parseFilter(destination);
        synchronized (lock) {
            for (Iterator<MessageReference> i = list.iterator(); i.hasNext();) {
                MessageReference ref = i.next();
                Message msg;
                msg = ref.getMessage();
                if (filter.matches(msg.getDestination())) {
                    result.add(msg);
                }

            }
        }
        return result.toArray(new Message[result.size()]);
    }

    /**
     * Returns a copy of the list
     */
    public List<MessageReference> getList() {
        synchronized (lock) {
            return new ArrayList<MessageReference>(list);
        }
    }

    public int getSize() {
        synchronized (lock) {
            return size;
        }
    }

    public void clear() {
        synchronized (lock) {
            list.clear();
            size = 0;
        }
    }

}
