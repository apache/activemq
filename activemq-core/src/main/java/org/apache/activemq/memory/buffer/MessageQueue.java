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
package org.apache.activemq.memory.buffer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.Message;

/**
 * Allows messages to be added to the end of the buffer such that they are kept
 * around and evicted in a FIFO manner.
 * 
 * @version $Revision: 1.1 $
 */
public class MessageQueue {

    private MessageBuffer buffer;
    private LinkedList list = new LinkedList();
    private int size;
    private Object lock = new Object();
    private int position;

    public MessageQueue(MessageBuffer buffer) {
        this.buffer = buffer;
    }

    public void add(MessageReference messageRef) {
        Message message = messageRef.getMessageHardRef();
        int delta = message.getSize();
        int newSize = 0;
        synchronized (lock) {
            list.add(messageRef);
            size += delta;
            newSize = size;
        }
        buffer.onSizeChanged(this, delta, newSize);
    }
    
    public void add(ActiveMQMessage message) {
        int delta = message.getSize();
        int newSize = 0;
        synchronized (lock) {
            list.add(message);
            size += delta;
            newSize = size;
        }
        buffer.onSizeChanged(this, delta, newSize);
    }

    public int evictMessage() {
        synchronized (lock) {
            if (!list.isEmpty()) {
                ActiveMQMessage message = (ActiveMQMessage) list.removeFirst();
                int messageSize = message.getSize();
                size -= messageSize;
                return messageSize;
            }
        }
        return 0;
    }

    /**
     * Returns a copy of the list
     */
    public List getList() {
        synchronized (lock) {
            return new ArrayList(list);
        }
    }

    public void appendMessages(List answer) {
        synchronized (lock) {
            for (Iterator iter = list.iterator(); iter.hasNext();) {
                answer.add(iter.next());
            }
        }
    }

    public int getSize() {
        synchronized (lock) {
            return size;
        }
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void clear() {
        synchronized (lock) {
            list.clear();
            size = 0;
        }
    }

}
