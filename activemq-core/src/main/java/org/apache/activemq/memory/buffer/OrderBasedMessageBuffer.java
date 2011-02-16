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

import java.util.Iterator;
import java.util.LinkedList;

/**
 * A {@link MessageBuffer} which evicts messages in arrival order so the oldest
 * messages are removed first.
 * 
 * 
 */
public class OrderBasedMessageBuffer implements MessageBuffer {

    private int limit = 100 * 64 * 1024;
    private LinkedList<MessageQueue> list = new LinkedList<MessageQueue>();
    private int size;
    private Object lock = new Object();

    public OrderBasedMessageBuffer() {
    }

    public OrderBasedMessageBuffer(int limit) {
        this.limit = limit;
    }

    public int getSize() {
        synchronized (lock) {
            return size;
        }
    }

    /**
     * Creates a new message queue instance
     */
    public MessageQueue createMessageQueue() {
        return new MessageQueue(this);
    }

    /**
     * After a message queue has changed we may need to perform some evictions
     * 
     * @param delta
     * @param queueSize
     */
    public void onSizeChanged(MessageQueue queue, int delta, int queueSize) {
        synchronized (lock) {
            list.addLast(queue);
            size += delta;
            while (size > limit) {
                MessageQueue biggest = list.removeFirst();
                size -= biggest.evictMessage();
            }
        }
    }

    public void clear() {
        synchronized (lock) {
            for (Iterator<MessageQueue> iter = list.iterator(); iter.hasNext();) {
                MessageQueue queue = iter.next();
                queue.clear();
            }
            size = 0;
        }
    }

}
