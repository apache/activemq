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
import java.util.List;

/**
 * A {@link MessageBuffer} which evicts from the largest buffers first.
 * 
 * 
 */
public class SizeBasedMessageBuffer implements MessageBuffer {

    private int limit = 100 * 64 * 1024;
    private List<MessageQueue> bubbleList = new ArrayList<MessageQueue>();
    private int size;
    private Object lock = new Object();

    public SizeBasedMessageBuffer() {
    }

    public SizeBasedMessageBuffer(int limit) {
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
        MessageQueue queue = new MessageQueue(this);
        synchronized (lock) {
            queue.setPosition(bubbleList.size());
            bubbleList.add(queue);
        }
        return queue;
    }

    /**
     * After a message queue has changed we may need to perform some evictions
     * 
     * @param delta
     * @param queueSize
     */
    public void onSizeChanged(MessageQueue queue, int delta, int queueSize) {
        synchronized (lock) {
            bubbleUp(queue, queueSize);

            size += delta;
            while (size > limit) {
                MessageQueue biggest = bubbleList.get(0);
                size -= biggest.evictMessage();

                bubbleDown(biggest, 0);
            }
        }
    }

    public void clear() {
        synchronized (lock) {
            for (Iterator<MessageQueue> iter = bubbleList.iterator(); iter.hasNext();) {
                MessageQueue queue = iter.next();
                queue.clear();
            }
            size = 0;
        }
    }

    protected void bubbleUp(MessageQueue queue, int queueSize) {
        // lets bubble up to head of queueif we need to
        int position = queue.getPosition();
        while (--position >= 0) {
            MessageQueue pivot = bubbleList.get(position);
            if (pivot.getSize() < queueSize) {
                swap(position, pivot, position + 1, queue);
            } else {
                break;
            }
        }
    }

    protected void bubbleDown(MessageQueue biggest, int position) {
        int queueSize = biggest.getSize();
        int end = bubbleList.size();
        for (int second = position + 1; second < end; second++) {
            MessageQueue pivot = bubbleList.get(second);
            if (pivot.getSize() > queueSize) {
                swap(position, biggest, second, pivot);
            } else {
                break;
            }
            position = second;
        }
    }

    protected void swap(int firstPosition, MessageQueue first, int secondPosition, MessageQueue second) {
        bubbleList.set(firstPosition, second);
        bubbleList.set(secondPosition, first);
        first.setPosition(secondPosition);
        second.setPosition(firstPosition);
    }
}
