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
package org.apache.activemq;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.activemq.command.MessageDispatch;

public class SimplePriorityMessageDispatchChannel implements MessageDispatchChannel {
    private static final Integer MAX_PRIORITY = 10;
    private final Object mutex = new Object();
    private final LinkedList<MessageDispatch>[] lists;
    private boolean closed;
    private boolean running;
    private int size = 0;

    @SuppressWarnings("unchecked")
    public SimplePriorityMessageDispatchChannel() {
        this.lists = new LinkedList[MAX_PRIORITY];
        for (int i = 0; i < MAX_PRIORITY; i++) {
            lists[i] = new LinkedList<MessageDispatch>();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#enqueue(org.apache.activemq.command.MessageDispatch)
     */
    @Override
    public void enqueue(MessageDispatch message) {
        synchronized (mutex) {
            getList(message).addLast(message);
            this.size++;
            mutex.notify();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#enqueueFirst(org.apache.activemq.command.MessageDispatch)
     */
    @Override
    public void enqueueFirst(MessageDispatch message) {
        synchronized (mutex) {
            getList(message).addFirst(message);
            this.size++;
            mutex.notify();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        return this.size == 0;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#dequeue(long)
     */
    @Override
    public MessageDispatch dequeue(long timeout) throws InterruptedException {
        synchronized (mutex) {
            // Wait until the consumer is ready to deliver messages.
            while (timeout != 0 && !closed && (isEmpty() || !running)) {
                if (timeout == -1) {
                    mutex.wait();
                } else {
                    mutex.wait(timeout);
                    break;
                }
            }
            if (closed || !running || isEmpty()) {
                return null;
            }
            return removeFirst();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#dequeueNoWait()
     */
    @Override
    public MessageDispatch dequeueNoWait() {
        synchronized (mutex) {
            if (closed || !running || isEmpty()) {
                return null;
            }
            return removeFirst();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#peek()
     */
    @Override
    public MessageDispatch peek() {
        synchronized (mutex) {
            if (closed || !running || isEmpty()) {
                return null;
            }
            return getFirst();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#start()
     */
    @Override
    public void start() {
        synchronized (mutex) {
            running = true;
            mutex.notifyAll();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#stop()
     */
    @Override
    public void stop() {
        synchronized (mutex) {
            running = false;
            mutex.notifyAll();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#close()
     */
    @Override
    public void close() {
        synchronized (mutex) {
            if (!closed) {
                running = false;
                closed = true;
            }
            mutex.notifyAll();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#clear()
     */
    @Override
    public void clear() {
        synchronized (mutex) {
            for (int i = 0; i < MAX_PRIORITY; i++) {
                lists[i].clear();
            }
            this.size = 0;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#isClosed()
     */
    @Override
    public boolean isClosed() {
        return closed;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#size()
     */
    @Override
    public int size() {
        synchronized (mutex) {
            return this.size;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#getMutex()
     */
    @Override
    public Object getMutex() {
        return mutex;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#isRunning()
     */
    @Override
    public boolean isRunning() {
        return running;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.activemq.MessageDispatchChannelI#removeAll()
     */
    @Override
    public List<MessageDispatch> removeAll() {
        synchronized (mutex) {
            ArrayList<MessageDispatch> result = new ArrayList<MessageDispatch>(size());
            for (int i = MAX_PRIORITY - 1; i >= 0; i--) {
                List<MessageDispatch> list = lists[i];
                result.addAll(list);
                size -= list.size();
                list.clear();
            }
            return result;
        }
    }

    @Override
    public String toString() {
        String result = "";
        for (int i = MAX_PRIORITY - 1; i >= 0; i--) {
            result += i + ":{" + lists[i].toString() + "}";
        }
        return result;
    }

    protected int getPriority(MessageDispatch message) {
        int priority = javax.jms.Message.DEFAULT_PRIORITY;
        if (message.getMessage() != null) {
            priority = Math.max(message.getMessage().getPriority(), 0);
            priority = Math.min(priority, 9);
        }
        return priority;
    }

    protected LinkedList<MessageDispatch> getList(MessageDispatch md) {
        return lists[getPriority(md)];
    }

    private final MessageDispatch removeFirst() {
        if (this.size > 0) {
            for (int i = MAX_PRIORITY - 1; i >= 0; i--) {
                LinkedList<MessageDispatch> list = lists[i];
                if (!list.isEmpty()) {
                    this.size--;
                    return list.removeFirst();
                }
            }
        }
        return null;
    }

    private final MessageDispatch getFirst() {
        if (this.size > 0) {
            for (int i = MAX_PRIORITY - 1; i >= 0; i--) {
                LinkedList<MessageDispatch> list = lists[i];
                if (!list.isEmpty()) {
                    return list.getFirst();
                }
            }
        }
        return null;
    }
}
