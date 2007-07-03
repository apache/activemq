/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import javax.jms.JMSException;

import org.apache.activemq.command.MessageDispatch;

public class MessageDispatchChannel {

    private final Object mutex = new Object();
    private final LinkedList<MessageDispatch> list;
    private boolean closed;
    private boolean running;

    public MessageDispatchChannel() {
        this.list = new LinkedList<MessageDispatch>();
    }

    public void enqueue(MessageDispatch message) {
        synchronized(mutex) {
            list.addLast(message);
            mutex.notify();
        }
    }

    public void enqueueFirst(MessageDispatch message) {
        synchronized(mutex) {
            list.addFirst(message);
            mutex.notify();
        }
    }

    public boolean isEmpty() {
        synchronized(mutex) {
            return list.isEmpty();
        }
    }

    /**
     * Used to get an enqueued message. 
     * The amount of time this method blocks is based on the timeout value. 
     * - if timeout==-1 then it blocks until a message is received. 
     * - if timeout==0 then it it tries to not block at all, it returns a message if it is available 
     * - if timeout>0 then it blocks up to timeout amount of time.
     * 
     * Expired messages will consumed by this method.  
     * 
     * @throws JMSException 
     * 
     * @return null if we timeout or if the consumer is closed.
     * @throws InterruptedException 
     */
    public MessageDispatch dequeue(long timeout) throws InterruptedException {
        synchronized (mutex) {
            // Wait until the consumer is ready to deliver messages.
            while(timeout != 0 && !closed && (list.isEmpty() || !running)) {
                if (timeout == -1) {
                    mutex.wait();
                } else {
                    mutex.wait(timeout);
                    break;
                }
            }
            if (closed || !running || list.isEmpty()) {
                return null;
            }
            return list.removeFirst();
        }
    }
    
    public MessageDispatch dequeueNoWait() {
        synchronized (mutex) {
            if (closed || !running || list.isEmpty()) {
                return null;
            }
            return list.removeFirst();
        }
    }
    
    public MessageDispatch peek() {
        synchronized (mutex) {
            if (closed || !running || list.isEmpty()) {
                return null;
            }
            return list.getFirst();
        }
    }

    public void start() {
        synchronized (mutex) {
            running = true;
            mutex.notifyAll();
        }
    }

    public void stop() {
        synchronized (mutex) {
            running = false;
            mutex.notifyAll();
        }
    }

    public void close() {
        synchronized (mutex) {
            if (!closed) {
                running = false;
                closed = true;
            }
            mutex.notifyAll();
        }
    }

    public void clear() {
        synchronized(mutex) {
            list.clear();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public int size() {
        synchronized(mutex) {
            return list.size();
        }
    }

    public Object getMutex() {
        return mutex;
    }

    public boolean isRunning() {
        return running;
    }

    public List<MessageDispatch> removeAll() {
        synchronized(mutex) {
            ArrayList <MessageDispatch>rc = new ArrayList<MessageDispatch>(list);
            list.clear();
            return rc;
        }
    }

    public String toString() {
        synchronized(mutex) {
            return list.toString();
        }
    }
}
