/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.transport.vm;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.command.Command;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportDisposedIOException;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Transport implementation that uses direct method invocations.
 * 
 * @version $Revision$
 */
public class VMTransport implements Transport, Task {

    private static final Log log = LogFactory.getLog(VMTransport.class);
    private static final AtomicLong nextId = new AtomicLong(0);
    private static final TaskRunnerFactory taskRunnerFactory = new TaskRunnerFactory("VMTransport", Thread.NORM_PRIORITY, true, 1000);
    protected VMTransport peer;
    protected TransportListener transportListener;
    protected boolean disposed;
    protected boolean marshal;
    protected boolean network;
    protected boolean async = true;
    protected int asyncQueueDepth = 2000;
    protected LinkedBlockingQueue messageQueue = null;
    protected boolean started;
    protected final URI location;
    protected final long id;
    private TaskRunner taskRunner;
    private final Object mutex = new Object();

    public VMTransport(URI location) {
        this.location = location;
        this.id = nextId.getAndIncrement();
    }

    public VMTransport getPeer() {
        synchronized (mutex) {
            return peer;
        }
    }

    public void setPeer(VMTransport peer) {
        synchronized (mutex) {
            this.peer = peer;
        }
    }

    public void oneway(Object command) throws IOException {
        if (disposed) {
            throw new TransportDisposedIOException("Transport disposed.");
        }
        if (peer == null)
            throw new IOException("Peer not connected.");

        TransportListener tl = null;
        synchronized (peer.mutex) {
            if (peer.disposed) {
                throw new TransportDisposedIOException("Peer (" + peer.toString() + ") disposed.");
            }
            if (peer.started) {
                if (peer.async) {
                    peer.enqueue(command);
                    peer.wakeup();
                } else {
                    tl = peer.transportListener;
                }
            } else {
                peer.enqueue(command);
            }
        }

        if (tl != null) {
            tl.onCommand(command);
        }

    }

    private void enqueue(Object command) throws IOException {
        try {
            getMessageQueue().put(command);
        } catch (final InterruptedException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    public Object request(Object command) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    public Object request(Object command, int timeout) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    public TransportListener getTransportListener() {
        synchronized (mutex) {
            return transportListener;
        }
    }

    public void setTransportListener(TransportListener commandListener) {
        synchronized (mutex) {
            this.transportListener = commandListener;
            wakeup();
        }
    }

    private LinkedBlockingQueue getMessageQueue() {
        synchronized (mutex) {
            if (messageQueue == null) {
                messageQueue = new LinkedBlockingQueue(this.asyncQueueDepth);
            }
            return messageQueue;
        }
    }

    public void start() throws Exception {
        if (transportListener == null)
            throw new IOException("TransportListener not set.");

        synchronized (mutex) {
            if (messageQueue != null) {
                Object command;
                while ((command = messageQueue.poll()) != null) {
                    transportListener.onCommand(command);
                }
            }
            started = true;
            wakeup();
        }
    }

    public void stop() throws Exception {
        TaskRunner tr = null;
        synchronized (mutex) {
            if (!disposed) {
                started = false;
                disposed = true;
                if (taskRunner != null) {
                    tr = taskRunner;
                    taskRunner = null;
                }
            }
        }
        if (tr != null) {
            tr.shutdown(1000);
        }
    }

    public Object narrow(Class target) {
        if (target.isAssignableFrom(getClass())) {
            return this;
        }
        return null;
    }

    public boolean isMarshal() {
        return marshal;
    }

    public void setMarshal(boolean marshal) {
        this.marshal = marshal;
    }

    public boolean isNetwork() {
        return network;
    }

    public void setNetwork(boolean network) {
        this.network = network;
    }

    public String toString() {
        return location + "#" + id;
    }

    public String getRemoteAddress() {
        if (peer != null) {
            return peer.toString();
        }
        return null;
    }

    /**
     * @see org.apache.activemq.thread.Task#iterate()
     */
    public boolean iterate() {
        final TransportListener tl;
        synchronized (mutex) {
            tl = transportListener;
            if (!started || disposed || tl == null)
                return false;
        }

        LinkedBlockingQueue mq = getMessageQueue();
        final Command command = (Command)mq.poll();
        if (command != null) {
            tl.onCommand(command);
            return !mq.isEmpty();
        } else {
            return false;
        }
    }

    /**
     * @return the async
     */
    public boolean isAsync() {
        return async;
    }

    /**
     * @param async the async to set
     */
    public void setAsync(boolean async) {
        this.async = async;
    }

    /**
     * @return the asyncQueueDepth
     */
    public int getAsyncQueueDepth() {
        return asyncQueueDepth;
    }

    /**
     * @param asyncQueueDepth the asyncQueueDepth to set
     */
    public void setAsyncQueueDepth(int asyncQueueDepth) {
        this.asyncQueueDepth = asyncQueueDepth;
    }

    protected void wakeup() {
        if (async) {
            synchronized (mutex) {
                if (taskRunner == null) {
                    taskRunner = taskRunnerFactory.createTaskRunner(this, "VMTransport: " + toString());
                }
            }
            try {
                taskRunner.wakeup();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isFaultTolerant() {
        return false;
    }
}
