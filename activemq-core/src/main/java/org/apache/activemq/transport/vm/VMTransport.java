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
package org.apache.activemq.transport.vm;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.thread.Valve;
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

    private static final Object DISCONNECT = new Object();
    private static final AtomicLong NEXT_ID = new AtomicLong(0);
    private static final TaskRunnerFactory TASK_RUNNER_FACTORY = new TaskRunnerFactory("VMTransport", Thread.NORM_PRIORITY, true, 1000);
    protected VMTransport peer;
    protected TransportListener transportListener;
    protected boolean disposed;
    protected boolean marshal;
    protected boolean network;
    protected boolean async = true;
    protected int asyncQueueDepth = 2000;
    protected LinkedBlockingQueue<Object> messageQueue;
    protected boolean started;
    protected final URI location;
    protected final long id;
    private TaskRunner taskRunner;
    private final Object lazyInitMutext = new Object();
    private final Valve enqueueValve = new Valve(true);
    private final AtomicBoolean stopping = new AtomicBoolean();
    
    public VMTransport(URI location) {
        this.location = location;
        this.id = NEXT_ID.getAndIncrement();
    }

    public void setPeer(VMTransport peer) {
        this.peer = peer;
    }

    public void oneway(Object command) throws IOException {
        if (disposed) {
            throw new TransportDisposedIOException("Transport disposed.");
        }
        if (peer == null) {
            throw new IOException("Peer not connected.");
        }

        
        TransportListener transportListener=null;
        try {
            // Disable the peer from changing his state while we try to enqueue onto him.
            peer.enqueueValve.increment();
        
            if (peer.disposed || peer.stopping.get()) {
                throw new TransportDisposedIOException("Peer (" + peer.toString() + ") disposed.");
            }
            
            if (peer.started) {
                if (peer.async) {
                    peer.getMessageQueue().put(command);
                    peer.wakeup();
                } else {
                    transportListener = peer.transportListener;
                }
            } else {
                peer.getMessageQueue().put(command);
            }
            
        } catch (InterruptedException e) {
            throw IOExceptionSupport.create(e);
        } finally {
            // Allow the peer to change state again...
            peer.enqueueValve.decrement();
        }

        if( transportListener!=null ) {
            if( command == DISCONNECT ) {
                transportListener.onException(new TransportDisposedIOException("Peer (" + peer.toString() + ") disposed."));
            } else {
                transportListener.onCommand(command);
            }
        }
    }

    public void start() throws Exception {
        if (transportListener == null) {
            throw new IOException("TransportListener not set.");
        }
        try {
            enqueueValve.turnOff();
            if (messageQueue != null && !async) {
                Object command;
                while ((command = messageQueue.poll()) != null && !stopping.get() ) {
                    transportListener.onCommand(command);
                }
            }
            started = true;
            wakeup();
        } finally {
            enqueueValve.turnOn();
        }
        // If we get stopped while starting up, then do the actual stop now 
        // that the enqueueValve is back on.
        if( stopping.get() ) {
            stop();
        }
    }

    public void stop() throws Exception {
        stopping.set(true);
        
        // If stop() is called while being start()ed.. then we can't stop until we return to the start() method.
        if( enqueueValve.isOn() ) {

            TaskRunner tr = null;
            try {
                enqueueValve.turnOff();
                if (!disposed) {
                    started = false;
                    disposed = true;
                    if (taskRunner != null) {
                        tr = taskRunner;
                        taskRunner = null;
                    }
                }
            } finally {
                stopping.set(false);
                enqueueValve.turnOn();
            }
            if (tr != null) {
                tr.shutdown(1000);
            }
            // let the peer know that we are disconnecting..
            try {
                oneway(DISCONNECT);
            } catch (Exception ignore) {
            }
        }
    }
    
    /**
     * @see org.apache.activemq.thread.Task#iterate()
     */
    public boolean iterate() {
        
        final TransportListener tl;
        try {
            // Disable changing the state variables while we are running... 
            enqueueValve.increment();
            tl = transportListener;
            if (!started || disposed || tl == null || stopping.get()) {
                if( stopping.get() ) {
                    // drain the queue it since folks could be blocked putting on to
                    // it and that would not allow the stop() method for finishing up.
                    getMessageQueue().clear();  
                }
                return false;
            }
        } catch (InterruptedException e) {
            return false;
        } finally {
            enqueueValve.decrement();
        }

        LinkedBlockingQueue<Object> mq = getMessageQueue();
        Object command = mq.poll();
        if (command != null) {
            if( command == DISCONNECT ) {
                tl.onException(new TransportDisposedIOException("Peer (" + peer.toString() + ") disposed."));
            } else {
                tl.onCommand(command);
            }
            return !mq.isEmpty();
        } else {
            return false;
        }
        
    }

    public void setTransportListener(TransportListener commandListener) {
        try {
            try {
                enqueueValve.turnOff();
                this.transportListener = commandListener;
                wakeup();
            } finally {
                enqueueValve.turnOn();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private LinkedBlockingQueue<Object> getMessageQueue() {
        synchronized (lazyInitMutext) {
            if (messageQueue == null) {
                messageQueue = new LinkedBlockingQueue<Object>(this.asyncQueueDepth);
            }
            return messageQueue;
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
        return transportListener;
    }

    public <T> T narrow(Class<T> target) {
        if (target.isAssignableFrom(getClass())) {
            return target.cast(this);
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
            synchronized (lazyInitMutext) {
                if (taskRunner == null) {
                    taskRunner = TASK_RUNNER_FACTORY.createTaskRunner(this, "VMTransport: " + toString());
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

	public boolean isDisposed() {
		return disposed;
	}
	
	public boolean isConnected() {
	    return started;
	}

	public void reconnect(URI uri) throws IOException {
		throw new IOException("Not supported");
	}
}
