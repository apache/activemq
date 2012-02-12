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
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.thread.DefaultThreadPools;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportDisposedIOException;
import org.apache.activemq.transport.TransportListener;

/**
 * A Transport implementation that uses direct method invocations.
 */
public class VMTransport implements Transport, Task {

    private static final Object DISCONNECT = new Object();
    private static final AtomicLong NEXT_ID = new AtomicLong(0);
    protected VMTransport peer;
    protected TransportListener transportListener;
    protected boolean marshal;
    protected boolean network;
    protected boolean async = true;
    protected int asyncQueueDepth = 2000;
    protected final URI location;
    protected final long id;
    protected LinkedBlockingQueue<Object> messageQueue = new LinkedBlockingQueue<Object>(this.asyncQueueDepth);

    protected final AtomicBoolean stopping = new AtomicBoolean();
    protected final AtomicBoolean started = new AtomicBoolean();
    protected final AtomicBoolean starting = new AtomicBoolean();
    protected final AtomicBoolean disposed = new AtomicBoolean();

    // thread can be eager, so initialisation needs to be last  so that partial state is not visible
    protected TaskRunner taskRunner = DefaultThreadPools.getDefaultTaskRunnerFactory().createTaskRunner(this, "VMTransport: " + toString());

    private volatile int receiveCounter;


    public VMTransport(URI location) {
        this.location = location;
        this.id = NEXT_ID.getAndIncrement();
    }

    public void setPeer(VMTransport peer) {
        this.peer = peer;
    }

    public void oneway(Object command) throws IOException {
        if (disposed.get()) {
            throw new TransportDisposedIOException("Transport disposed.");
        }
        if (peer == null) {
            throw new IOException("Peer not connected.");
        }

        TransportListener transportListener = null;
        try {
            if (peer.disposed.get() || peer.stopping.get()) {
                throw new TransportDisposedIOException("Peer (" + peer.toString() + ") disposed.");
            }

            if (peer.started.get()) {
                if (peer.async) {
                    peer.messageQueue.put(command);
                    peer.wakeup();
                } else {
                    transportListener = peer.transportListener;
                }
            } else {
                peer.messageQueue.put(command);
                synchronized (peer.starting) {
                    if (peer.started.get() && !peer.messageQueue.isEmpty()) {
                        // we missed the pending dispatch during start
                        if (peer.async) {
                            peer.wakeup();
                        } else {
                            transportListener = peer.transportListener;
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            InterruptedIOException iioe = new InterruptedIOException(e.getMessage());
            iioe.initCause(e);
            throw iioe;
        }
        dispatch(peer, transportListener, command);
    }

    public void dispatch(VMTransport transport, TransportListener transportListener, Object command) {
        if (transportListener != null) {
            if (command == DISCONNECT) {
                transportListener.onException(
                        new TransportDisposedIOException("Peer (" + peer.toString() + ") disposed."));
            } else {
                transport.receiveCounter++;
                transportListener.onCommand(command);
            }
        }
    }

    public void start() throws Exception {

        if (starting.compareAndSet(false, true)) {

            if (transportListener == null) {
                throw new IOException("TransportListener not set.");
            }

            // ensure there is no missed dispatch during start, sync with oneway
            synchronized (peer.starting) {
                Object command;
                while ((command = messageQueue.poll()) != null && !stopping.get()) {
                    dispatch(this, transportListener, command);
                }

                if (!disposed.get()) {

                    started.set(true);

                    if (async) {
                        taskRunner.wakeup();
                    } else {
                        messageQueue.clear();
                        messageQueue = null;
                        taskRunner.shutdown();
                        taskRunner = null;
                    }
                }
            }
        }
    }

    public void stop() throws Exception {
        if (disposed.compareAndSet(false, true)) {
            stopping.set(true);
            // let the peer know that we are disconnecting..
            try {
                peer.transportListener.onCommand(new ShutdownInfo());
            } catch (Exception ignore) {
            }

            if (messageQueue != null) {
                messageQueue.clear();
            }
            if (taskRunner != null) {
                taskRunner.shutdown(1000);
                taskRunner = null;
            }
        }
    }

    /**
     * @see org.apache.activemq.thread.Task#iterate()
     */
    public boolean iterate() {

        if (disposed.get() || stopping.get()) {
            return false;
        }

        LinkedBlockingQueue<Object> mq = messageQueue;
        Object command = mq.poll();
        if (command != null) {
            if (command == DISCONNECT) {
                transportListener.onException(new TransportDisposedIOException("Peer (" + peer.toString() + ") disposed."));
            } else {
                transportListener.onCommand(command);
            }
            return !mq.isEmpty();
        } else {
            return false;
        }
    }

    public void setTransportListener(TransportListener commandListener) {
        this.transportListener = commandListener;
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

    @Override
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
        return disposed.get();
    }

    public boolean isConnected() {
        return started.get();
    }

    public void reconnect(URI uri) throws IOException {
        throw new IOException("reconnection Not supported by this transport.");
    }

    public boolean isReconnectSupported() {
        return false;
    }

    public boolean isUpdateURIsSupported() {
        return false;
    }

    public void updateURIs(boolean reblance, URI[] uris) throws IOException {
        throw new IOException("Not supported");
    }

    public int getReceiveCounter() {
        return receiveCounter;
    }
}
