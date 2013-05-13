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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
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

    // Transport Configuration
    protected VMTransport peer;
    protected TransportListener transportListener;
    protected boolean marshal;
    protected boolean network;
    protected boolean async = true;
    protected int asyncQueueDepth = 2000;
    protected final URI location;
    protected final long id;

    // Implementation
    private LinkedBlockingQueue<Object> messageQueue;
    private TaskRunnerFactory taskRunnerFactory;
    private TaskRunner taskRunner;

    // Transport State
    protected final AtomicBoolean started = new AtomicBoolean();
    protected final AtomicBoolean disposed = new AtomicBoolean();

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

        try {

            if (peer.disposed.get()) {
                throw new TransportDisposedIOException("Peer (" + peer.toString() + ") disposed.");
            }

            if (peer.async || !peer.started.get()) {
                peer.getMessageQueue().put(command);
                peer.wakeup();
                return;
            }

        } catch (InterruptedException e) {
            InterruptedIOException iioe = new InterruptedIOException(e.getMessage());
            iioe.initCause(e);
            throw iioe;
        }

        dispatch(peer, peer.messageQueue, command);
    }

    public void dispatch(VMTransport transport, BlockingQueue<Object> pending, Object command) {
        TransportListener transportListener = transport.getTransportListener();
        if (transportListener != null) {
            // Lock here on the target transport's started since we want to wait for its start()
            // method to finish dispatching out of the queue before we do our own.
            synchronized (transport.started) {

                // Ensure that no additional commands entered the queue in the small time window
                // before the start method locks the dispatch lock and the oneway method was in
                // an put operation.
                while(pending != null && !pending.isEmpty() && !transport.isDisposed()) {
                    doDispatch(transport, transportListener, pending.poll());
                }

                // We are now in sync mode and won't enqueue any more commands to the target
                // transport so lets clean up its resources.
                transport.messageQueue = null;

                // Don't dispatch if either end was disposed already.
                if (command != null && !this.disposed.get() && !transport.isDisposed()) {
                    doDispatch(transport, transportListener, command);
                }
            }
        }
    }

    public void doDispatch(VMTransport transport, TransportListener transportListener, Object command) {
        if (command == DISCONNECT) {
            transportListener.onException(new TransportDisposedIOException("Peer (" + peer.toString() + ") disposed."));
        } else {
            transport.receiveCounter++;
            transportListener.onCommand(command);
        }
    }

    public void start() throws Exception {

        if (transportListener == null) {
            throw new IOException("TransportListener not set.");
        }

        // If we are not in async mode we lock the dispatch lock here and then start to
        // prevent any sync dispatches from occurring until we dispatch the pending messages
        // to maintain delivery order.  When async this happens automatically so just set
        // started and wakeup the task runner.
        if (!async) {
            synchronized (started) {
                if (started.compareAndSet(false, true)) {
                    LinkedBlockingQueue<Object> mq = getMessageQueue();
                    Object command;
                    while ((command = mq.poll()) != null && !disposed.get() ) {
                        receiveCounter++;
                        doDispatch(this, transportListener, command);
                    }
                }
            }
        } else {
            if (started.compareAndSet(false, true)) {
                wakeup();
            }
        }
    }

    public void stop() throws Exception {
        // Only need to do this once, all future oneway calls will now
        // fail as will any asnyc jobs in the task runner.
        if (disposed.compareAndSet(false, true)) {

            TaskRunner tr = taskRunner;
            LinkedBlockingQueue<Object> mq = this.messageQueue;

            taskRunner = null;
            messageQueue = null;

            if (mq != null) {
                mq.clear();
            }

            // Allow pending deliveries to finish up, but don't wait
            // forever in case of an stalled onCommand.
            if (tr != null) {
                try {
                    tr.shutdown(TimeUnit.SECONDS.toMillis(1));
                } catch(Exception e) {
                }
                taskRunner = null;
            }

            // let the peer know that we are disconnecting after attempting
            // to cleanly shutdown the async tasks so that this is the last
            // command it see's.
            try {
                peer.transportListener.onCommand(new ShutdownInfo());
            } catch (Exception ignore) {
            }

            // let any requests pending a response see an exception
            try {
                peer.transportListener.onException(new TransportDisposedIOException("peer (" + this + ") stopped."));
            } catch (Exception ignore) {
            }

            // shutdown task runner factory
            if (taskRunnerFactory != null) {
                taskRunnerFactory.shutdownNow();
                taskRunnerFactory = null;
            }
        }
    }

    protected void wakeup() {
        if (async && started.get()) {
            try {
                getTaskRunner().wakeup();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (TransportDisposedIOException e) {
            }
        }
    }

    /**
     * @see org.apache.activemq.thread.Task#iterate()
     */
    public boolean iterate() {

        final TransportListener tl = transportListener;

        LinkedBlockingQueue<Object> mq;
        try {
            mq = getMessageQueue();
        } catch (TransportDisposedIOException e) {
            return false;
        }

        Object command = mq.poll();
        if (command != null && !disposed.get()) {
            if( command == DISCONNECT ) {
                tl.onException(new TransportDisposedIOException("Peer (" + peer.toString() + ") disposed."));
            } else {
                tl.onCommand(command);
            }
            return !mq.isEmpty() && !disposed.get();
        } else {
            if(disposed.get()) {
                mq.clear();
            }
            return false;
        }
    }

    public void setTransportListener(TransportListener commandListener) {
        this.transportListener = commandListener;
    }

    public void setMessageQueue(LinkedBlockingQueue<Object> asyncQueue) {
        synchronized (this) {
            if (messageQueue == null) {
                messageQueue = asyncQueue;
            }
        }
    }

    public LinkedBlockingQueue<Object> getMessageQueue() throws TransportDisposedIOException {
        LinkedBlockingQueue<Object> result = messageQueue;
        if (result == null) {
            synchronized (this) {
                result = messageQueue;
                if (result == null) {
                    if (disposed.get()) {
                        throw new TransportDisposedIOException("The Transport has been disposed");
                    }

                    messageQueue = result = new LinkedBlockingQueue<Object>(this.asyncQueueDepth);
                }
            }
        }
        return result;
    }

    protected TaskRunner getTaskRunner() throws TransportDisposedIOException {
        TaskRunner result = taskRunner;
        if (result == null) {
            synchronized (this) {
                result = taskRunner;
                if (result == null) {
                    if (disposed.get()) {
                        throw new TransportDisposedIOException("The Transport has been disposed");
                    }

                    String name = "ActiveMQ VMTransport: " + toString();
                    if (taskRunnerFactory == null) {
                        taskRunnerFactory = new TaskRunnerFactory(name);
                        taskRunnerFactory.init();
                    }
                    taskRunner = result = taskRunnerFactory.createTaskRunner(this, name);
                }
            }
        }
        return result;
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

    public boolean isFaultTolerant() {
        return false;
    }

    public boolean isDisposed() {
        return disposed.get();
    }

    public boolean isConnected() {
        return !disposed.get();
    }

    public void reconnect(URI uri) throws IOException {
        throw new IOException("Transport reconnect is not supported");
    }

    public boolean isReconnectSupported() {
        return false;
    }

    public boolean isUpdateURIsSupported() {
        return false;
    }

    public void updateURIs(boolean reblance,URI[] uris) throws IOException {
        throw new IOException("URI update feature not supported");
    }

    public int getReceiveCounter() {
        return receiveCounter;
    }
}
