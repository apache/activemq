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
package org.apache.activemq.transport.fanout;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.Response;
import org.apache.activemq.state.ConnectionStateTracker;
import org.apache.activemq.thread.DefaultThreadPools;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.transport.CompositeTransport;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Transport that fans out a connection to multiple brokers.
 * 
 * @version $Revision$
 */
public class FanoutTransport implements CompositeTransport {

    private static final Log log = LogFactory.getLog(FanoutTransport.class);

    private TransportListener transportListener;
    private boolean disposed;

    private final Object reconnectMutex = new Object();
    private final ConnectionStateTracker stateTracker = new ConnectionStateTracker();
    private final ConcurrentHashMap requestMap = new ConcurrentHashMap();

    private final TaskRunner reconnectTask;
    private boolean started;

    private ArrayList transports = new ArrayList();
    private int connectedCount = 0;

    private int minAckCount = 2;

    private long initialReconnectDelay = 10;
    private long maxReconnectDelay = 1000 * 30;
    private long backOffMultiplier = 2;
    private boolean useExponentialBackOff = true;
    private int maxReconnectAttempts;
    private Exception connectionFailure;
    private FanoutTransportHandler primary;

    static class RequestCounter {

        final Command command;
        final AtomicInteger ackCount;

        RequestCounter(Command command, int count) {
            this.command = command;
            this.ackCount = new AtomicInteger(count);
        }

        public String toString() {
            return command.getCommandId() + "=" + ackCount.get();
        }
    }

    class FanoutTransportHandler extends DefaultTransportListener {

        private final URI uri;
        private Transport transport;

        private int connectFailures;
        private long reconnectDelay = initialReconnectDelay;
        private long reconnectDate;

        public FanoutTransportHandler(URI uri) {
            this.uri = uri;
        }

        public void onCommand(Object o) {
            Command command = (Command)o;
            if (command.isResponse()) {
                Integer id = new Integer(((Response)command).getCorrelationId());
                RequestCounter rc = (RequestCounter)requestMap.get(id);
                if (rc != null) {
                    if (rc.ackCount.decrementAndGet() <= 0) {
                        requestMap.remove(id);
                        transportListenerOnCommand(command);
                    }
                } else {
                    transportListenerOnCommand(command);
                }
            } else {
                transportListenerOnCommand(command);
            }
        }

        public void onException(IOException error) {
            try {
                synchronized (reconnectMutex) {
                    if (transport == null)
                        return;

                    log.debug("Transport failed, starting up reconnect task", error);

                    ServiceSupport.dispose(transport);
                    transport = null;
                    connectedCount--;
                    if (primary == this) {
                        primary = null;
                    }
                    reconnectTask.wakeup();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (transportListener != null) {
                    transportListener.onException(new InterruptedIOException());
                }
            }
        }
    }

    public FanoutTransport() throws InterruptedIOException {
        // Setup a task that is used to reconnect the a connection async.
        reconnectTask = DefaultThreadPools.getDefaultTaskRunnerFactory().createTaskRunner(new Task() {
            public boolean iterate() {
                return doConnect();
            }
        }, "ActiveMQ Fanout Worker: " + System.identityHashCode(this));
    }

    /**
     * @return
     */
    private boolean doConnect() {
        long closestReconnectDate = 0;
        synchronized (reconnectMutex) {

            if (disposed || connectionFailure != null) {
                reconnectMutex.notifyAll();
            }

            if (transports.size() == connectedCount || disposed || connectionFailure != null) {
                return false;
            } else {

                if (transports.isEmpty()) {
                    // connectionFailure = new IOException("No uris available to
                    // connect to.");
                } else {

                    // Try to connect them up.
                    Iterator iter = transports.iterator();
                    for (int i = 0; iter.hasNext() && !disposed; i++) {

                        long now = System.currentTimeMillis();

                        FanoutTransportHandler fanoutHandler = (FanoutTransportHandler)iter.next();
                        if (fanoutHandler.transport != null) {
                            continue;
                        }

                        // Are we waiting a little to try to reconnect this one?
                        if (fanoutHandler.reconnectDate != 0 && fanoutHandler.reconnectDate > now) {
                            if (closestReconnectDate == 0 || fanoutHandler.reconnectDate < closestReconnectDate) {
                                closestReconnectDate = fanoutHandler.reconnectDate;
                            }
                            continue;
                        }

                        URI uri = fanoutHandler.uri;
                        try {
                            log.debug("Stopped: " + this);
                            log.debug("Attempting connect to: " + uri);
                            Transport t = TransportFactory.compositeConnect(uri);
                            log.debug("Connection established");
                            fanoutHandler.transport = t;
                            fanoutHandler.reconnectDelay = 10;
                            fanoutHandler.connectFailures = 0;
                            if (primary == null) {
                                primary = fanoutHandler;
                            }
                            t.setTransportListener(fanoutHandler);
                            connectedCount++;
                            if (started) {
                                restoreTransport(fanoutHandler);
                            }
                        } catch (Exception e) {
                            log.debug("Connect fail to: " + uri + ", reason: " + e);

                            if (maxReconnectAttempts > 0 && ++fanoutHandler.connectFailures >= maxReconnectAttempts) {
                                log.error("Failed to connect to transport after: " + fanoutHandler.connectFailures + " attempt(s)");
                                connectionFailure = e;
                                reconnectMutex.notifyAll();
                                return false;
                            } else {

                                if (useExponentialBackOff) {
                                    // Exponential increment of reconnect delay.
                                    fanoutHandler.reconnectDelay *= backOffMultiplier;
                                    if (fanoutHandler.reconnectDelay > maxReconnectDelay)
                                        fanoutHandler.reconnectDelay = maxReconnectDelay;
                                }

                                fanoutHandler.reconnectDate = now + fanoutHandler.reconnectDelay;

                                if (closestReconnectDate == 0 || fanoutHandler.reconnectDate < closestReconnectDate) {
                                    closestReconnectDate = fanoutHandler.reconnectDate;
                                }
                            }
                        }
                    }
                    if (transports.size() == connectedCount || disposed) {
                        reconnectMutex.notifyAll();
                        return false;
                    }

                }
            }

        }

        try {
            long reconnectDelay = closestReconnectDate - System.currentTimeMillis();
            if (reconnectDelay > 0) {
                log.debug("Waiting " + reconnectDelay + " ms before attempting connection. ");
                Thread.sleep(reconnectDelay);
            }
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
        }
        return true;
    }

    public void start() throws Exception {
        synchronized (reconnectMutex) {
            log.debug("Started.");
            if (started)
                return;
            started = true;
            for (Iterator iter = transports.iterator(); iter.hasNext();) {
                FanoutTransportHandler th = (FanoutTransportHandler)iter.next();
                if (th.transport != null) {
                    restoreTransport(th);
                }
            }
        }
    }

    public void stop() throws Exception {
        synchronized (reconnectMutex) {
            ServiceStopper ss = new ServiceStopper();

            if (!started)
                return;
            started = false;
            disposed = true;

            for (Iterator iter = transports.iterator(); iter.hasNext();) {
                FanoutTransportHandler th = (FanoutTransportHandler)iter.next();
                if (th.transport != null) {
                    ss.stop(th.transport);
                }
            }

            log.debug("Stopped: " + this);
            ss.throwFirstException();
        }
        reconnectTask.shutdown();
    }

    public long getInitialReconnectDelay() {
        return initialReconnectDelay;
    }

    public void setInitialReconnectDelay(long initialReconnectDelay) {
        this.initialReconnectDelay = initialReconnectDelay;
    }

    public long getMaxReconnectDelay() {
        return maxReconnectDelay;
    }

    public void setMaxReconnectDelay(long maxReconnectDelay) {
        this.maxReconnectDelay = maxReconnectDelay;
    }

    public long getReconnectDelayExponent() {
        return backOffMultiplier;
    }

    public void setReconnectDelayExponent(long reconnectDelayExponent) {
        this.backOffMultiplier = reconnectDelayExponent;
    }

    public int getMaxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    public void oneway(Object o) throws IOException {
        final Command command = (Command)o;
        try {
            synchronized (reconnectMutex) {

                // If it was a request and it was not being tracked by
                // the state tracker,
                // then hold it in the requestMap so that we can replay
                // it later.
                boolean fanout = isFanoutCommand(command);
                if (stateTracker.track(command) == null && command.isResponseRequired()) {
                    int size = fanout ? minAckCount : 1;
                    requestMap.put(new Integer(command.getCommandId()), new RequestCounter(command, size));
                }

                // Wait for transport to be connected.
                while (connectedCount != minAckCount && !disposed && connectionFailure == null) {
                    log.debug("Waiting for at least " + minAckCount + " transports to be connected.");
                    reconnectMutex.wait(1000);
                }

                // Still not fully connected.
                if (connectedCount != minAckCount) {

                    Exception error;

                    // Throw the right kind of error..
                    if (disposed) {
                        error = new IOException("Transport disposed.");
                    } else if (connectionFailure != null) {
                        error = connectionFailure;
                    } else {
                        error = new IOException("Unexpected failure.");
                    }

                    if (error instanceof IOException)
                        throw (IOException)error;
                    throw IOExceptionSupport.create(error);
                }

                // Send the message.
                if (fanout) {
                    for (Iterator iter = transports.iterator(); iter.hasNext();) {
                        FanoutTransportHandler th = (FanoutTransportHandler)iter.next();
                        if (th.transport != null) {
                            try {
                                th.transport.oneway(command);
                            } catch (IOException e) {
                                log.debug("Send attempt: failed.");
                                th.onException(e);
                            }
                        }
                    }
                } else {
                    try {
                        primary.transport.oneway(command);
                    } catch (IOException e) {
                        log.debug("Send attempt: failed.");
                        primary.onException(e);
                    }
                }

            }
        } catch (InterruptedException e) {
            // Some one may be trying to stop our thread.
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
    }

    /**
     * @param command
     * @return
     */
    private boolean isFanoutCommand(Command command) {
        if (command.isMessage()) {
            return ((Message)command).getDestination().isTopic();
        }
        if (command.getDataStructureType() == ConsumerInfo.DATA_STRUCTURE_TYPE) {
            return false;
        }
        return true;
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

    public void reconnect() {
        log.debug("Waking up reconnect task");
        try {
            reconnectTask.wakeup();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public TransportListener getTransportListener() {
        return transportListener;
    }

    public void setTransportListener(TransportListener commandListener) {
        this.transportListener = commandListener;
    }

    public Object narrow(Class target) {

        if (target.isAssignableFrom(getClass())) {
            return this;
        }

        synchronized (reconnectMutex) {
            for (Iterator iter = transports.iterator(); iter.hasNext();) {
                FanoutTransportHandler th = (FanoutTransportHandler)iter.next();
                if (th.transport != null) {
                    Object rc = th.transport.narrow(target);
                    if (rc != null)
                        return rc;
                }
            }
        }

        return null;

    }

    protected void restoreTransport(FanoutTransportHandler th) throws Exception, IOException {
        th.transport.start();
        stateTracker.setRestoreConsumers(th.transport == primary);
        stateTracker.restore(th.transport);
        for (Iterator iter2 = requestMap.values().iterator(); iter2.hasNext();) {
            RequestCounter rc = (RequestCounter)iter2.next();
            th.transport.oneway(rc.command);
        }
    }

    public void add(URI uris[]) {

        synchronized (reconnectMutex) {
            for (int i = 0; i < uris.length; i++) {
                URI uri = uris[i];

                boolean match = false;
                for (Iterator iter = transports.iterator(); iter.hasNext();) {
                    FanoutTransportHandler th = (FanoutTransportHandler)iter.next();
                    if (th.uri.equals(uri)) {
                        match = true;
                        break;
                    }
                }
                if (!match) {
                    FanoutTransportHandler th = new FanoutTransportHandler(uri);
                    transports.add(th);
                    reconnect();
                }
            }
        }

    }

    public void remove(URI uris[]) {

        synchronized (reconnectMutex) {
            for (int i = 0; i < uris.length; i++) {
                URI uri = uris[i];

                boolean match = false;
                for (Iterator iter = transports.iterator(); iter.hasNext();) {
                    FanoutTransportHandler th = (FanoutTransportHandler)iter.next();
                    if (th.uri.equals(uri)) {
                        if (th.transport != null) {
                            ServiceSupport.dispose(th.transport);
                            connectedCount--;
                        }
                        iter.remove();
                        break;
                    }
                }
            }
        }

    }

    public String getRemoteAddress() {
        if (primary != null) {
            if (primary.transport != null) {
                return primary.transport.getRemoteAddress();
            }
        }
        return null;
    }

    protected void transportListenerOnCommand(Command command) {
        if (transportListener != null) {
            transportListener.onCommand(command);
        }
    }

    public boolean isFaultTolerant() {
        return true;
    }
}
