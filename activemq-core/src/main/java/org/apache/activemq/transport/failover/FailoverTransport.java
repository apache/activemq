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
package org.apache.activemq.transport.failover;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.state.ConnectionStateTracker;
import org.apache.activemq.state.Tracked;
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
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Transport that is made reliable by being able to fail over to another
 * transport when a transport failure is detected.
 */
public class FailoverTransport implements CompositeTransport {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverTransport.class);
    private static final int DEFAULT_INITIAL_RECONNECT_DELAY = 10;
    private TransportListener transportListener;
    private boolean disposed;
    private boolean connected;
    private final CopyOnWriteArrayList<URI> uris = new CopyOnWriteArrayList<URI>();
    private final CopyOnWriteArrayList<URI> updated = new CopyOnWriteArrayList<URI>();

    private final Object reconnectMutex = new Object();
    private final Object backupMutex = new Object();
    private final Object sleepMutex = new Object();
    private final Object listenerMutex = new Object();
    private final ConnectionStateTracker stateTracker = new ConnectionStateTracker();
    private final Map<Integer, Command> requestMap = new LinkedHashMap<Integer, Command>();

    private URI connectedTransportURI;
    private URI failedConnectTransportURI;
    private final AtomicReference<Transport> connectedTransport = new AtomicReference<Transport>();
    private final TaskRunner reconnectTask;
    private boolean started;
    private boolean initialized;
    private long initialReconnectDelay = DEFAULT_INITIAL_RECONNECT_DELAY;
    private long maxReconnectDelay = 1000 * 30;
    private double backOffMultiplier = 2d;
    private long timeout = -1;
    private boolean useExponentialBackOff = true;
    private boolean randomize = true;
    private int maxReconnectAttempts;
    private int startupMaxReconnectAttempts;
    private int connectFailures;
    private long reconnectDelay = DEFAULT_INITIAL_RECONNECT_DELAY;
    private Exception connectionFailure;
    private boolean firstConnection = true;
    // optionally always have a backup created
    private boolean backup = false;
    private final List<BackupTransport> backups = new CopyOnWriteArrayList<BackupTransport>();
    private int backupPoolSize = 1;
    private boolean trackMessages = false;
    private boolean trackTransactionProducers = true;
    private int maxCacheSize = 128 * 1024;
    private final TransportListener disposedListener = new DefaultTransportListener() {
    };
    //private boolean connectionInterruptProcessingComplete;

    private final TransportListener myTransportListener = createTransportListener();
    private boolean updateURIsSupported = true;
    private boolean reconnectSupported = true;
    // remember for reconnect thread
    private SslContext brokerSslContext;
    private String updateURIsURL = null;
    private boolean rebalanceUpdateURIs = true;
    private boolean doRebalance = false;

    public FailoverTransport() throws InterruptedIOException {
        brokerSslContext = SslContext.getCurrentSslContext();
        stateTracker.setTrackTransactions(true);
        // Setup a task that is used to reconnect the a connection async.
        reconnectTask = DefaultThreadPools.getDefaultTaskRunnerFactory().createTaskRunner(new Task() {
            public boolean iterate() {
                boolean result = false;
                boolean buildBackup = true;
                synchronized (backupMutex) {
                    if ((connectedTransport.get() == null || doRebalance) && !disposed) {
                        result = doReconnect();
                        buildBackup = false;
                    }
                }
                if (buildBackup) {
                    buildBackups();
                } else {
                    // build backups on the next iteration
                    buildBackup = true;
                    try {
                        reconnectTask.wakeup();
                    } catch (InterruptedException e) {
                        LOG.debug("Reconnect task has been interrupted.", e);
                    }
                }
                return result;
            }

        }, "ActiveMQ Failover Worker: " + System.identityHashCode(this));
    }

    TransportListener createTransportListener() {
        return new TransportListener() {
            public void onCommand(Object o) {
                Command command = (Command) o;
                if (command == null) {
                    return;
                }
                if (command.isResponse()) {
                    Object object = null;
                    synchronized (requestMap) {
                        object = requestMap.remove(Integer.valueOf(((Response) command).getCorrelationId()));
                    }
                    if (object != null && object.getClass() == Tracked.class) {
                        ((Tracked) object).onResponses(command);
                    }
                }
                if (!initialized) {
                    initialized = true;
                }

                if (command.isConnectionControl()) {
                    handleConnectionControl((ConnectionControl) command);
                }
                if (transportListener != null) {
                    transportListener.onCommand(command);
                }
            }

            public void onException(IOException error) {
                try {
                    handleTransportFailure(error);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    transportListener.onException(new InterruptedIOException());
                }
            }

            public void transportInterupted() {
                if (transportListener != null) {
                    transportListener.transportInterupted();
                }
            }

            public void transportResumed() {
                if (transportListener != null) {
                    transportListener.transportResumed();
                }
            }
        };
    }

    public final void disposeTransport(Transport transport) {
        transport.setTransportListener(disposedListener);
        ServiceSupport.dispose(transport);
    }

    public final void handleTransportFailure(IOException e) throws InterruptedException {
        if (LOG.isTraceEnabled()) {
            LOG.trace(this + " handleTransportFailure: " + e);
        }
        Transport transport = connectedTransport.getAndSet(null);
        if (transport == null) {
            // sync with possible in progress reconnect
            synchronized (reconnectMutex) {
                transport = connectedTransport.getAndSet(null);
            }
        }
        if (transport != null) {

            disposeTransport(transport);

            boolean reconnectOk = false;
            synchronized (reconnectMutex) {
                if (started) {
                    LOG.warn("Transport (" + transport.getRemoteAddress() + ") failed to " + connectedTransportURI
                            + " , attempting to automatically reconnect due to: " + e);
                    LOG.debug("Transport failed with the following exception:", e);
                    reconnectOk = true;
                }
                initialized = false;
                failedConnectTransportURI = connectedTransportURI;
                connectedTransportURI = null;
                connected = false;

                // notify before any reconnect attempt so ack state can be whacked
                if (transportListener != null) {
                    transportListener.transportInterupted();
                }

                if (reconnectOk) {
                    reconnectTask.wakeup();
                }
            }
        }
    }

    public final void handleConnectionControl(ConnectionControl control) {
        String reconnectStr = control.getReconnectTo();
        if (reconnectStr != null) {
            reconnectStr = reconnectStr.trim();
            if (reconnectStr.length() > 0) {
                try {
                    URI uri = new URI(reconnectStr);
                    if (isReconnectSupported()) {
                        reconnect(uri);
                        LOG.info("Reconnected to: " + uri);
                    }
                } catch (Exception e) {
                    LOG.error("Failed to handle ConnectionControl reconnect to " + reconnectStr, e);
                }
            }
        }
        processNewTransports(control.isRebalanceConnection(), control.getConnectedBrokers());
    }

    private final void processNewTransports(boolean rebalance, String newTransports) {
        if (newTransports != null) {
            newTransports = newTransports.trim();
            if (newTransports.length() > 0 && isUpdateURIsSupported()) {
                List<URI> list = new ArrayList<URI>();
                StringTokenizer tokenizer = new StringTokenizer(newTransports, ",");
                while (tokenizer.hasMoreTokens()) {
                    String str = tokenizer.nextToken();
                    try {
                        URI uri = new URI(str);
                        list.add(uri);
                    } catch (Exception e) {
                        LOG.error("Failed to parse broker address: " + str, e);
                    }
                }
                if (list.isEmpty() == false) {
                    try {
                        updateURIs(rebalance, list.toArray(new URI[list.size()]));
                    } catch (IOException e) {
                        LOG.error("Failed to update transport URI's from: " + newTransports, e);
                    }
                }
            }
        }
    }

    public void start() throws Exception {
        synchronized (reconnectMutex) {
            LOG.debug("Started.");
            if (started) {
                return;
            }
            started = true;
            stateTracker.setMaxCacheSize(getMaxCacheSize());
            stateTracker.setTrackMessages(isTrackMessages());
            stateTracker.setTrackTransactionProducers(isTrackTransactionProducers());
            if (connectedTransport.get() != null) {
                stateTracker.restore(connectedTransport.get());
            } else {
                reconnect(false);
            }
        }
    }

    public void stop() throws Exception {
        Transport transportToStop = null;
        synchronized (reconnectMutex) {
            LOG.debug("Stopped.");
            if (!started) {
                return;
            }
            started = false;
            disposed = true;
            connected = false;
            for (BackupTransport t : backups) {
                t.setDisposed(true);
            }
            backups.clear();

            if (connectedTransport.get() != null) {
                transportToStop = connectedTransport.getAndSet(null);
            }
            reconnectMutex.notifyAll();
        }
        synchronized (sleepMutex) {
            sleepMutex.notifyAll();
        }
        reconnectTask.shutdown();
        if (transportToStop != null) {
            transportToStop.stop();
        }
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

    public long getReconnectDelay() {
        return reconnectDelay;
    }

    public void setReconnectDelay(long reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }

    public double getReconnectDelayExponent() {
        return backOffMultiplier;
    }

    public void setReconnectDelayExponent(double reconnectDelayExponent) {
        this.backOffMultiplier = reconnectDelayExponent;
    }

    public Transport getConnectedTransport() {
        return connectedTransport.get();
    }

    public URI getConnectedTransportURI() {
        return connectedTransportURI;
    }

    public int getMaxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    public int getStartupMaxReconnectAttempts() {
        return this.startupMaxReconnectAttempts;
    }

    public void setStartupMaxReconnectAttempts(int startupMaxReconnectAttempts) {
        this.startupMaxReconnectAttempts = startupMaxReconnectAttempts;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Returns the randomize.
     */
    public boolean isRandomize() {
        return randomize;
    }

    /**
     * @param randomize The randomize to set.
     */
    public void setRandomize(boolean randomize) {
        this.randomize = randomize;
    }

    public boolean isBackup() {
        return backup;
    }

    public void setBackup(boolean backup) {
        this.backup = backup;
    }

    public int getBackupPoolSize() {
        return backupPoolSize;
    }

    public void setBackupPoolSize(int backupPoolSize) {
        this.backupPoolSize = backupPoolSize;
    }

    public boolean isTrackMessages() {
        return trackMessages;
    }

    public void setTrackMessages(boolean trackMessages) {
        this.trackMessages = trackMessages;
    }

    public boolean isTrackTransactionProducers() {
        return this.trackTransactionProducers;
    }

    public void setTrackTransactionProducers(boolean trackTransactionProducers) {
        this.trackTransactionProducers = trackTransactionProducers;
    }

    public int getMaxCacheSize() {
        return maxCacheSize;
    }

    public void setMaxCacheSize(int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    /**
     * @return Returns true if the command is one sent when a connection is
     *         being closed.
     */
    private boolean isShutdownCommand(Command command) {
        return (command != null && (command.isShutdownInfo() || command instanceof RemoveInfo));
    }

    public void oneway(Object o) throws IOException {

        Command command = (Command) o;
        Exception error = null;
        try {

            synchronized (reconnectMutex) {

                if (isShutdownCommand(command) && connectedTransport.get() == null) {
                    if (command.isShutdownInfo()) {
                        // Skipping send of ShutdownInfo command when not
                        // connected.
                        return;
                    }
                    if (command instanceof RemoveInfo || command.isMessageAck()) {
                        // Simulate response to RemoveInfo command or ack (as it
                        // will be stale)
                        stateTracker.track(command);
                        Response response = new Response();
                        response.setCorrelationId(command.getCommandId());
                        myTransportListener.onCommand(response);
                        return;
                    }
                }
                // Keep trying until the message is sent.
                for (int i = 0; !disposed; i++) {
                    try {

                        // Wait for transport to be connected.
                        Transport transport = connectedTransport.get();
                        long start = System.currentTimeMillis();
                        boolean timedout = false;
                        while (transport == null && !disposed && connectionFailure == null
                                && !Thread.currentThread().isInterrupted()) {
                            LOG.trace("Waiting for transport to reconnect..: " + command);
                            long end = System.currentTimeMillis();
                            if (timeout > 0 && (end - start > timeout)) {
                                timedout = true;
                                LOG.info("Failover timed out after " + (end - start) + "ms");
                                break;
                            }
                            try {
                                reconnectMutex.wait(100);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                LOG.debug("Interupted: " + e, e);
                            }
                            transport = connectedTransport.get();
                        }

                        if (transport == null) {
                            // Previous loop may have exited due to use being
                            // disposed.
                            if (disposed) {
                                error = new IOException("Transport disposed.");
                            } else if (connectionFailure != null) {
                                error = connectionFailure;
                            } else if (timedout == true) {
                                error = new IOException("Failover timeout of " + timeout + " ms reached.");
                            } else {
                                error = new IOException("Unexpected failure.");
                            }
                            break;
                        }

                        // If it was a request and it was not being tracked by
                        // the state tracker,
                        // then hold it in the requestMap so that we can replay
                        // it later.
                        Tracked tracked = stateTracker.track(command);
                        synchronized (requestMap) {
                            if (tracked != null && tracked.isWaitingForResponse()) {
                                requestMap.put(Integer.valueOf(command.getCommandId()), tracked);
                            } else if (tracked == null && command.isResponseRequired()) {
                                requestMap.put(Integer.valueOf(command.getCommandId()), command);
                            }
                        }

                        // Send the message.
                        try {
                            transport.oneway(command);
                            stateTracker.trackBack(command);
                        } catch (IOException e) {

                            // If the command was not tracked.. we will retry in
                            // this method
                            if (tracked == null) {

                                // since we will retry in this method.. take it
                                // out of the request
                                // map so that it is not sent 2 times on
                                // recovery
                                if (command.isResponseRequired()) {
                                    requestMap.remove(Integer.valueOf(command.getCommandId()));
                                }

                                // Rethrow the exception so it will handled by
                                // the outer catch
                                throw e;
                            }
                        }

                        return;

                    } catch (IOException e) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Send oneway attempt: " + i + " failed for command:" + command);
                        }
                        handleTransportFailure(e);
                    }
                }
            }
        } catch (InterruptedException e) {
            // Some one may be trying to stop our thread.
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
        if (!disposed) {
            if (error != null) {
                if (error instanceof IOException) {
                    throw (IOException) error;
                }
                throw IOExceptionSupport.create(error);
            }
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

    public void add(boolean rebalance, URI u[]) {
        boolean newURI = false;
        for (URI uri : u) {
            if (!contains(uri)) {
                uris.add(uri);
                newURI = true;
            }
        }
        if (newURI) {
            reconnect(rebalance);
        }
    }

    public void remove(boolean rebalance, URI u[]) {
        for (URI uri : u) {
            uris.remove(uri);
        }
        // rebalance is automatic if any connected to removed/stopped broker
    }

    public void add(boolean rebalance, String u) {
        try {
            URI newURI = new URI(u);
            if (contains(newURI) == false) {
                uris.add(newURI);
                reconnect(rebalance);
            }

        } catch (Exception e) {
            LOG.error("Failed to parse URI: " + u);
        }
    }

    public void reconnect(boolean rebalance) {
        synchronized (reconnectMutex) {
            if (started) {
                if (rebalance) {
                    doRebalance = true;
                }
                LOG.debug("Waking up reconnect task");
                try {
                    reconnectTask.wakeup();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                LOG.debug("Reconnect was triggered but transport is not started yet. Wait for start to connect the transport.");
            }
        }
    }

    private List<URI> getConnectList() {
        ArrayList<URI> l = new ArrayList<URI>(uris);
        boolean removed = false;
        if (failedConnectTransportURI != null) {
            removed = l.remove(failedConnectTransportURI);
        }
        if (randomize) {
            // Randomly, reorder the list by random swapping
            for (int i = 0; i < l.size(); i++) {
                int p = (int) (Math.random() * 100 % l.size());
                URI t = l.get(p);
                l.set(p, l.get(i));
                l.set(i, t);
            }
        }
        if (removed) {
            l.add(failedConnectTransportURI);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("urlList connectionList:" + l + ", from: " + uris);
        }
        return l;
    }

    public TransportListener getTransportListener() {
        return transportListener;
    }

    public void setTransportListener(TransportListener commandListener) {
        synchronized (listenerMutex) {
            this.transportListener = commandListener;
            listenerMutex.notifyAll();
        }
    }

    public <T> T narrow(Class<T> target) {

        if (target.isAssignableFrom(getClass())) {
            return target.cast(this);
        }
        Transport transport = connectedTransport.get();
        if (transport != null) {
            return transport.narrow(target);
        }
        return null;

    }

    protected void restoreTransport(Transport t) throws Exception, IOException {
        t.start();
        // send information to the broker - informing it we are an ft client
        ConnectionControl cc = new ConnectionControl();
        cc.setFaultTolerant(true);
        t.oneway(cc);
        stateTracker.restore(t);
        Map<Integer, Command> tmpMap = null;
        synchronized (requestMap) {
            tmpMap = new LinkedHashMap<Integer, Command>(requestMap);
        }
        for (Command command : tmpMap.values()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("restore requestMap, replay: " + command);
            }
            t.oneway(command);
        }
    }

    public boolean isUseExponentialBackOff() {
        return useExponentialBackOff;
    }

    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }

    @Override
    public String toString() {
        return connectedTransportURI == null ? "unconnected" : connectedTransportURI.toString();
    }

    public String getRemoteAddress() {
        Transport transport = connectedTransport.get();
        if (transport != null) {
            return transport.getRemoteAddress();
        }
        return null;
    }

    public boolean isFaultTolerant() {
        return true;
    }

    private void doUpdateURIsFromDisk() {

        // If updateURIsURL is specified, read the file and add any new
        // transport URI's to this FailOverTransport.
        // Note: Could track file timestamp to avoid unnecessary reading.
        String fileURL = getUpdateURIsURL();
        if (fileURL != null) {
            BufferedReader in = null;
            String newUris = null;
            StringBuffer buffer = new StringBuffer();

            try {
                in = new BufferedReader(getURLStream(fileURL));
                while (true) {
                    String line = in.readLine();
                    if (line == null) {
                        break;
                    }
                    buffer.append(line);
                }
                newUris = buffer.toString();
            } catch (IOException ioe) {
                LOG.error("Failed to read updateURIsURL: " + fileURL, ioe);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException ioe) {
                        // ignore
                    }
                }
            }

            processNewTransports(isRebalanceUpdateURIs(), newUris);
        }
    }

    final boolean doReconnect() {
        Exception failure = null;
        synchronized (reconnectMutex) {

            // First ensure we are up to date.
            doUpdateURIsFromDisk();

            if (disposed || connectionFailure != null) {
                reconnectMutex.notifyAll();
            }

            if ((connectedTransport.get() != null && !doRebalance) || disposed || connectionFailure != null) {
                return false;
            } else {
                List<URI> connectList = getConnectList();
                if (connectList.isEmpty()) {
                    failure = new IOException("No uris available to connect to.");
                } else {
                    if (doRebalance) {
                        if (connectList.get(0).equals(connectedTransportURI)) {
                            // already connected to first in the list, no need to rebalance
                            doRebalance = false;
                            return false;
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Doing rebalance from: " + connectedTransportURI + " to " + connectList);
                            }
                            try {
                                Transport transport = this.connectedTransport.getAndSet(null);
                                if (transport != null) {
                                    disposeTransport(transport);
                                }
                            } catch (Exception e) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Caught an exception stopping existing transport for rebalance", e);
                                }
                            }
                        }
                        doRebalance = false;
                    }
                    if (!useExponentialBackOff || reconnectDelay == DEFAULT_INITIAL_RECONNECT_DELAY) {
                        reconnectDelay = initialReconnectDelay;
                    }
                    synchronized (backupMutex) {
                        if (backup && !backups.isEmpty()) {
                            BackupTransport bt = backups.remove(0);
                            Transport t = bt.getTransport();
                            URI uri = bt.getUri();
                            t.setTransportListener(myTransportListener);
                            try {
                                if (started) {
                                    restoreTransport(t);
                                }
                                reconnectDelay = initialReconnectDelay;
                                failedConnectTransportURI = null;
                                connectedTransportURI = uri;
                                connectedTransport.set(t);
                                reconnectMutex.notifyAll();
                                connectFailures = 0;
                                LOG.info("Successfully reconnected to backup " + uri);
                                return false;
                            } catch (Exception e) {
                                LOG.debug("Backup transport failed", e);
                            }
                        }
                    }

                    // Sleep for the reconnectDelay
                    if (!firstConnection && (reconnectDelay > 0) && !disposed) {
                        synchronized (sleepMutex) {
                            LOG.debug("Waiting " + reconnectDelay + " ms before attempting connection. ");
                            try {
                                sleepMutex.wait(reconnectDelay);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }

                    Iterator<URI> iter = connectList.iterator();
                    while (iter.hasNext() && connectedTransport.get() == null && !disposed) {

                        URI uri = iter.next();
                        Transport t = null;
                        try {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Attempting connect to: " + uri);
                            }
                            SslContext.setCurrentSslContext(brokerSslContext);
                            t = TransportFactory.compositeConnect(uri);
                            t.setTransportListener(myTransportListener);
                            t.start();

                            if (started) {
                                restoreTransport(t);
                            }

                            LOG.debug("Connection established");
                            reconnectDelay = initialReconnectDelay;
                            connectedTransportURI = uri;
                            connectedTransport.set(t);
                            reconnectMutex.notifyAll();
                            connectFailures = 0;
                            // Make sure on initial startup, that the
                            // transportListener
                            // has been initialized for this instance.
                            synchronized (listenerMutex) {
                                if (transportListener == null) {
                                    try {
                                        // if it isn't set after 2secs - it
                                        // probably never will be
                                        listenerMutex.wait(2000);
                                    } catch (InterruptedException ex) {
                                    }
                                }
                            }
                            if (transportListener != null) {
                                transportListener.transportResumed();
                            } else {
                                LOG.debug("transport resumed by transport listener not set");
                            }
                            if (firstConnection) {
                                firstConnection = false;
                                LOG.info("Successfully connected to " + uri);
                            } else {
                                LOG.info("Successfully reconnected to " + uri);
                            }
                            connected = true;
                            return false;
                        } catch (Exception e) {
                            failure = e;
                            LOG.debug("Connect fail to: " + uri + ", reason: " + e);
                            if (t != null) {
                                try {
                                    t.stop();
                                } catch (Exception ee) {
                                    LOG.debug("Stop of failed transport: " + t + " failed with reason: " + ee);
                                }
                            }
                        } finally {
                            SslContext.setCurrentSslContext(null);
                        }
                    }
                }
            }
            int reconnectAttempts = 0;
            if (firstConnection) {
                if (this.startupMaxReconnectAttempts != 0) {
                    reconnectAttempts = this.startupMaxReconnectAttempts;
                }
            }
            if (reconnectAttempts == 0) {
                reconnectAttempts = this.maxReconnectAttempts;
            }
            if (reconnectAttempts > 0 && ++connectFailures >= reconnectAttempts) {
                LOG.error("Failed to connect to transport after: " + connectFailures + " attempt(s)");
                connectionFailure = failure;

                // Make sure on initial startup, that the transportListener has
                // been initialized for this instance.
                synchronized (listenerMutex) {
                    if (transportListener == null) {
                        try {
                            listenerMutex.wait(2000);
                        } catch (InterruptedException ex) {
                        }
                    }
                }

                if (transportListener != null) {
                    if (connectionFailure instanceof IOException) {
                        transportListener.onException((IOException) connectionFailure);
                    } else {
                        transportListener.onException(IOExceptionSupport.create(connectionFailure));
                    }
                }
                reconnectMutex.notifyAll();
                return false;
            }
        }

        if (!disposed) {

            if (reconnectDelay > 0) {
                synchronized (sleepMutex) {
                    LOG.debug("Waiting " + reconnectDelay + " ms before attempting connection. ");
                    try {
                        sleepMutex.wait(reconnectDelay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            if (useExponentialBackOff) {
                // Exponential increment of reconnect delay.
                reconnectDelay *= backOffMultiplier;
                if (reconnectDelay > maxReconnectDelay) {
                    reconnectDelay = maxReconnectDelay;
                }
            }
        }

        return !disposed;
    }

    final boolean buildBackups() {
        synchronized (backupMutex) {
            if (!disposed && backup && backups.size() < backupPoolSize) {
                List<URI> connectList = getConnectList();
                // removed disposed backups
                List<BackupTransport> disposedList = new ArrayList<BackupTransport>();
                for (BackupTransport bt : backups) {
                    if (bt.isDisposed()) {
                        disposedList.add(bt);
                    }
                }
                backups.removeAll(disposedList);
                disposedList.clear();
                for (Iterator<URI> iter = connectList.iterator(); iter.hasNext() && backups.size() < backupPoolSize; ) {
                    URI uri = iter.next();
                    if (connectedTransportURI != null && !connectedTransportURI.equals(uri)) {
                        try {
                            SslContext.setCurrentSslContext(brokerSslContext);
                            BackupTransport bt = new BackupTransport(this);
                            bt.setUri(uri);
                            if (!backups.contains(bt)) {
                                Transport t = TransportFactory.compositeConnect(uri);
                                t.setTransportListener(bt);
                                t.start();
                                bt.setTransport(t);
                                backups.add(bt);
                            }
                        } catch (Exception e) {
                            LOG.debug("Failed to build backup ", e);
                        } finally {
                            SslContext.setCurrentSslContext(null);
                        }
                    }
                }
            }
        }
        return false;
    }

    public boolean isDisposed() {
        return disposed;
    }

    public boolean isConnected() {
        return connected;
    }

    public void reconnect(URI uri) throws IOException {
        add(true, new URI[]{uri});
    }

    public boolean isReconnectSupported() {
        return this.reconnectSupported;
    }

    public void setReconnectSupported(boolean value) {
        this.reconnectSupported = value;
    }

    public boolean isUpdateURIsSupported() {
        return this.updateURIsSupported;
    }

    public void setUpdateURIsSupported(boolean value) {
        this.updateURIsSupported = value;
    }

    public void updateURIs(boolean rebalance, URI[] updatedURIs) throws IOException {
        if (isUpdateURIsSupported()) {
            List<URI> copy = new ArrayList<URI>(this.updated);
            List<URI> add = new ArrayList<URI>();
            if (updatedURIs != null && updatedURIs.length > 0) {
                Set<URI> set = new HashSet<URI>();
                for (URI uri : updatedURIs) {
                    if (uri != null) {
                        set.add(uri);
                    }
                }
                for (URI uri : set) {
                    if (copy.remove(uri) == false) {
                        add.add(uri);
                    }
                }
                synchronized (reconnectMutex) {
                    this.updated.clear();
                    this.updated.addAll(add);
                    for (URI uri : copy) {
                        this.uris.remove(uri);
                    }
                    add(rebalance, add.toArray(new URI[add.size()]));
                }
            }
        }
    }

    /**
     * @return the updateURIsURL
     */
    public String getUpdateURIsURL() {
        return this.updateURIsURL;
    }

    /**
     * @param updateURIsURL the updateURIsURL to set
     */
    public void setUpdateURIsURL(String updateURIsURL) {
        this.updateURIsURL = updateURIsURL;
    }

    /**
     * @return the rebalanceUpdateURIs
     */
    public boolean isRebalanceUpdateURIs() {
        return this.rebalanceUpdateURIs;
    }

    /**
     * @param rebalanceUpdateURIs the rebalanceUpdateURIs to set
     */
    public void setRebalanceUpdateURIs(boolean rebalanceUpdateURIs) {
        this.rebalanceUpdateURIs = rebalanceUpdateURIs;
    }

    public int getReceiveCounter() {
        Transport transport = connectedTransport.get();
        if (transport == null) {
            return 0;
        }
        return transport.getReceiveCounter();
    }

    public void connectionInterruptProcessingComplete(ConnectionId connectionId) {
        synchronized (reconnectMutex) {
            stateTracker.connectionInterruptProcessingComplete(this, connectionId);
        }
    }

    public ConnectionStateTracker getStateTracker() {
        return stateTracker;
    }

    private boolean contains(URI newURI) {

        boolean result = false;
        try {
            for (URI uri : uris) {
                if (newURI.getPort() == uri.getPort()) {
                    InetAddress newAddr = InetAddress.getByName(newURI.getHost());
                    InetAddress addr = InetAddress.getByName(uri.getHost());
                    if (addr.equals(newAddr)) {
                        result = true;
                        break;
                    }
                }
            }
        } catch (IOException e) {
            result = true;
            LOG.error("Failed to verify URI " + newURI + " already known: " + e);
        }
        return result;
    }

    private InputStreamReader getURLStream(String path) throws IOException {
        InputStreamReader result = null;
        URL url = null;
        try {
            url = new URL(path);
            result = new InputStreamReader(url.openStream());
        } catch (MalformedURLException e) {
            // ignore - it could be a path to a a local file
        }
        if (result == null) {
            result = new FileReader(path);
        }
        return result;
    }
}
