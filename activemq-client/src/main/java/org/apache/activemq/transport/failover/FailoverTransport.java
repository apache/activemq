/*
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
import java.net.URISyntaxException;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.state.ConnectionStateTracker;
import org.apache.activemq.state.Tracked;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.CompositeTransport;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Transport that is made reliable by being able to fail over to another
 * transport when a transport failure is detected.
 */
public class FailoverTransport implements CompositeTransport {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverTransport.class);
    private static final int DEFAULT_INITIAL_RECONNECT_DELAY = 10;
    private static final int INFINITE = -1;
    private TransportListener transportListener;
    private volatile boolean disposed;
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
    private final TaskRunnerFactory reconnectTaskFactory;
    private final TaskRunner reconnectTask;
    private volatile boolean started;
    private long initialReconnectDelay = DEFAULT_INITIAL_RECONNECT_DELAY;
    private long maxReconnectDelay = 1000 * 30;
    private double backOffMultiplier = 2d;
    private long timeout = INFINITE;
    private boolean useExponentialBackOff = true;
    private boolean randomize = true;
    private int maxReconnectAttempts = INFINITE;
    private int startupMaxReconnectAttempts = INFINITE;
    private int connectFailures;
    private int warnAfterReconnectAttempts = 10;
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
    private final TransportListener disposedListener = new DefaultTransportListener() {};
    private boolean updateURIsSupported = true;
    private boolean reconnectSupported = true;
    // remember for reconnect thread
    private SslContext brokerSslContext;
    private String updateURIsURL = null;
    private boolean rebalanceUpdateURIs = true;
    private boolean doRebalance = false;
    private boolean connectedToPriority = false;

    private boolean priorityBackup = false;
    private final ArrayList<URI> priorityList = new ArrayList<URI>();
    private boolean priorityBackupAvailable = false;
    private String nestedExtraQueryOptions;
    private volatile boolean shuttingDown = false;

    public FailoverTransport() {
        brokerSslContext = SslContext.getCurrentSslContext();
        stateTracker.setTrackTransactions(true);
        // Setup a task that is used to reconnect the a connection async.
        reconnectTaskFactory = new TaskRunnerFactory();
        reconnectTaskFactory.init();
        reconnectTask = reconnectTaskFactory.createTaskRunner(new Task() {
            @Override
            public boolean iterate() {
                boolean result = false;
                if (!started) {
                    return result;
                }
                boolean buildBackup = true;
                synchronized (backupMutex) {
                    if ((connectedTransport.get() == null || doRebalance || priorityBackupAvailable) && !disposed) {
                        result = doReconnect();
                        buildBackup = false;
                    }
                }
                if (buildBackup) {
                    buildBackups();
                    if (priorityBackup && !connectedToPriority) {
                        try {
                            doDelay();
                            if (reconnectTask == null) {
                                return true;
                            }
                            reconnectTask.wakeup();
                        } catch (InterruptedException e) {
                            LOG.debug("Reconnect task has been interrupted.", e);
                        }
                    }
                } else {
                    // build backups on the next iteration
                    buildBackup = true;
                    try {
                        if (reconnectTask == null) {
                            return true;
                        }
                        reconnectTask.wakeup();
                    } catch (InterruptedException e) {
                        LOG.debug("Reconnect task has been interrupted.", e);
                    }
                }
                return result;
            }

        }, "ActiveMQ Failover Worker: " + System.identityHashCode(this));
    }

    private void processCommand(Object incoming) {
        Command command = (Command) incoming;
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

        if (command.isConnectionControl()) {
            handleConnectionControl((ConnectionControl) command);
        } else if (command.isConsumerControl()) {
            ConsumerControl consumerControl = (ConsumerControl)command;
            if (consumerControl.isClose()) {
                stateTracker.processRemoveConsumer(consumerControl.getConsumerId(), RemoveInfo.LAST_DELIVERED_UNKNOWN);
            }
        }

        if (transportListener != null) {
            transportListener.onCommand(command);
        }
    }

    private TransportListener createTransportListener(final Transport owner) {
        return new TransportListener() {

            @Override
            public void onCommand(Object o) {
                processCommand(o);
            }

            @Override
            public void onException(IOException error) {
                try {
                    handleTransportFailure(owner, error);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (transportListener != null) {
                        transportListener.onException(new InterruptedIOException());
                    }
                }
            }

            @Override
            public void transportInterupted() {
            }

            @Override
            public void transportResumed() {
            }
        };
    }

    public final void disposeTransport(Transport transport) {
        transport.setTransportListener(disposedListener);
        ServiceSupport.dispose(transport);
    }

    public final void handleTransportFailure(IOException e) throws InterruptedException {
        handleTransportFailure(getConnectedTransport(), e);
    }

    public final void handleTransportFailure(Transport failed, IOException e) throws InterruptedException {
        if (shuttingDown) {
            // shutdown info sent and remote socket closed and we see that before a local close
            // let the close do the work
            return;
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace(this + " handleTransportFailure: " + e, e);
        }

        // could be blocked in write with the reconnectMutex held, but still needs to be whacked
        Transport transport = null;

        if (connectedTransport.compareAndSet(failed, null)) {
            transport = failed;
            if (transport != null) {
                disposeTransport(transport);
            }
        }

        synchronized (reconnectMutex) {
            if (transport != null && connectedTransport.get() == null) {
                boolean reconnectOk = false;

                if (canReconnect()) {
                    reconnectOk = true;
                }

                LOG.warn("Transport ({}) failed {} attempting to automatically reconnect: {}",
                         connectedTransportURI, (reconnectOk ? "," : ", not"), e);

                failedConnectTransportURI = connectedTransportURI;
                connectedTransportURI = null;
                connectedToPriority = false;

                if (reconnectOk) {
                    // notify before any reconnect attempt so ack state can be whacked
                    if (transportListener != null) {
                        transportListener.transportInterupted();
                    }

                    updated.remove(failedConnectTransportURI);
                    reconnectTask.wakeup();
                } else if (!isDisposed()) {
                    propagateFailureToExceptionListener(e);
                }
            }
        }
    }

    private boolean canReconnect() {
        return started && 0 != calculateReconnectAttemptLimit();
    }

    public final void handleConnectionControl(ConnectionControl control) {
        String reconnectStr = control.getReconnectTo();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Received ConnectionControl: {}", control);
        }

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

    @Override
    public void start() throws Exception {
        synchronized (reconnectMutex) {
            LOG.debug("Started {}", this);
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

    @Override
    public void stop() throws Exception {
        Transport transportToStop = null;
        List<Transport> backupsToStop = new ArrayList<Transport>(backups.size());

        try {
            synchronized (reconnectMutex) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Stopped {}", this);
                }
                if (!started) {
                    return;
                }
                started = false;
                disposed = true;

                if (connectedTransport.get() != null) {
                    transportToStop = connectedTransport.getAndSet(null);
                }
                reconnectMutex.notifyAll();
            }
            synchronized (sleepMutex) {
                sleepMutex.notifyAll();
            }
        } finally {
            reconnectTask.shutdown();
            reconnectTaskFactory.shutdownNow();
        }

        synchronized(backupMutex) {
            for (BackupTransport backup : backups) {
                backup.setDisposed(true);
                Transport transport = backup.getTransport();
                if (transport != null) {
                    transport.setTransportListener(disposedListener);
                    backupsToStop.add(transport);
                }
            }
            backups.clear();
        }
        for (Transport transport : backupsToStop) {
            try {
                LOG.trace("Stopped backup: {}", transport);
                disposeTransport(transport);
            } catch (Exception e) {
            }
        }
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

    public int getCurrentBackups() {
        return this.backups.size();
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

    public boolean isPriorityBackup() {
        return priorityBackup;
    }

    public void setPriorityBackup(boolean priorityBackup) {
        this.priorityBackup = priorityBackup;
    }

    public void setPriorityURIs(String priorityURIs) {
        StringTokenizer tokenizer = new StringTokenizer(priorityURIs, ",");
        while (tokenizer.hasMoreTokens()) {
            String str = tokenizer.nextToken();
            try {
                URI uri = new URI(str);
                priorityList.add(uri);
            } catch (Exception e) {
                LOG.error("Failed to parse broker address: " + str, e);
            }
        }
    }

    @Override
    public void oneway(Object o) throws IOException {

        Command command = (Command) o;
        Exception error = null;
        try {

            synchronized (reconnectMutex) {

                if (command != null && connectedTransport.get() == null) {
                    if (command.isShutdownInfo()) {
                        // Skipping send of ShutdownInfo command when not connected.
                        return;
                    } else if (command instanceof RemoveInfo || command.isMessageAck()) {
                        // Simulate response to RemoveInfo command or MessageAck (as it will be stale)
                        stateTracker.track(command);
                        if (command.isResponseRequired()) {
                            Response response = new Response();
                            response.setCorrelationId(command.getCommandId());
                            processCommand(response);
                        }
                        return;
                    } else if (command instanceof MessagePull) {
                        // Simulate response to MessagePull if timed as we can't honor that now.
                        MessagePull pullRequest = (MessagePull) command;
                        if (pullRequest.getTimeout() != 0) {
                            MessageDispatch dispatch = new MessageDispatch();
                            dispatch.setConsumerId(pullRequest.getConsumerId());
                            dispatch.setDestination(pullRequest.getDestination());
                            processCommand(dispatch);
                        }
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
                                && !Thread.currentThread().isInterrupted() && willReconnect()) {

                            LOG.trace("Waiting for transport to reconnect..: {}", command);
                            long end = System.currentTimeMillis();
                            if (command.isMessage() && timeout > 0 && (end - start > timeout)) {
                                timedout = true;
                                LOG.info("Failover timed out after {} ms", (end - start));
                                break;
                            }
                            try {
                                reconnectMutex.wait(100);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                LOG.debug("Interupted:", e);
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
                            } else if (!willReconnect()) {
                                error = new IOException("Reconnect attempts of " + maxReconnectAttempts + " exceeded");
                            } else {
                                error = new IOException("Unexpected failure.");
                            }
                            break;
                        }

                        Tracked tracked = null;
                        try {
                            tracked = stateTracker.track(command);
                        } catch (IOException ioe) {
                            LOG.debug("Cannot track the command {} {}", command, ioe);
                        }
                        // If it was a request and it was not being tracked by
                        // the state tracker,
                        // then hold it in the requestMap so that we can replay
                        // it later.
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
                            if (command.isShutdownInfo()) {
                                shuttingDown = true;
                            }
                        } catch (IOException e) {

                            // If the command was not tracked.. we will retry in
                            // this method
                            if (tracked == null && canReconnect()) {

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
                            } else {
                                // Handle the error but allow the method to return since the
                                // tracked commands are replayed on reconnect.
                                LOG.debug("Send oneway attempt: {} failed for command: {}", i, command);
                                handleTransportFailure(e);
                            }
                        }

                        return;
                    } catch (IOException e) {
                        LOG.debug("Send oneway attempt: {} failed for command: {}", i, command);
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

    private boolean willReconnect() {
        return firstConnection || 0 != calculateReconnectAttemptLimit();
    }

    @Override
    public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    @Override
    public Object request(Object command) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    @Override
    public Object request(Object command, int timeout) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    @Override
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

    @Override
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
            LOG.error("Failed to parse URI: {}", u);
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
        if (!updated.isEmpty()) {
            return updated;
        }
        ArrayList<URI> l = new ArrayList<URI>(uris);
        boolean removed = false;
        if (failedConnectTransportURI != null) {
            removed = l.remove(failedConnectTransportURI);
        }
        if (randomize) {
            // Randomly, reorder the list by random swapping
            for (int i = 0; i < l.size(); i++) {
                // meed parenthesis due other JDKs (see AMQ-4826)
                int p = ((int) (Math.random() * 100)) % l.size();
                URI t = l.get(p);
                l.set(p, l.get(i));
                l.set(i, t);
            }
        }
        if (removed) {
            l.add(failedConnectTransportURI);
        }

        LOG.debug("urlList connectionList:{}, from: {}", l, uris);

        return l;
    }

    @Override
    public TransportListener getTransportListener() {
        return transportListener;
    }

    @Override
    public void setTransportListener(TransportListener commandListener) {
        synchronized (listenerMutex) {
            this.transportListener = commandListener;
            listenerMutex.notifyAll();
        }
    }

    @Override
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
            LOG.trace("restore requestMap, replay: {}", command);
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

    @Override
    public String getRemoteAddress() {
        Transport transport = connectedTransport.get();
        if (transport != null) {
            return transport.getRemoteAddress();
        }
        return null;
    }

    @Override
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
                LOG.error("Failed to read updateURIsURL: {} {}",fileURL, ioe);
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
            if ((connectedTransport.get() != null && !doRebalance && !priorityBackupAvailable) || disposed || connectionFailure != null) {
                return false;
            } else {
                List<URI> connectList = getConnectList();
                if (connectList.isEmpty()) {
                    failure = new IOException("No uris available to connect to.");
                } else {
                    if (doRebalance) {
                        if (connectedToPriority || compareURIs(connectList.get(0), connectedTransportURI)) {
                            // already connected to first in the list, no need to rebalance
                            doRebalance = false;
                            return false;
                        } else {
                            LOG.debug("Doing rebalance from: {} to {}", connectedTransportURI, connectList);

                            try {
                                Transport transport = this.connectedTransport.getAndSet(null);
                                if (transport != null) {
                                    disposeTransport(transport);
                                }
                            } catch (Exception e) {
                                LOG.debug("Caught an exception stopping existing transport for rebalance", e);
                            }
                        }
                        doRebalance = false;
                    }

                    resetReconnectDelay();

                    Transport transport = null;
                    URI uri = null;

                    // If we have a backup already waiting lets try it.
                    synchronized (backupMutex) {
                        if ((priorityBackup || backup) && !backups.isEmpty()) {
                            ArrayList<BackupTransport> l = new ArrayList<BackupTransport>(backups);
                            if (randomize) {
                                Collections.shuffle(l);
                            }
                            BackupTransport bt = l.remove(0);
                            backups.remove(bt);
                            transport = bt.getTransport();
                            uri = bt.getUri();
                            processCommand(bt.getBrokerInfo());
                            if (priorityBackup && priorityBackupAvailable) {
                                Transport old = this.connectedTransport.getAndSet(null);
                                if (old != null) {
                                    disposeTransport(old);
                                }
                                priorityBackupAvailable = false;
                            }
                        }
                    }

                    // When there was no backup and we are reconnecting for the first time
                    // we honor the initialReconnectDelay before trying a new connection, after
                    // this normal reconnect delay happens following a failed attempt.
                    if (transport == null && !firstConnection && connectFailures == 0 && initialReconnectDelay > 0 && !disposed) {
                        // reconnectDelay will be equal to initialReconnectDelay since we are on
                        // the first connect attempt after we had a working connection, doDelay
                        // will apply updates to move to the next reconnectDelay value based on
                        // configuration.
                        doDelay();
                    }

                    Iterator<URI> iter = connectList.iterator();
                    while ((transport != null || iter.hasNext()) && (connectedTransport.get() == null && !disposed)) {

                        try {
                            SslContext.setCurrentSslContext(brokerSslContext);

                            // We could be starting with a backup and if so we wait to grab a
                            // URI from the pool until next time around.
                            if (transport == null) {
                                uri = addExtraQueryOptions(iter.next());
                                transport = TransportFactory.compositeConnect(uri);
                            }

                            LOG.debug("Attempting {}th connect to: {}", connectFailures, uri);

                            transport.setTransportListener(createTransportListener(transport));
                            transport.start();

                            if (started && !firstConnection) {
                                restoreTransport(transport);
                            }

                            LOG.debug("Connection established");

                            reconnectDelay = initialReconnectDelay;
                            connectedTransportURI = uri;
                            connectedTransport.set(transport);
                            connectedToPriority = isPriority(connectedTransportURI);
                            reconnectMutex.notifyAll();
                            connectFailures = 0;

                            // Make sure on initial startup, that the transportListener
                            // has been initialized for this instance.
                            synchronized (listenerMutex) {
                                if (transportListener == null) {
                                    try {
                                        // if it isn't set after 2secs - it probably never will be
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
                                LOG.info("Successfully connected to {}", uri);
                            } else {
                                LOG.info("Successfully reconnected to {}", uri);
                            }

                            return false;
                        } catch (Exception e) {
                            failure = e;
                            LOG.debug("Connect fail to: {}, reason: {}", uri, e);
                            if (transport != null) {
                                try {
                                    transport.stop();
                                    transport = null;
                                } catch (Exception ee) {
                                    LOG.debug("Stop of failed transport: {} failed with reason: {}", transport, ee);
                                }
                            }
                        } finally {
                            SslContext.setCurrentSslContext(null);
                        }
                    }
                }
            }

            int reconnectLimit = calculateReconnectAttemptLimit();

            connectFailures++;
            if (reconnectLimit != INFINITE && connectFailures >= reconnectLimit) {
                LOG.error("Failed to connect to {} after: {} attempt(s)", uris, connectFailures);
                connectionFailure = failure;

                // Make sure on initial startup, that the transportListener has been
                // initialized for this instance.
                synchronized (listenerMutex) {
                    if (transportListener == null) {
                        try {
                            listenerMutex.wait(2000);
                        } catch (InterruptedException ex) {
                        }
                    }
                }

                propagateFailureToExceptionListener(connectionFailure);
                return false;
            }

            int warnInterval = getWarnAfterReconnectAttempts();
            if (warnInterval > 0 && (connectFailures % warnInterval) == 0) {
                LOG.warn("Failed to connect to {} after: {} attempt(s) continuing to retry.",
                         uris, connectFailures);
            }
        }

        if (!disposed) {
            doDelay();
        }

        return !disposed;
    }

    private void doDelay() {
        if (reconnectDelay > 0) {
            synchronized (sleepMutex) {
                LOG.debug("Waiting {} ms before attempting connection", reconnectDelay);
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

    private void resetReconnectDelay() {
        if (!useExponentialBackOff || reconnectDelay == DEFAULT_INITIAL_RECONNECT_DELAY) {
            reconnectDelay = initialReconnectDelay;
        }
    }

    /*
     * called with reconnectMutex held
     */
    private void propagateFailureToExceptionListener(Exception exception) {
        if (transportListener != null) {
            if (exception instanceof IOException) {
                transportListener.onException((IOException)exception);
            } else {
                transportListener.onException(IOExceptionSupport.create(exception));
            }
        }
        reconnectMutex.notifyAll();
    }

    private int calculateReconnectAttemptLimit() {
        int maxReconnectValue = this.maxReconnectAttempts;
        if (firstConnection && this.startupMaxReconnectAttempts != INFINITE) {
            maxReconnectValue = this.startupMaxReconnectAttempts;
        }
        return maxReconnectValue;
    }

    private boolean shouldBuildBackups() {
       return (backup && backups.size() < backupPoolSize) || (priorityBackup && !(priorityBackupAvailable || connectedToPriority));
    }

    final boolean buildBackups() {
        synchronized (backupMutex) {
            if (!disposed && shouldBuildBackups()) {
                ArrayList<URI> backupList = new ArrayList<URI>(priorityList);
                List<URI> connectList = getConnectList();
                for (URI uri: connectList) {
                    if (!backupList.contains(uri)) {
                        backupList.add(uri);
                    }
                }
                // removed disposed backups
                List<BackupTransport> disposedList = new ArrayList<BackupTransport>();
                for (BackupTransport bt : backups) {
                    if (bt.isDisposed()) {
                        disposedList.add(bt);
                    }
                }
                backups.removeAll(disposedList);
                disposedList.clear();
                for (Iterator<URI> iter = backupList.iterator(); !disposed && iter.hasNext() && shouldBuildBackups(); ) {
                    URI uri = addExtraQueryOptions(iter.next());
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
                                if (priorityBackup && isPriority(uri)) {
                                   priorityBackupAvailable = true;
                                   backups.add(0, bt);
                                   // if this priority backup overflows the pool
                                   // remove the backup with the lowest priority
                                   if (backups.size() > backupPoolSize) {
                                       BackupTransport disposeTransport = backups.remove(backups.size() - 1);
                                       disposeTransport.setDisposed(true);
                                       Transport transport = disposeTransport.getTransport();
                                       if (transport != null) {
                                           transport.setTransportListener(disposedListener);
                                           disposeTransport(transport);
                                       }
                                   }
                                } else {
                                    backups.add(bt);
                                }
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

    protected boolean isPriority(URI uri) {
        if (!priorityBackup) {
            return false;
        }

        if (!priorityList.isEmpty()) {
            for (URI priorityURI : priorityList) {
                if (compareURIs(priorityURI, uri)) {
                    return true;
                }
            }

        } else if (!uris.isEmpty()) {
            return compareURIs(uris.get(0), uri);
        }

        return false;
    }

    @Override
    public boolean isDisposed() {
        return disposed;
    }

    @Override
    public boolean isConnected() {
        return connectedTransport.get() != null;
    }

    @Override
    public void reconnect(URI uri) throws IOException {
        add(true, new URI[]{uri});
    }

    @Override
    public boolean isReconnectSupported() {
        return this.reconnectSupported;
    }

    public void setReconnectSupported(boolean value) {
        this.reconnectSupported = value;
    }

    @Override
    public boolean isUpdateURIsSupported() {
        return this.updateURIsSupported;
    }

    public void setUpdateURIsSupported(boolean value) {
        this.updateURIsSupported = value;
    }

    @Override
    public void updateURIs(boolean rebalance, URI[] updatedURIs) throws IOException {
        if (isUpdateURIsSupported()) {
            HashSet<URI> copy = new HashSet<URI>();
            synchronized (reconnectMutex) {
                copy.addAll(this.updated);
                updated.clear();
                if (updatedURIs != null && updatedURIs.length > 0) {
                    for (URI uri : updatedURIs) {
                        if (uri != null && !updated.contains(uri)) {
                            updated.add(uri);
                        }
                    }
                }
            }
            if (!(copy.isEmpty() && updated.isEmpty()) && !copy.equals(new HashSet<URI>(updated))) {
                buildBackups();
                reconnect(rebalance);
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

    @Override
    public int getReceiveCounter() {
        Transport transport = connectedTransport.get();
        if (transport == null) {
            return 0;
        }
        return transport.getReceiveCounter();
    }

    public int getConnectFailures() {
        return connectFailures;
    }

    public void connectionInterruptProcessingComplete(ConnectionId connectionId) {
        synchronized (reconnectMutex) {
            stateTracker.connectionInterruptProcessingComplete(this, connectionId);
        }
    }

    public ConnectionStateTracker getStateTracker() {
        return stateTracker;
    }

    public boolean isConnectedToPriority() {
        return connectedToPriority;
    }

    private boolean contains(URI newURI) {
        boolean result = false;
        for (URI uri : uris) {
            if (compareURIs(newURI, uri)) {
                result = true;
                break;
            }
        }

        return result;
    }

    private boolean compareURIs(final URI first, final URI second) {

        boolean result = false;
        if (first == null || second == null) {
            return result;
        }

        if (first.getPort() == second.getPort()) {
            InetAddress firstAddr = null;
            InetAddress secondAddr = null;
            try {
                firstAddr = InetAddress.getByName(first.getHost());
                secondAddr = InetAddress.getByName(second.getHost());

                if (firstAddr.equals(secondAddr)) {
                    result = true;
                }

            } catch(IOException e) {

                if (firstAddr == null) {
                    LOG.error("Failed to Lookup INetAddress for URI[{}] : {}", first, e);
                } else {
                    LOG.error("Failed to Lookup INetAddress for URI[{}] : {}", second, e);
                }

                if (first.getHost().equalsIgnoreCase(second.getHost())) {
                    result = true;
                }
            }
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

    private URI addExtraQueryOptions(URI uri) {
        try {
            if( nestedExtraQueryOptions!=null && !nestedExtraQueryOptions.isEmpty() ) {
                if( uri.getQuery() == null ) {
                    uri = URISupport.createURIWithQuery(uri, nestedExtraQueryOptions);
                } else {
                    uri = URISupport.createURIWithQuery(uri, uri.getQuery()+"&"+nestedExtraQueryOptions);
                }
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return uri;
    }

    public void setNestedExtraQueryOptions(String nestedExtraQueryOptions) {
        this.nestedExtraQueryOptions = nestedExtraQueryOptions;
    }

    public int getWarnAfterReconnectAttempts() {
        return warnAfterReconnectAttempts;
    }

    /**
     * Sets the number of Connect / Reconnect attempts that must occur before a warn message
     * is logged indicating that the transport is not connected.  This can be useful when the
     * client is running inside some container or service as it give an indication of some
     * problem with the client connection that might not otherwise be visible.  To disable the
     * log messages this value should be set to a value @{code attempts <= 0}
     *
     * @param warnAfterReconnectAttempts
     *      The number of failed connection attempts that must happen before a warning is logged.
     */
    public void setWarnAfterReconnectAttempts(int warnAfterReconnectAttempts) {
        this.warnAfterReconnectAttempts = warnAfterReconnectAttempts;
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        Transport transport = connectedTransport.get();
        if (transport != null) {
            return transport.getPeerCertificates();
        } else {
            return null;
        }
    }

    @Override
    public void setPeerCertificates(X509Certificate[] certificates) {
    }

    @Override
    public WireFormat getWireFormat() {
        Transport transport = connectedTransport.get();
        if (transport != null) {
            return transport.getWireFormat();
        } else {
            return null;
        }
    }
}
