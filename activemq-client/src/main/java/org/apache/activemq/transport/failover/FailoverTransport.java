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
import java.util.LinkedHashSet;
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2715
        brokerSslContext = SslContext.getCurrentSslContext();
//IC see: https://issues.apache.org/jira/browse/AMQ-915
//IC see: https://issues.apache.org/jira/browse/AMQ-915
        stateTracker.setTrackTransactions(true);
        // Setup a task that is used to reconnect the a connection async.
//IC see: https://issues.apache.org/jira/browse/AMQ-3451
        reconnectTaskFactory = new TaskRunnerFactory();
        reconnectTaskFactory.init();
        reconnectTask = reconnectTaskFactory.createTaskRunner(new Task() {
            @Override
            public boolean iterate() {
                boolean result = false;
//IC see: https://issues.apache.org/jira/browse/AMQ-3542
                if (!started) {
                    return result;
                }
                boolean buildBackup = true;
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
                synchronized (backupMutex) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3699
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
//IC see: https://issues.apache.org/jira/browse/AMQ-3222
//IC see: https://issues.apache.org/jira/browse/AMQ-2981
//IC see: https://issues.apache.org/jira/browse/AMQ-2598
//IC see: https://issues.apache.org/jira/browse/AMQ-2939
                    buildBackup = true;
                    try {
//IC see: https://issues.apache.org/jira/browse/AMQ-3782
//IC see: https://issues.apache.org/jira/browse/AMQ-3782
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
//IC see: https://issues.apache.org/jira/browse/AMQ-6248
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2556
                ((Tracked) object).onResponses(command);
            }
        }

        if (command.isConnectionControl()) {
            handleConnectionControl((ConnectionControl) command);
        } else if (command.isConsumerControl()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-5844
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
//IC see: https://issues.apache.org/jira/browse/AMQ-891
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
        transport.setTransportListener(disposedListener);
        ServiceSupport.dispose(transport);
    }

    public final void handleTransportFailure(IOException e) throws InterruptedException {
//IC see: https://issues.apache.org/jira/browse/AMQ-6248
        handleTransportFailure(getConnectedTransport(), e);
    }

    public final void handleTransportFailure(Transport failed, IOException e) throws InterruptedException {
//IC see: https://issues.apache.org/jira/browse/AMQ-5241
//IC see: https://issues.apache.org/jira/browse/AMQ-4897
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
//IC see: https://issues.apache.org/jira/browse/AMQ-6248

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

                LOG.warn("Transport ({}) failed{} attempting to automatically reconnect",
                         connectedTransportURI, (reconnectOk ? "," : ", not"), e);
//IC see: https://issues.apache.org/jira/browse/AMQ-6248

                failedConnectTransportURI = connectedTransportURI;
                connectedTransportURI = null;
                connectedToPriority = false;
//IC see: https://issues.apache.org/jira/browse/AMQ-4501

                if (reconnectOk) {
                    // notify before any reconnect attempt so ack state can be whacked
//IC see: https://issues.apache.org/jira/browse/AMQ-2560
//IC see: https://issues.apache.org/jira/browse/AMQ-4785
                    if (transportListener != null) {
                        transportListener.transportInterupted();
                    }

                    reconnectTask.wakeup();
//IC see: https://issues.apache.org/jira/browse/AMQ-3972
                } else if (!isDisposed()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3542
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
//IC see: https://issues.apache.org/jira/browse/AMQ-4505
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2807
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2807
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
//IC see: https://issues.apache.org/jira/browse/AMQ-1613
            if (connectedTransport.get() != null) {
                stateTracker.restore(connectedTransport.get());
            } else {
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
                reconnect(false);
            }
        }
    }

    @Override
    public void stop() throws Exception {
        Transport transportToStop = null;
        List<Transport> backupsToStop = new ArrayList<Transport>(backups.size());
//IC see: https://issues.apache.org/jira/browse/AMQ-3939

//IC see: https://issues.apache.org/jira/browse/AMQ-3451
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

//IC see: https://issues.apache.org/jira/browse/AMQ-1613
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

//IC see: https://issues.apache.org/jira/browse/AMQ-3939
        synchronized(backupMutex) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3939
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
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
//IC see: https://issues.apache.org/jira/browse/AMQ-1613
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2385
        return this.startupMaxReconnectAttempts;
    }

    public void setStartupMaxReconnectAttempts(int startupMaxReconnectAttempts) {
        this.startupMaxReconnectAttempts = startupMaxReconnectAttempts;
    }

    public long getTimeout() {
//IC see: https://issues.apache.org/jira/browse/AMQ-2061
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
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
//IC see: https://issues.apache.org/jira/browse/AMQ-1572
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
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
//IC see: https://issues.apache.org/jira/browse/AMQ-3517
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
//IC see: https://issues.apache.org/jira/browse/AMQ-3699
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

//IC see: https://issues.apache.org/jira/browse/AMQ-976
//IC see: https://issues.apache.org/jira/browse/AMQ-976
        Command command = (Command) o;
        Exception error = null;
        try {

            synchronized (reconnectMutex) {

//IC see: https://issues.apache.org/jira/browse/AMQ-3516
                if (command != null && connectedTransport.get() == null) {
                    if (command.isShutdownInfo()) {
                        // Skipping send of ShutdownInfo command when not connected.
                        return;
                    } else if (command instanceof RemoveInfo || command.isMessageAck()) {
                        // Simulate response to RemoveInfo command or MessageAck (as it will be stale)
                        stateTracker.track(command);
//IC see: https://issues.apache.org/jira/browse/AMQ-3517
                        if (command.isResponseRequired()) {
                            Response response = new Response();
                            response.setCorrelationId(command.getCommandId());
//IC see: https://issues.apache.org/jira/browse/AMQ-6248
                            processCommand(response);
                        }
                        return;
//IC see: https://issues.apache.org/jira/browse/AMQ-3932
                    } else if (command instanceof MessagePull) {
                        // Simulate response to MessagePull if timed as we can't honor that now.
//IC see: https://issues.apache.org/jira/browse/AMQ-3939
                        MessagePull pullRequest = (MessagePull) command;
                        if (pullRequest.getTimeout() != 0) {
                            MessageDispatch dispatch = new MessageDispatch();
                            dispatch.setConsumerId(pullRequest.getConsumerId());
                            dispatch.setDestination(pullRequest.getDestination());
//IC see: https://issues.apache.org/jira/browse/AMQ-6248
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2061
                        long start = System.currentTimeMillis();
                        boolean timedout = false;
                        while (transport == null && !disposed && connectionFailure == null
                                && !Thread.currentThread().isInterrupted() && willReconnect()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-5951

                            LOG.trace("Waiting for transport to reconnect..: {}", command);
                            long end = System.currentTimeMillis();
//IC see: https://issues.apache.org/jira/browse/AMQ-5231
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
                                error = new IOException("Failover timeout of " + timeout + " ms reached.");
//IC see: https://issues.apache.org/jira/browse/AMQ-5951
                            } else if (!willReconnect()) {
                                error = new IOException("Reconnect attempts of " + maxReconnectAttempts + " exceeded");
                            } else {
                                error = new IOException("Unexpected failure.");
                            }
                            break;
                        }

//IC see: https://issues.apache.org/jira/browse/AMQ-5090
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
                        synchronized (requestMap) {
//IC see: https://issues.apache.org/jira/browse/AMQ-1488
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
//IC see: https://issues.apache.org/jira/browse/AMQ-5241
                            if (command.isShutdownInfo()) {
                                shuttingDown = true;
                            }
                        } catch (IOException e) {

                            // If the command was not tracked.. we will retry in
                            // this method
                            if (tracked == null && canReconnect()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-5951

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
//IC see: https://issues.apache.org/jira/browse/AMQ-891
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }

        if (!disposed) {
            if (error != null) {
                if (error instanceof IOException) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
                    throw (IOException) error;
                }
                throw IOExceptionSupport.create(error);
            }
        }
    }

    private boolean willReconnect() {
//IC see: https://issues.apache.org/jira/browse/AMQ-5951
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
//IC see: https://issues.apache.org/jira/browse/AMQ-3513
        for (URI uri : u) {
            if (!contains(uri)) {
                uris.add(uri);
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
                newURI = true;
            }
        }
        if (newURI) {
            reconnect(rebalance);
        }
    }

    @Override
    public void remove(boolean rebalance, URI u[]) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3513
        for (URI uri : u) {
            uris.remove(uri);
        }
        // rebalance is automatic if any connected to removed/stopped broker
    }

    public void add(boolean rebalance, String u) {
        try {
            URI newURI = new URI(u);
//IC see: https://issues.apache.org/jira/browse/AMQ-3513
            if (contains(newURI) == false) {
                uris.add(newURI);
                reconnect(rebalance);
            }

        } catch (Exception e) {
            LOG.error("Failed to parse URI: {}", u);
        }
    }

    public void reconnect(boolean rebalance) {
//IC see: https://issues.apache.org/jira/browse/AMQ-1771
        synchronized (reconnectMutex) {
            if (started) {
                if (rebalance) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3077
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
                    doRebalance = true;
                }
                LOG.debug("Waking up reconnect task");
                try {
//IC see: https://issues.apache.org/jira/browse/AMQ-726
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
        // updated have precedence
//IC see: https://issues.apache.org/jira/browse/AMQ-7165
        LinkedHashSet<URI> uniqueUris = new LinkedHashSet<URI>(updated);
        uniqueUris.addAll(uris);

        boolean removed = false;
        if (failedConnectTransportURI != null) {
            removed = uniqueUris.remove(failedConnectTransportURI);
        }

        ArrayList<URI> l = new ArrayList<URI>(uniqueUris);
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

        LOG.debug("urlList connectionList:{}, from: {}", l, uniqueUris);

        return l;
    }

    @Override
    public TransportListener getTransportListener() {
        return transportListener;
    }

    @Override
    public void setTransportListener(TransportListener commandListener) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
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
//IC see: https://issues.apache.org/jira/browse/AMQ-3513
        Map<Integer, Command> tmpMap = null;
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2807
//IC see: https://issues.apache.org/jira/browse/AMQ-3513
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

//IC see: https://issues.apache.org/jira/browse/AMQ-2632
    final boolean doReconnect() {
        Exception failure = null;
        synchronized (reconnectMutex) {
//IC see: https://issues.apache.org/jira/browse/AMQ-7165
            List<URI> connectList = null;
            // First ensure we are up to date.
            doUpdateURIsFromDisk();

            if (disposed || connectionFailure != null) {
                reconnectMutex.notifyAll();
            }
//IC see: https://issues.apache.org/jira/browse/AMQ-3699
            if ((connectedTransport.get() != null && !doRebalance && !priorityBackupAvailable) || disposed || connectionFailure != null) {
                return false;
            } else {
//IC see: https://issues.apache.org/jira/browse/AMQ-7165
                connectList = getConnectList();
                if (connectList.isEmpty()) {
                    failure = new IOException("No uris available to connect to.");
                } else {
                    if (doRebalance) {
//IC see: https://issues.apache.org/jira/browse/AMQ-4501
                        if (connectedToPriority || compareURIs(connectList.get(0), connectedTransportURI)) {
                            // already connected to first in the list, no need to rebalance
                            doRebalance = false;
                            return false;
                        } else {
                            LOG.debug("Doing rebalance from: {} to {}", connectedTransportURI, connectList);

                            try {
                                Transport transport = this.connectedTransport.getAndSet(null);
                                if (transport != null) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
                                    disposeTransport(transport);
                                }
                            } catch (Exception e) {
                                LOG.debug("Caught an exception stopping existing transport for rebalance", e);
                            }
                        }
                        doRebalance = false;
                    }

                    resetReconnectDelay();
//IC see: https://issues.apache.org/jira/browse/AMQ-3542

                    Transport transport = null;
                    URI uri = null;

                    // If we have a backup already waiting lets try it.
                    synchronized (backupMutex) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3699
                        if ((priorityBackup || backup) && !backups.isEmpty()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3706
                            ArrayList<BackupTransport> l = new ArrayList<BackupTransport>(backups);
//IC see: https://issues.apache.org/jira/browse/AMQ-3685
                            if (randomize) {
                                Collections.shuffle(l);
                            }
                            BackupTransport bt = l.remove(0);
                            backups.remove(bt);
                            transport = bt.getTransport();
                            uri = bt.getUri();
//IC see: https://issues.apache.org/jira/browse/AMQ-6248
                            processCommand(bt.getBrokerInfo());
//IC see: https://issues.apache.org/jira/browse/AMQ-3699
                            if (priorityBackup && priorityBackupAvailable) {
                                Transport old = this.connectedTransport.getAndSet(null);
//IC see: https://issues.apache.org/jira/browse/AMQ-4507
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
//IC see: https://issues.apache.org/jira/browse/AMQ-5819
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
//IC see: https://issues.apache.org/jira/browse/AMQ-4718
                                uri = addExtraQueryOptions(iter.next());
                                transport = TransportFactory.compositeConnect(uri);
                            }

                            LOG.debug("Attempting {}th connect to: {}", connectFailures, uri);

//IC see: https://issues.apache.org/jira/browse/AMQ-6248
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

//IC see: https://issues.apache.org/jira/browse/AMQ-2730
            connectFailures++;
            if (reconnectLimit != INFINITE && connectFailures >= reconnectLimit) {
                LOG.error("Failed to connect to {} after: {} attempt(s)", connectList, connectFailures);
                connectionFailure = failure;

                // Make sure on initial startup, that the transportListener has been
                // initialized for this instance.
                synchronized (listenerMutex) {
                    if (transportListener == null) {
                        try {
//IC see: https://issues.apache.org/jira/browse/AMQ-1771
                            listenerMutex.wait(2000);
                        } catch (InterruptedException ex) {
                        }
                    }
                }

//IC see: https://issues.apache.org/jira/browse/AMQ-3542
                propagateFailureToExceptionListener(connectionFailure);
                return false;
            }

            int warnInterval = getWarnAfterReconnectAttempts();
            if (warnInterval > 0 && (connectFailures == 1 || (connectFailures % warnInterval) == 0)) {
                LOG.warn("Failed to connect to {} after: {} attempt(s) with {}, continuing to retry.",
//IC see: https://issues.apache.org/jira/browse/AMQ-7165
                         connectList, connectFailures, (failure == null ? "?" : failure.getLocalizedMessage()));
            }
        }

        if (!disposed) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3699
            doDelay();
        }

        return !disposed;
    }

    private void doDelay() {
//IC see: https://issues.apache.org/jira/browse/AMQ-3513
        if (reconnectDelay > 0) {
            synchronized (sleepMutex) {
                LOG.debug("Waiting {} ms before attempting connection", reconnectDelay);
                try {
                    sleepMutex.wait(reconnectDelay);
                } catch (InterruptedException e) {
//IC see: https://issues.apache.org/jira/browse/AMQ-891
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
//IC see: https://issues.apache.org/jira/browse/AMQ-3049
//IC see: https://issues.apache.org/jira/browse/AMQ-3542
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
//IC see: https://issues.apache.org/jira/browse/AMQ-4461
       return (backup && backups.size() < backupPoolSize) || (priorityBackup && !(priorityBackupAvailable || connectedToPriority));
    }

    final boolean buildBackups() {
        synchronized (backupMutex) {
            if (!disposed && shouldBuildBackups()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3699
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
//IC see: https://issues.apache.org/jira/browse/AMQ-4461
                for (Iterator<URI> iter = backupList.iterator(); !disposed && iter.hasNext() && shouldBuildBackups(); ) {
//IC see: https://issues.apache.org/jira/browse/AMQ-4718
                    URI uri = addExtraQueryOptions(iter.next());
                    if (connectedTransportURI != null && !connectedTransportURI.equals(uri)) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3939
                        try {
//IC see: https://issues.apache.org/jira/browse/AMQ-2715
//IC see: https://issues.apache.org/jira/browse/AMQ-2715
                            SslContext.setCurrentSslContext(brokerSslContext);
                            BackupTransport bt = new BackupTransport(this);
                            bt.setUri(uri);
                            if (!backups.contains(bt)) {
                                Transport t = TransportFactory.compositeConnect(uri);
                                t.setTransportListener(bt);
//IC see: https://issues.apache.org/jira/browse/AMQ-610
                                t.start();
                                bt.setTransport(t);
//IC see: https://issues.apache.org/jira/browse/AMQ-3699
                                if (priorityBackup && isPriority(uri)) {
                                   priorityBackupAvailable = true;
//IC see: https://issues.apache.org/jira/browse/AMQ-4461
                                   backups.add(0, bt);
                                   // if this priority backup overflows the pool
                                   // remove the backup with the lowest priority
//IC see: https://issues.apache.org/jira/browse/AMQ-4461
                                   if (backups.size() > backupPoolSize) {
                                       BackupTransport disposeTransport = backups.remove(backups.size() - 1);
                                       disposeTransport.setDisposed(true);
                                       Transport transport = disposeTransport.getTransport();
                                       if (transport != null) {
                                           transport.setTransportListener(disposedListener);
//IC see: https://issues.apache.org/jira/browse/AMQ-2632
                                           disposeTransport(transport);
                                       }
                                   }
                                } else {
                                    backups.add(bt);
                                }
                            }
                        } catch (Exception e) {
                            LOG.debug("Failed to build backup ", e);
//IC see: https://issues.apache.org/jira/browse/AMQ-2715
//IC see: https://issues.apache.org/jira/browse/AMQ-2715
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
//IC see: https://issues.apache.org/jira/browse/AMQ-4501
        if (!priorityBackup) {
            return false;
        }

        if (!priorityList.isEmpty()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-5336
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
//IC see: https://issues.apache.org/jira/browse/AMQ-4897
        return connectedTransport.get() != null;
    }

    @Override
    public void reconnect(URI uri) throws IOException {
//IC see: https://issues.apache.org/jira/browse/AMQ-3513
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
//IC see: https://issues.apache.org/jira/browse/AMQ-3513
                    for (URI uri : updatedURIs) {
                        if (uri != null && !updated.contains(uri)) {
                            updated.add(uri);
//IC see: https://issues.apache.org/jira/browse/AMQ-7165
                            if (failedConnectTransportURI != null && failedConnectTransportURI.equals(uri)) {
                                failedConnectTransportURI = null;
                            }
                        }
                    }
                }
            }
//IC see: https://issues.apache.org/jira/browse/AMQ-3706
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2807
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2511
        Transport transport = connectedTransport.get();
        if (transport == null) {
            return 0;
        }
        return transport.getReceiveCounter();
    }

    public int getConnectFailures() {
//IC see: https://issues.apache.org/jira/browse/AMQ-2730
        return connectFailures;
    }

    public void connectionInterruptProcessingComplete(ConnectionId connectionId) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2579
        synchronized (reconnectMutex) {
            stateTracker.connectionInterruptProcessingComplete(this, connectionId);
        }
    }

    public ConnectionStateTracker getStateTracker() {
//IC see: https://issues.apache.org/jira/browse/AMQ-2556
        return stateTracker;
    }

    public boolean isConnectedToPriority() {
//IC see: https://issues.apache.org/jira/browse/AMQ-5336
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
//IC see: https://issues.apache.org/jira/browse/AMQ-4501
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
//IC see: https://issues.apache.org/jira/browse/AMQ-2807
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
//IC see: https://issues.apache.org/jira/browse/AMQ-4718
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
//IC see: https://issues.apache.org/jira/browse/AMQ-4706
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
//IC see: https://issues.apache.org/jira/browse/AMQ-6339
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
