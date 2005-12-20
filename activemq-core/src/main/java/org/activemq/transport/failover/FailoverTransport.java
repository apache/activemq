/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
package org.activemq.transport.failover;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.activemq.command.Command;
import org.activemq.command.Response;
import org.activemq.state.ConnectionStateTracker;
import org.activemq.thread.DefaultThreadPools;
import org.activemq.thread.Task;
import org.activemq.thread.TaskRunner;
import org.activemq.transport.CompositeTransport;
import org.activemq.transport.FutureResponse;
import org.activemq.transport.Transport;
import org.activemq.transport.TransportFactory;
import org.activemq.transport.TransportListener;
import org.activemq.util.IOExceptionSupport;
import org.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.CopyOnWriteArrayList;

/**
 * A Transport that is made reliable by being able to fail over to another
 * transport when a transport failure is detected.
 * 
 * @version $Revision$
 */
public class FailoverTransport implements CompositeTransport {

    private static final Log log = LogFactory.getLog(FailoverTransport.class);

    private TransportListener transportListener;
    private boolean disposed;
    private final CopyOnWriteArrayList uris = new CopyOnWriteArrayList();

    private final Object reconnectMutex = new Object();
    private final ConnectionStateTracker stateTracker = new ConnectionStateTracker();
    private final ConcurrentHashMap requestMap = new ConcurrentHashMap();

    private URI connectedTransportURI;
    private Transport connectedTransport;
    private final TaskRunner reconnectTask;
    private boolean started;

    private long initialReconnectDelay = 10;
    private long maxReconnectDelay = 1000 * 30;
    private long backOffMultiplier = 2;
    private boolean useExponentialBackOff = true;
    private int maxReconnectAttempts;
    private int connectFailures;
    private long reconnectDelay = initialReconnectDelay;
    private Exception connectionFailure;

    private final TransportListener myTransportListener = new TransportListener() {
        public void onCommand(Command command) {
            if (command.isResponse()) {
                requestMap.remove(new Short(((Response) command).getCorrelationId()));
            }
            transportListener.onCommand(command);
        }

        public void onException(IOException error) {
            try {
                handleTransportFailure(error);
            }
            catch (InterruptedException e) {
                transportListener.onException(new InterruptedIOException());
            }
        }
    };

    public FailoverTransport() throws InterruptedIOException {

        // Setup a task that is used to reconnect the a connection async.
        reconnectTask = DefaultThreadPools.getDefaultTaskRunnerFactory().createTaskRunner(new Task() {

            public boolean iterate() {

                Exception failure=null;
                synchronized (reconnectMutex) {

                    if (disposed || connectionFailure!=null) {
                        reconnectMutex.notifyAll();
                    }

                    if (connectedTransport != null || disposed || connectionFailure!=null) {
                        return false;
                    } else {
                        ArrayList connectList = getConnectList();
                        if( connectList.isEmpty() ) {
                            failure = new IOException("No uris available to connect to.");
                        } else {
                            reconnectDelay = initialReconnectDelay;
                            Iterator iter = connectList.iterator();
                            for (int i = 0; iter.hasNext() && connectedTransport == null && !disposed; i++) {
                                URI uri = (URI) iter.next();
                                try {
                                    log.debug("Attempting connect to: " + uri);
                                    Transport t = TransportFactory.compositeConnect(uri);
                                    t.setTransportListener(myTransportListener);
                                    if (started) {
                                        restoreTransport(t);
                                    }
                                    log.debug("Connection established");
                                    reconnectDelay = initialReconnectDelay;
                                    connectedTransportURI = uri;
                                    connectedTransport = t;
                                    reconnectMutex.notifyAll();
                                    connectFailures = 0;
                                    return false;
                                }
                                catch (Exception e) {
                                    failure = e;
                                    log.debug("Connect fail to: " + uri + ", reason: " + e);
                                }
                            }
                        }
                    }
                    
                    if (maxReconnectAttempts > 0 && ++connectFailures >= maxReconnectAttempts) {
                        log.error("Failed to connect to transport after: " + connectFailures + " attempt(s)");
                        connectionFailure = failure;
                        reconnectMutex.notifyAll();
                        return false;
                    }
                }

                
                try {
                    log.debug("Waiting " + reconnectDelay + " ms before attempting connection. ");
                    Thread.sleep(reconnectDelay);
                }
                catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }

                if (useExponentialBackOff) {
                    // Exponential increment of reconnect delay.
                    reconnectDelay *= backOffMultiplier;
                    if (reconnectDelay > maxReconnectDelay)
                        reconnectDelay = maxReconnectDelay;
                }
                return true;
            }

        });
    }

    private void handleTransportFailure(IOException e) throws InterruptedException {
        synchronized (reconnectMutex) {
            log.debug("Transport failed, starting up reconnect task", e);
            if (connectedTransport != null) {
                ServiceSupport.dispose(connectedTransport);
                connectedTransport = null;
                connectedTransportURI = null;
                reconnectTask.wakeup();
            }
        }
    }

    public void start() throws Exception {
        synchronized (reconnectMutex) {
            log.debug("Started.");
            if (started)
                return;
            started = true;
            if (connectedTransport != null) {
                connectedTransport.start();
                stateTracker.restore(connectedTransport);
            }
        }
    }

    public void stop() throws Exception {
        synchronized (reconnectMutex) {
            log.debug("Stopped.");
            if (!started)
                return;
            started = false;
            disposed = true;

            if (connectedTransport != null) {
                connectedTransport.stop();
            }
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

    public long getReconnectDelayExponent() {
        return backOffMultiplier;
    }

    public void setReconnectDelayExponent(long reconnectDelayExponent) {
        this.backOffMultiplier = reconnectDelayExponent;
    }

    public Transport getConnectedTransport() {
        return connectedTransport;
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

    public void oneway(Command command) throws IOException {
        Exception error = null;
        try {

            synchronized (reconnectMutex) {
                // Keep trying until the message is sent.
                for (int i = 0;; i++) {
                    try {

                        // Wait for transport to be connected.
                        while (connectedTransport == null && !disposed && connectionFailure==null ) {
                            log.debug("Waiting for transport to reconnect.");
                            reconnectMutex.wait(1000);
                        }

                        if( connectedTransport==null ) {
                            // Previous loop may have exited due to use being
                            // disposed.
                            if (disposed) {
                                error = new IOException("Transport disposed.");
                            } else if (connectionFailure!=null) {
                                error = connectionFailure;
                            } else {
                                error = new IOException("Unexpected failure.");
                            }
                            break;
                        }
                        
                        // Send the message.
                        connectedTransport.oneway(command);

                        // If it was a request and it was not being tracked by
                        // the state tracker,
                        // then hold it in the requestMap so that we can replay
                        // it later.
                        if (!stateTracker.track(command) && command.isResponseRequired()) {
                            requestMap.put(new Short(command.getCommandId()), command);
                        }
                        return;

                    }
                    catch (IOException e) {
                        log.debug("Send oneway attempt: " + i + " failed.");
                        handleTransportFailure(e);
                    }
                }
            }
        }
        catch (InterruptedException e) {
            // Some one may be trying to stop our thread.
            throw new InterruptedIOException();
        }
        if( error instanceof IOException )
            throw (IOException)error;
        throw IOExceptionSupport.create(error);
    }

    public FutureResponse asyncRequest(Command command) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    public Response request(Command command) throws IOException {
        throw new AssertionError("Unsupported Method");
    }

    public void add(URI u[]) {
        for (int i = 0; i < u.length; i++) {
            if( !uris.contains(u[i]) )
                uris.add(u[i]);
        }
        reconnect();
    }

    public void remove(URI u[]) {
        for (int i = 0; i < u.length; i++) {
            uris.remove(u[i]);
        }
        reconnect();
    }

    public void reconnect() {
        log.debug("Waking up reconnect task");
        try {
            reconnectTask.wakeup();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ArrayList getConnectList() {
        ArrayList l = new ArrayList(uris);

        // Randomly, reorder the list by random swapping
        Random r = new Random();
        r.setSeed(System.currentTimeMillis());
        for (int i = 0; i < l.size(); i++) {
            int p = r.nextInt(l.size());
            Object t = l.get(p);
            l.set(p, l.get(i));
            l.set(i, t);
        }
        return l;
    }

    public void setTransportListener(TransportListener commandListener) {
        this.transportListener = commandListener;
    }

    public Object narrow(Class target) {

        if (target.isAssignableFrom(getClass())) {
            return this;
        }
        synchronized (reconnectMutex) {
            if (connectedTransport != null) {
                return connectedTransport.narrow(target);
            }
        }
        return null;

    }

    protected void restoreTransport(Transport t) throws Exception, IOException {
        t.start();
        stateTracker.restore(t);
        for (Iterator iter2 = requestMap.values().iterator(); iter2.hasNext();) {
            Command command = (Command) iter2.next();
            t.oneway(command);
        }
    }

    public boolean isUseExponentialBackOff() {
        return useExponentialBackOff;
    }

    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }

    public String toString() {
        return connectedTransportURI==null ? "unconnected" : connectedTransportURI.toString();
    }

}
