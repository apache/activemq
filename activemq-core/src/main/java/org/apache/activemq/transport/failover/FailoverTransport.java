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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Transport that is made reliable by being able to fail over to another
 * transport when a transport failure is detected.
 * 
 * @version $Revision$
 */
public class FailoverTransport implements CompositeTransport {

    private static final Log LOG = LogFactory.getLog(FailoverTransport.class);

    private TransportListener transportListener;
    private boolean disposed;
    private boolean connected;
    private final CopyOnWriteArrayList<URI> uris = new CopyOnWriteArrayList<URI>();

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

    private long initialReconnectDelay = 10;
    private long maxReconnectDelay = 1000 * 30;
    private double backOffMultiplier = 2d;
    private long timeout = -1;
    private boolean useExponentialBackOff = true;
    private boolean randomize = true;
    private boolean initialized;
    private int maxReconnectAttempts;
    private int connectFailures;
    private long reconnectDelay = this.initialReconnectDelay;
    private Exception connectionFailure;
    private boolean firstConnection = true;
    //optionally always have a backup created
    private boolean backup=false;
    private List<BackupTransport> backups=new CopyOnWriteArrayList<BackupTransport>();
    private int backupPoolSize=1;
    private boolean trackMessages = false;
    private int maxCacheSize = 128 * 1024;
    private TransportListener disposedListener = new DefaultTransportListener() {};
    

    private final TransportListener myTransportListener = createTransportListener();

    public FailoverTransport() throws InterruptedIOException {

        stateTracker.setTrackTransactions(true);
        // Setup a task that is used to reconnect the a connection async.
        reconnectTask = DefaultThreadPools.getDefaultTaskRunnerFactory().createTaskRunner(new Task() {
            public boolean iterate() {
            	boolean result=false;
            	boolean buildBackup=true;
            	boolean doReconnect = !disposed;
            	synchronized(backupMutex) {
                	if (connectedTransport.get()==null && !disposed) {
                		result=doReconnect();
                		buildBackup=false;
                	}
            	}
            	if(buildBackup) {
            		buildBackups();
            	}else {
            		//build backups on the next iteration
            		result=true;
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
                Command command = (Command)o;
                if (command == null) {
                    return;
                }
                if (command.isResponse()) {
                    Object object = null;
                    synchronized(requestMap) {
                     object = requestMap.remove(Integer.valueOf(((Response)command).getCorrelationId()));
                    }
                    if (object != null && object.getClass() == Tracked.class) {
                        ((Tracked)object).onResponses();
                    }
                }
                if (!initialized) {
                    if (command.isBrokerInfo()) {
                        BrokerInfo info = (BrokerInfo)command;
                        BrokerInfo[] peers = info.getPeerBrokerInfos();
                        if (peers != null) {
                            for (int i = 0; i < peers.length; i++) {
                                String brokerString = peers[i].getBrokerURL();
                                add(brokerString);
                            }
                        }
                        initialized = true;
                    }

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


    public final void handleTransportFailure(IOException e) throws InterruptedException {
        
        Transport transport = connectedTransport.getAndSet(null);
        if( transport!=null ) {
            
            transport.setTransportListener(disposedListener);
            ServiceSupport.dispose(transport);
            
            synchronized (reconnectMutex) {
                boolean reconnectOk = false;
                if(started) {
                    LOG.warn("Transport failed to " + connectedTransportURI+ " , attempting to automatically reconnect due to: " + e);
                    LOG.debug("Transport failed with the following exception:", e);
                    reconnectOk = true;
                }
                
                initialized = false;
                failedConnectTransportURI=connectedTransportURI;
                connectedTransportURI = null;
                connected=false;
                if(reconnectOk) {
                    reconnectTask.wakeup();
                }
            }

            if (transportListener != null) {
                transportListener.transportInterupted();
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
            if (connectedTransport.get() != null) {
                stateTracker.restore(connectedTransport.get());
            } else {
                reconnect();
            }
        }
    }

    public void stop() throws Exception {
        Transport transportToStop=null;
        synchronized (reconnectMutex) {
            LOG.debug("Stopped.");
            if (!started) {
                return;
            }
            started = false;
            disposed = true;
            connected = false;
            for (BackupTransport t:backups) {
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
        if( transportToStop!=null ) {
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

    public int getMaxCacheSize() {
        return maxCacheSize;
    }

    public void setMaxCacheSize(int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }
	
    /**
     * @return Returns true if the command is one sent when a connection
     * is being closed.
     */
    private boolean isShutdownCommand(Command command) {
	return (command != null && (command.isShutdownInfo() || command instanceof RemoveInfo));
    }
	 

    public void oneway(Object o) throws IOException {
        
        Command command = (Command)o;
        Exception error = null;
        try {

            synchronized (reconnectMutex) {
            	
                if (isShutdownCommand(command) && connectedTransport.get() == null) {
                    if(command.isShutdownInfo()) {
                        // Skipping send of ShutdownInfo command when not connected.
                        return;
                    }
                    if(command instanceof RemoveInfo) {
                        // Simulate response to RemoveInfo command
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
                        while (transport == null && !disposed
                                && connectionFailure == null
                                && !Thread.currentThread().isInterrupted()) {
                            LOG.trace("Waiting for transport to reconnect.");
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
                            }else {
                                error = new IOException("Unexpected failure.");
                            }
                            break;
                        }

                        // If it was a request and it was not being tracked by
                        // the state tracker,
                        // then hold it in the requestMap so that we can replay
                        // it later.
                        Tracked tracked = stateTracker.track(command);
                        synchronized(requestMap) {
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
                    throw (IOException)error;
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

    public void add(URI u[]) {
        for (int i = 0; i < u.length; i++) {
            if (!uris.contains(u[i])) {
                uris.add(u[i]);
            }
        }
        reconnect();
    }

    public void remove(URI u[]) {
        for (int i = 0; i < u.length; i++) {
            uris.remove(u[i]);
        }
        reconnect();
    }

    public void add(String u) {
        try {
            URI uri = new URI(u);
            if (!uris.contains(uri)) {
                uris.add(uri);
            }

            reconnect();
        } catch (Exception e) {
            LOG.error("Failed to parse URI: " + u);
        }
    }

    public void reconnect() {
        synchronized (reconnectMutex) {
            if (started) {
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
                int p = (int) (Math.random()*100 % l.size());
                URI t = l.get(p);
                l.set(p, l.get(i));
                l.set(i, t);
            }
        }
        if (removed) {
            l.add(failedConnectTransportURI);
        }
        LOG.debug("urlList connectionList:" + l);
        return l;
    }

    public TransportListener getTransportListener() {
        return transportListener;
    }

    public void setTransportListener(TransportListener commandListener) {
        synchronized(listenerMutex) {
            this.transportListener = commandListener;
            listenerMutex.notifyAll();
        }
    }

    public <T> T narrow(Class<T> target) {

        if (target.isAssignableFrom(getClass())) {
            return target.cast(this);
        }
        Transport transport = connectedTransport.get();
        if ( transport != null) {
            return transport.narrow(target);
        }
        return null;

    }

    protected void restoreTransport(Transport t) throws Exception, IOException {
        t.start();
        //send information to the broker - informing it we are an ft client
        ConnectionControl cc = new ConnectionControl();
        cc.setFaultTolerant(true);
        t.oneway(cc);
        stateTracker.restore(t);
        Map tmpMap = null;
        synchronized(requestMap) {
            tmpMap = new LinkedHashMap<Integer, Command>(requestMap);
        }
        for (Iterator<Command> iter2 = tmpMap.values().iterator(); iter2.hasNext();) {
            Command command = iter2.next();
            if (LOG.isTraceEnabled()) {
                LOG.trace("restore, replay: " + command);
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

    public String toString() {
        return connectedTransportURI == null ? "unconnected" : connectedTransportURI.toString();
    }

    public String getRemoteAddress() {
        Transport transport = connectedTransport.get();
        if ( transport != null) {
            return transport.getRemoteAddress();
        }
        return null;
    }

    public boolean isFaultTolerant() {
        return true;
    }
    
   final boolean doReconnect() {
        Exception failure = null;
        synchronized (reconnectMutex) {

            if (disposed || connectionFailure != null) {
                reconnectMutex.notifyAll();
            }

            if (connectedTransport.get() != null || disposed || connectionFailure != null) {
                return false;
            } else {
                List<URI> connectList = getConnectList();
                if (connectList.isEmpty()) {
                    failure = new IOException("No uris available to connect to.");
                } else {
                    if (!useExponentialBackOff) {
                        reconnectDelay = initialReconnectDelay;
                    }
                    synchronized(backupMutex) {
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
                                failedConnectTransportURI=null;
                                connectedTransportURI = uri;
                                connectedTransport.set(t);
                                reconnectMutex.notifyAll();
                                connectFailures = 0;
                                LOG.info("Successfully reconnected to backup " + uri);
                                return false;
                            }catch (Exception e) {
                                LOG.debug("Backup transport failed",e);
                             }
                        }
                    }
                    
                    Iterator<URI> iter = connectList.iterator();
                    while(iter.hasNext() && connectedTransport.get() == null && !disposed) {
                        URI uri = iter.next();
                        Transport t = null;
                        try {
                            LOG.debug("Attempting connect to: " + uri);
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
                         // Make sure on initial startup, that the transportListener 
                         // has been initialized for this instance.
                            synchronized(listenerMutex) {
                                if (transportListener==null) {
                                    try {
                                        //if it isn't set after 2secs - it
                                        //probably never will be
                                        listenerMutex.wait(2000);
                                    }catch(InterruptedException ex) {}
                                }
                            }
                            if (transportListener != null) {
                                transportListener.transportResumed();
                            }else {
                                LOG.debug("transport resumed by transport listener not set");
                            }
                            if (firstConnection) {
                                firstConnection=false;
                                LOG.info("Successfully connected to " + uri);
                            }else {
                                LOG.info("Successfully reconnected to " + uri);
                            }
                            connected=true;
                            return false;
                        } catch (Exception e) {
                            failure = e;
                            LOG.debug("Connect fail to: " + uri + ", reason: " + e);
                            if (t!=null) {
                                try {
                                    t.stop();       
                                } catch (Exception ee) {
                                    LOG.debug("Stop of failed transport: " + t + " failed with reason: " + ee);
                                }
                            }
                        }
                    }
                }
            }
            
            if (maxReconnectAttempts > 0 && ++connectFailures >= maxReconnectAttempts) {
                LOG.error("Failed to connect to transport after: " + connectFailures + " attempt(s)");
                connectionFailure = failure;
 	
                // Make sure on initial startup, that the transportListener has been initialized
                // for this instance.
                synchronized(listenerMutex) {
                    if (transportListener==null) {
                        try {
                            listenerMutex.wait(2000);
                        }catch(InterruptedException ex) {}
                    }
                }

          
                if(transportListener != null) {
                    if (connectionFailure instanceof IOException) {
                    	transportListener.onException((IOException)connectionFailure);
                    } else {
                    	transportListener.onException(IOExceptionSupport.create(connectionFailure));
                    }
                }        
                reconnectMutex.notifyAll();
                return false;
            }
        }
        if (!disposed) {

            LOG.debug("Waiting " + reconnectDelay + " ms before attempting connection. ");
            synchronized (sleepMutex) {
                try {
                    sleepMutex.wait(reconnectDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
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
			   //removed disposed backups
			   List<BackupTransport>disposedList = new ArrayList<BackupTransport>();
			   for (BackupTransport bt:backups) {
				   if (bt.isDisposed()) {
					   disposedList.add(bt);
				   }
			   }
			   backups.removeAll(disposedList);
			   disposedList.clear();
			   for (Iterator<URI>iter = connectList.iterator();iter.hasNext() && backups.size() < backupPoolSize;) {
				   URI uri = iter.next();
				   if (connectedTransportURI != null && !connectedTransportURI.equals(uri)) {
					   try {
						   BackupTransport bt = new BackupTransport(this);
						   bt.setUri(uri);
						   if (!backups.contains(bt)) {
							   Transport t = TransportFactory.compositeConnect(uri);
		                       t.setTransportListener(bt);
		                       t.start();
		                       bt.setTransport(t);
		                       backups.add(bt);
						   }
					   }catch(Exception e) {
						   LOG.debug("Failed to build backup ",e);
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
    	add(new URI[] {uri});
    }
}
