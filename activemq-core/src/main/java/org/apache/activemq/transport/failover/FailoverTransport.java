/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Random;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.Response;
import org.apache.activemq.state.ConnectionStateTracker;
import org.apache.activemq.state.Tracked;
import org.apache.activemq.thread.DefaultThreadPools;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.transport.CompositeTransport;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

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
    private final Object sleepMutex = new Object();
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
    private boolean randomize = true;
    private boolean initialized;
    private int maxReconnectAttempts;
    private int connectFailures;
    private long reconnectDelay = initialReconnectDelay;
    private Exception connectionFailure;

    private final TransportListener myTransportListener = createTransportListener();
    
    TransportListener createTransportListener() {
    	return new TransportListener() {
	        public void onCommand(Object o) {
            	Command command = (Command) o;
	            if (command == null) {
	                return;
	            }
	            if (command.isResponse()) {
                    Object object = requestMap.remove(Integer.valueOf(((Response) command).getCorrelationId()));
                    if( object!=null && object.getClass() == Tracked.class ) {
                	   ((Tracked)object).onResponses();
                    }
	            }
	            if (!initialized){
	                if (command.isBrokerInfo()){
	                    BrokerInfo info = (BrokerInfo)command;
	                    BrokerInfo[] peers = info.getPeerBrokerInfos();
	                    if (peers!= null){
	                        for (int i =0; i < peers.length;i++){
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
	            }
	            catch (InterruptedException e) {
	                Thread.currentThread().interrupt();
	                transportListener.onException(new InterruptedIOException());
	            }
	        }
	        
	        public void transportInterupted(){
	            if (transportListener != null){
	                transportListener.transportInterupted();
	            }
	        }
	
	        public void transportResumed(){
	            if(transportListener != null){
	                transportListener.transportResumed();
	            }
	        }
	    };
    }
    
    public FailoverTransport() throws InterruptedIOException {

    	stateTracker.setTrackTransactions(true);
    	
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
                            if (!useExponentialBackOff){
                                reconnectDelay = initialReconnectDelay;
                            }
                            Iterator iter = connectList.iterator();
                            for (int i = 0; iter.hasNext() && connectedTransport == null && !disposed; i++) {
                                URI uri = (URI) iter.next();
                                try {
                                    log.debug("Attempting connect to: " + uri);
                                    Transport t = TransportFactory.compositeConnect(uri);
                                    t.setTransportListener(myTransportListener);
                                    t.start();
                                    
                                    if (started) {
                                        restoreTransport(t);
                                    }
                                    
                                    log.debug("Connection established");
                                    reconnectDelay = initialReconnectDelay;
                                    connectedTransportURI = uri;
                                    connectedTransport = t;
                                    reconnectMutex.notifyAll();
                                    connectFailures = 0;
                                    if (transportListener != null){
                                        transportListener.transportResumed();
                                    }
                                    log.info("Successfully reconnected to " + uri);
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

                if(!disposed){
                    
                        log.debug("Waiting "+reconnectDelay+" ms before attempting connection. ");
                        synchronized(sleepMutex){
                            try{
                                sleepMutex.wait(reconnectDelay);
                            }catch(InterruptedException e){
                               Thread.currentThread().interrupt();
                            }
                        }
                        
                    
                    if(useExponentialBackOff){
                        // Exponential increment of reconnect delay.
                        reconnectDelay*=backOffMultiplier;
                        if(reconnectDelay>maxReconnectDelay)
                            reconnectDelay=maxReconnectDelay;
                    }
                }
                return !disposed;
            }

        }, "ActiveMQ Failover Worker: "+System.identityHashCode(this));
    }

    final void handleTransportFailure(IOException e) throws InterruptedException {
        if (transportListener != null){
            transportListener.transportInterupted();
        }
        synchronized (reconnectMutex) {
            log.warn("Transport failed, attempting to automatically reconnect due to: " + e, e);
            if (connectedTransport != null) {
                initialized = false;
                ServiceSupport.dispose(connectedTransport);
                connectedTransport = null;
                connectedTransportURI = null;
            }
            reconnectTask.wakeup();
        }
    }

    public void start() throws Exception {
        synchronized (reconnectMutex) {
            log.debug("Started.");
            if (started)
                return;
            started = true;
            if (connectedTransport != null) {
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
                connectedTransport=null;
            }
            reconnectMutex.notifyAll();
        }
        synchronized(sleepMutex){
            sleepMutex.notifyAll();
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

    /**
     * @return Returns the randomize.
     */
    public boolean isRandomize(){
        return randomize;
    }

    /**
     * @param randomize The randomize to set.
     */
    public void setRandomize(boolean randomize){
        this.randomize=randomize;
    }

    public void oneway(Object o) throws IOException {
    	Command command = (Command) o;
        Exception error = null;
        try {

            synchronized (reconnectMutex) {
                // Keep trying until the message is sent.
                for (int i = 0;!disposed; i++) {
                    try {

                        // Wait for transport to be connected.
                        while (connectedTransport == null && !disposed && connectionFailure==null ) {
                            log.trace("Waiting for transport to reconnect.");
                            try {
                                reconnectMutex.wait(1000);
                            }
                            catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                log.debug("Interupted: " + e, e);
                            }
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

                        // If it was a request and it was not being tracked by
                        // the state tracker,
                        // then hold it in the requestMap so that we can replay
                        // it later.
                        Tracked tracked = stateTracker.track(command);
                        if( tracked!=null && tracked.isWaitingForResponse() ) {
                            requestMap.put(Integer.valueOf(command.getCommandId()), tracked);
                        } else if ( tracked==null && command.isResponseRequired()) {
                            requestMap.put(Integer.valueOf(command.getCommandId()), command);
                        }
                                                
                        // Send the message.
                        try {
                            connectedTransport.oneway(command);
                        } catch (IOException e) {
                        	
                        	// If the command was not tracked.. we will retry in this method
                        	if( tracked==null ) {
                        		
                        		// since we will retry in this method.. take it out of the request
                        		// map so that it is not sent 2 times on recovery
                            	if( command.isResponseRequired() ) {
                            		requestMap.remove(Integer.valueOf(command.getCommandId()));
                            	}
                            	
                                // Rethrow the exception so it will handled by the outer catch
                                throw e;
                        	}
                        	
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
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
        if(!disposed){
            if(error!=null){
                if(error instanceof IOException)
                    throw (IOException) error;
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
    
    public Object request(Object command,int timeout) throws IOException {
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
    
    public void add(String u){
        try {
        URI uri = new URI(u);
        if (!uris.contains(uri))
            uris.add(uri);

        reconnect();
        }catch(Exception e){
            log.error("Failed to parse URI: " + u);
        }
    }


    public void reconnect() {
        log.debug("Waking up reconnect task");
        try {
            reconnectTask.wakeup();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ArrayList getConnectList(){
        ArrayList l=new ArrayList(uris);
        if (randomize){
            // Randomly, reorder the list by random swapping
            Random r=new Random();
            r.setSeed(System.currentTimeMillis());
            for (int i=0;i<l.size();i++){
                int p=r.nextInt(l.size());
                Object t=l.get(p);
                l.set(p,l.get(i));
                l.set(i,t);
            }
        }
        return l;
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

	public String getRemoteAddress() {
		if(connectedTransport != null){
			return connectedTransport.getRemoteAddress();
		}
		return null;
	}

}
