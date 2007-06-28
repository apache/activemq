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
package org.apache.activemq.network;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.NetworkBridgeFilter;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.filter.DestinationFilter;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportDisposedIOException;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A useful base class for implementing demand forwarding bridges.
 * 
 * @version $Revision$
 */
public abstract class DemandForwardingBridgeSupport implements NetworkBridge {
    protected static final Log log = LogFactory.getLog(DemandForwardingBridge.class);
    protected final Transport localBroker;
    protected final Transport remoteBroker;
    protected final IdGenerator idGenerator = new IdGenerator();
    protected final LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    protected ConnectionInfo localConnectionInfo;
    protected ConnectionInfo remoteConnectionInfo;
    protected SessionInfo localSessionInfo;
    protected ProducerInfo producerInfo;
    protected String remoteBrokerName = "Unknown";
    protected String localClientId;
    protected ConsumerInfo demandConsumerInfo;
    protected int demandConsumerDispatched;
    protected final AtomicBoolean localBridgeStarted = new AtomicBoolean(false);
    protected final AtomicBoolean remoteBridgeStarted = new AtomicBoolean(false);
    protected boolean disposed = false;
    protected BrokerId localBrokerId;
    protected ActiveMQDestination[] excludedDestinations;
    protected ActiveMQDestination[] dynamicallyIncludedDestinations;
    protected ActiveMQDestination[] staticallyIncludedDestinations;
    protected ActiveMQDestination[] durableDestinations;
    protected final ConcurrentHashMap subscriptionMapByLocalId = new ConcurrentHashMap();
    protected final ConcurrentHashMap subscriptionMapByRemoteId = new ConcurrentHashMap();
    protected final BrokerId localBrokerPath[] = new BrokerId[] { null };
    protected CountDownLatch startedLatch = new CountDownLatch(2);
    protected CountDownLatch remoteBrokerNameKnownLatch = new CountDownLatch(1);
    protected final AtomicBoolean remoteInterupted = new AtomicBoolean(false);
    protected final AtomicBoolean lastConnectSucceeded = new AtomicBoolean(false);
    protected NetworkBridgeConfiguration configuration;
    private NetworkBridgeListener networkBridgeListener;
    private boolean createdByDuplex;

    private BrokerInfo localBrokerInfo;
    private BrokerInfo remoteBrokerInfo;

    final AtomicLong enqueueCounter = new AtomicLong();
    final AtomicLong dequeueCounter = new AtomicLong();
    
    public DemandForwardingBridgeSupport(NetworkBridgeConfiguration configuration, Transport localBroker, Transport remoteBroker) {
        this.configuration=configuration;
        this.localBroker = localBroker;
        this.remoteBroker = remoteBroker;
    }

    public void start() throws Exception {
        localBroker.setTransportListener(new DefaultTransportListener(){
            public void onCommand(Object o){
            	Command command = (Command) o;
                serviceLocalCommand(command);
            }
    
            public void onException(IOException error){
                serviceLocalException(error);
            }
        });
        remoteBroker.setTransportListener(new TransportListener(){
            public void onCommand(Object o){
            	Command command = (Command) o;
                serviceRemoteCommand(command);
            }
    
            public void onException(IOException error){
                serviceRemoteException(error);
            }
    
            public void transportInterupted(){
                //clear any subscriptions - to try and prevent the bridge from stalling the broker
                if( remoteInterupted.compareAndSet(false, true) ) {
                	
                    log.info("Outbound transport to " + remoteBrokerName + " interrupted.");                                        

                	if( localBridgeStarted.get() ) {
	                    clearDownSubscriptions();
	                    synchronized( DemandForwardingBridgeSupport.this ) {
		                    try{
		                        localBroker.oneway(localConnectionInfo.createRemoveCommand());
		                    }catch(TransportDisposedIOException td){
                                log.debug("local broker is now disposed",td);
                            }
                            catch(IOException e){
		                        log.warn("Caught exception from local start",e);
		                    }
	                    }
                	}
                	
                    localBridgeStarted.set(false);
                    remoteBridgeStarted.set(false);
                    startedLatch = new CountDownLatch(2);
                }
                
            }
    
            public void transportResumed(){                
                if( remoteInterupted.compareAndSet(true, false) ) {
                	
                	// We want to slow down false connects so that we don't get in a busy loop.
                	// False connects can occurr if you using SSH tunnels.
                	if( !lastConnectSucceeded.get() ) {
                		try {
                            log.debug("Previous connection was never fully established. Sleeping for second to avoid busy loop.");                                        
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
                	}
                    lastConnectSucceeded.set(false);
                    try {
                        startLocalBridge();
                        remoteBridgeStarted.set(true);
                        startedLatch.countDown();
                        log.info("Outbound transport to " + remoteBrokerName + " resumed");   
                    }catch(Exception e) {
                        log.error("Caught exception  from local start in resume transport",e );
                    }
                                                         
                }
            }
        });
        localBroker.start();
        remoteBroker.start();
        
        try{                    	
            triggerRemoteStartBridge();
        }catch(IOException e){
            log.warn("Caught exception from remote start",e);
        }
        
        NetworkBridgeListener l = this.networkBridgeListener;
        if (l!=null) {
            l.onStart(this);
        }

    }

    protected void triggerLocalStartBridge() throws IOException {
        Thread thead=new Thread(){
            public void run(){
                try{
                    startLocalBridge();
                }catch(Exception e){
                    serviceLocalException(e);
                }
            }
        };
        thead.start();
    }

    protected void triggerRemoteStartBridge() throws IOException {
        Thread thead=new Thread(){
            public void run(){
                try{
                    startRemoteBridge();
                }catch(Exception e){
                    serviceRemoteException(e);
                }
            }
        };
        thead.start();
    }

    protected void startLocalBridge() throws Exception {
        if(localBridgeStarted.compareAndSet(false,true)){
            synchronized( this ) {
            	
        		remoteBrokerNameKnownLatch.await();

	            localConnectionInfo=new ConnectionInfo();
	            localConnectionInfo.setConnectionId(new ConnectionId(idGenerator.generateId()));
	            localClientId="NC_"+remoteBrokerName+"_inbound"+configuration.getBrokerName();
	            localConnectionInfo.setClientId(localClientId);
	            localConnectionInfo.setUserName(configuration.getUserName());
	            localConnectionInfo.setPassword(configuration.getPassword());
	            localBroker.oneway(localConnectionInfo);
	
	            localSessionInfo=new SessionInfo(localConnectionInfo,1);
	            localBroker.oneway(localSessionInfo);
	            
	            log.info("Network connection between "+localBroker+" and "+remoteBroker+"("+remoteBrokerName
	                            +") has been established.");
	            
	            startedLatch.countDown();
	            setupStaticDestinations();
            }
        }
    }

    protected void startRemoteBridge() throws Exception {
        if(remoteBridgeStarted.compareAndSet(false,true)) {
    
        	synchronized (this) {
        		        		
            	if( remoteConnectionInfo!=null ) {
            		remoteBroker.oneway(remoteConnectionInfo.createRemoveCommand());
            	}
            	
                remoteConnectionInfo=new ConnectionInfo();
                remoteConnectionInfo.setConnectionId(new ConnectionId(idGenerator.generateId()));
                remoteConnectionInfo.setClientId("NC_"+configuration.getBrokerName()+"_outbound");
                remoteConnectionInfo.setUserName(configuration.getUserName());
                remoteConnectionInfo.setPassword(configuration.getPassword());
                remoteBroker.oneway(remoteConnectionInfo);
                if (isCreatedByDuplex()==false) {
                BrokerInfo brokerInfo=new BrokerInfo();
                brokerInfo.setBrokerName(configuration.getBrokerName());
                brokerInfo.setNetworkConnection(true);
                brokerInfo.setDuplexConnection(configuration.isDuplex());
              
                //set our properties
                Properties props = new Properties();
                IntrospectionSupport.getProperties(this,props,null); 
                String str = MarshallingSupport.propertiesToString(props);
                brokerInfo.setNetworkProperties(str);                
                remoteBroker.oneway(brokerInfo);
                }

                SessionInfo remoteSessionInfo=new SessionInfo(remoteConnectionInfo,1);
                remoteBroker.oneway(remoteSessionInfo);

                producerInfo=new ProducerInfo(remoteSessionInfo,1);
                producerInfo.setResponseRequired(false);
                remoteBroker.oneway(producerInfo);

                // Listen to consumer advisory messages on the remote broker to determine demand.
                demandConsumerInfo=new ConsumerInfo(remoteSessionInfo,1);
                demandConsumerInfo.setDispatchAsync(configuration.isDispatchAsync());
                String advisoryTopic = AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX+configuration.getDestinationFilter();
                if( configuration.isBridgeTempDestinations() ) {
                	advisoryTopic += ","+AdvisorySupport.TEMP_DESTINATION_COMPOSITE_ADVISORY_TOPIC;
                }
                demandConsumerInfo.setDestination(new ActiveMQTopic(advisoryTopic));
                demandConsumerInfo.setPrefetchSize(configuration.getPrefetchSize());
                remoteBroker.oneway(demandConsumerInfo);                
                startedLatch.countDown();
                
                if (!disposed){
                    triggerLocalStartBridge();
                }
        		
        	}
        }
    }

    public void stop() throws Exception{
        log.debug(" stopping "+configuration.getBrokerName()+" bridge to "+remoteBrokerName+" is disposed already ? "+disposed);
        boolean wasDisposedAlready=disposed;
        if(!disposed){
            NetworkBridgeListener l = this.networkBridgeListener;
            if (l!=null) {
                l.onStop(this);
            }

            try{
                disposed=true;
                remoteBridgeStarted.set(false);
                localBroker.oneway(new ShutdownInfo());
                remoteBroker.oneway(new ShutdownInfo());
            }catch(IOException e){
                log.debug("Caught exception stopping",e);
            }finally{
                ServiceStopper ss=new ServiceStopper();
                ss.stop(localBroker);
                ss.stop(remoteBroker);
                ss.throwFirstException();
            }
        }
        if(wasDisposedAlready){
            log.debug(configuration.getBrokerName()+" bridge to "+remoteBrokerName+" stopped");
        }else{
            log.info(configuration.getBrokerName()+" bridge to "+remoteBrokerName+" stopped");
        }
    }
    
    public void serviceRemoteException(Throwable error){
        if(!disposed){
            if(error instanceof SecurityException||error instanceof GeneralSecurityException){
                log.error("Network connection between "+localBroker+" and "+remoteBroker
                        +" shutdown due to a remote error: "+error);
            }else{
                log.warn("Network connection between "+localBroker+" and "+remoteBroker
                        +" shutdown due to a remote error: "+error);
            }
            log.debug("The remote Exception was: "+error,error);
            new Thread(){

                public void run(){
                    ServiceSupport.dispose(DemandForwardingBridgeSupport.this);
                }
            }.start();
            fireBridgeFailed();
        }
    }

    protected void serviceRemoteCommand(Command command) {
        if(!disposed){
            try{
                if(command.isMessageDispatch()){
                    waitStarted();
                    MessageDispatch md=(MessageDispatch) command;
                    serviceRemoteConsumerAdvisory(md.getMessage().getDataStructure());
                    demandConsumerDispatched++;
                    if(demandConsumerDispatched>(demandConsumerInfo.getPrefetchSize()*.75)){
                        remoteBroker.oneway(new MessageAck(md,MessageAck.STANDARD_ACK_TYPE,demandConsumerDispatched));
                        demandConsumerDispatched=0;
                    }
                }else if(command.isBrokerInfo()){
                	
                	lastConnectSucceeded.set(true);
                        remoteBrokerInfo = ((BrokerInfo)command);

                	serviceRemoteBrokerInfo(command);
                	// Let the local broker know the remote broker's ID.
                	localBroker.oneway(command);
                    
                }else if(command.getClass() == ConnectionError.class ) {
                	ConnectionError ce = (ConnectionError) command;
                	serviceRemoteException(ce.getException());
                }else{
                    switch(command.getDataStructureType()){
                    case KeepAliveInfo.DATA_STRUCTURE_TYPE:
                    case WireFormatInfo.DATA_STRUCTURE_TYPE:
                    case ShutdownInfo.DATA_STRUCTURE_TYPE:
                        break;
                    default:
                        log.warn("Unexpected remote command: "+command);
                    }
                }
            }catch(Exception e){
                serviceRemoteException(e);
            }
        }
    }

    private void serviceRemoteConsumerAdvisory(DataStructure data) throws IOException {
       
        final int networkTTL = configuration.getNetworkTTL();
        if(data.getClass()==ConsumerInfo.class){
            // Create a new local subscription
            ConsumerInfo info=(ConsumerInfo) data;
            BrokerId[] path=info.getBrokerPath();
            if((path!=null&&path.length>= networkTTL)){
                if(log.isDebugEnabled())
                    log.debug(configuration.getBrokerName() + " Ignoring Subscription " + info + " restricted to " + networkTTL + " network hops only");
                return;
            }
            if(contains(info.getBrokerPath(),localBrokerPath[0])){
                // Ignore this consumer as it's a consumer we locally sent to the broker.
                if(log.isDebugEnabled())
                    log.debug(configuration.getBrokerName()  + " Ignoring sub " + info + " already routed through this broker once");
                return;
            }
            if (!isPermissableDestination(info.getDestination())){
                //ignore if not in the permited or in the excluded list
                if(log.isDebugEnabled())
                    log.debug(configuration.getBrokerName()  + " Ignoring sub " + info + " destination " + info.getDestination() + " is not permiited");
                return;
            }
            // Update the packet to show where it came from.
            info=info.copy();
            addRemoteBrokerToBrokerPath(info);
            DemandSubscription sub=createDemandSubscription(info);
            if (sub != null){
                addSubscription(sub);
                if(log.isDebugEnabled())
                    log.debug(configuration.getBrokerName() + " Forwarding sub on "+localBroker+" from "+remoteBrokerName+" :  "+info);
            }else {
                if(log.isDebugEnabled())
                    log.debug(configuration.getBrokerName()  + " Ignoring sub " + info + " already subscribed to matching destination");
            }
        }
        else if (data.getClass()==DestinationInfo.class){
//          It's a destination info - we want to pass up
            //infomation about temporary destinations 
            DestinationInfo destInfo = (DestinationInfo) data;
            BrokerId[] path=destInfo.getBrokerPath();
            if((path!=null&&path.length>= networkTTL)){
                if(log.isDebugEnabled())
                    log.debug("Ignoring Subscription " + destInfo + " restricted to " + networkTTL + " network hops only");
                return;
            }
            if(contains(destInfo.getBrokerPath(),localBrokerPath[0])){
                // Ignore this consumer as it's a consumer we locally sent to the broker.
                if(log.isDebugEnabled())
                    log.debug("Ignoring sub " + destInfo + " already routed through this broker once");
                return;
            }
            
            destInfo.setConnectionId(localConnectionInfo.getConnectionId());
            if (destInfo.getDestination() instanceof ActiveMQTempDestination){
                //re-set connection id so comes from here
                ActiveMQTempDestination tempDest = (ActiveMQTempDestination) destInfo.getDestination();
                tempDest.setConnectionId(localSessionInfo.getSessionId().getConnectionId());
            }
                        
            destInfo.setBrokerPath(appendToBrokerPath(destInfo.getBrokerPath(),getRemoteBrokerPath()));
            
            log.debug("Replying destination control command: "+destInfo);            
            localBroker.oneway(destInfo);
            
        }
        else if(data.getClass()==RemoveInfo.class){
            ConsumerId id=(ConsumerId) ((RemoveInfo) data).getObjectId();
            removeDemandSubscription(id);
        }
    }

    public void serviceLocalException(Throwable error) {
    	if( !disposed ) {
	        log.info("Network connection between "+localBroker+" and "+remoteBroker+" shutdown due to a local error: "+error);
	        log.debug("The local Exception was:"+error,error);
	        new Thread() {
	        	public void run() {
	                ServiceSupport.dispose(DemandForwardingBridgeSupport.this);
	        	}
	        }.start();
            fireBridgeFailed();
    	}
    }

    protected void addSubscription(DemandSubscription sub) throws IOException {
        if (sub != null){
            localBroker.oneway(sub.getLocalInfo());
        }
    }

    protected void removeSubscription(DemandSubscription sub) throws IOException {
        if(sub!=null){
            subscriptionMapByLocalId.remove(sub.getLocalInfo().getConsumerId());
            localBroker.oneway(sub.getLocalInfo().createRemoveCommand());
        }
    }

    protected DemandSubscription getDemandSubscription(MessageDispatch md) {
        return (DemandSubscription) subscriptionMapByLocalId.get(md.getConsumerId());
    }

    protected Message configureMessage(MessageDispatch md) {
        Message message=md.getMessage().copy();
        // Update the packet to show where it came from.
        message.setBrokerPath(appendToBrokerPath(message.getBrokerPath(),localBrokerPath));
        message.setProducerId(producerInfo.getProducerId());
        message.setDestination(md.getDestination());
        if(message.getOriginalTransactionId()==null)
            message.setOriginalTransactionId(message.getTransactionId());
        message.setTransactionId(null);
        return message;
    }

    protected void serviceLocalCommand(Command command) {
        if(!disposed){
            final boolean trace=log.isTraceEnabled();
            try{
                if(command.isMessageDispatch()){
                	enqueueCounter.incrementAndGet();
                    waitStarted();
                    final MessageDispatch md=(MessageDispatch) command;
                    DemandSubscription sub=(DemandSubscription) subscriptionMapByLocalId.get(md.getConsumerId());
                    if(sub!=null){
                        Message message= configureMessage(md);
                        if(trace)
                            log.trace("bridging "+configuration.getBrokerName()+" -> "+remoteBrokerName+": "+message);
                        
                        
                        
                        if( !message.isResponseRequired() ) {
                            
                            // If the message was originally sent using async send, we will preserve that QOS
                            // by bridging it using an async send (small chance of message loss).
                            remoteBroker.oneway(message);
                            localBroker.oneway(new MessageAck(md,MessageAck.STANDARD_ACK_TYPE,1));
                            dequeueCounter.incrementAndGet();
                            
                        } else {
                            
                            // The message was not sent using async send, so we should only ack the local 
                            // broker when we get confirmation that the remote broker has received the message.
                            ResponseCallback callback = new ResponseCallback() {
                                public void onCompletion(FutureResponse future) {
                                    try {
                                        Response response = future.getResult();
                                        if(response.isException()){
                                            ExceptionResponse er=(ExceptionResponse) response;
                                            serviceLocalException(er.getException());
                                        } else {
                                            localBroker.oneway(new MessageAck(md,MessageAck.STANDARD_ACK_TYPE,1));
                                            dequeueCounter.incrementAndGet();
                                        }
                                    } catch (IOException e) {
                                        serviceLocalException(e);
                                    }
                                }
                            };

                            remoteBroker.asyncRequest(message, callback);
                        }
                        
                    }else{
                        if (trace)log.trace("No subscription registered with this network bridge for consumerId " + md.getConsumerId() + " for message: " + md.getMessage());
                    } 
                }else if(command.isBrokerInfo()){
                	localBrokerInfo = ((BrokerInfo)command);
                    serviceLocalBrokerInfo(command);
                }else if(command.isShutdownInfo()){
                    log.info(configuration.getBrokerName()+" Shutting down");
                    // Don't shut down the whole connector if the remote side was interrupted.
                    // the local transport is just shutting down temporarily until the remote side
                    // is restored.
                    if( !remoteInterupted.get() ) { 
                        stop();
                    }
                }else if(command.getClass() == ConnectionError.class ) {
                	ConnectionError ce = (ConnectionError) command;
                	serviceLocalException(ce.getException());                    
                }else{
                    switch(command.getDataStructureType()){
                    case WireFormatInfo.DATA_STRUCTURE_TYPE:
                        break;
                    default:
                        log.warn("Unexpected local command: "+command);
                    }
                }
            }catch(Exception e){
                serviceLocalException(e);
            }
        }
    }

    /**
     * @return Returns the dynamicallyIncludedDestinations.
     */
    public ActiveMQDestination[] getDynamicallyIncludedDestinations() {
        return dynamicallyIncludedDestinations;
    }

    /**
     * @param dynamicallyIncludedDestinations The dynamicallyIncludedDestinations to set.
     */
    public void setDynamicallyIncludedDestinations(ActiveMQDestination[] dynamicallyIncludedDestinations) {
        this.dynamicallyIncludedDestinations=dynamicallyIncludedDestinations;
    }

    /**
     * @return Returns the excludedDestinations.
     */
    public ActiveMQDestination[] getExcludedDestinations() {
        return excludedDestinations;
    }

    /**
     * @param excludedDestinations The excludedDestinations to set.
     */
    public void setExcludedDestinations(ActiveMQDestination[] excludedDestinations) {
        this.excludedDestinations=excludedDestinations;
    }

    /**
     * @return Returns the staticallyIncludedDestinations.
     */
    public ActiveMQDestination[] getStaticallyIncludedDestinations() {
        return staticallyIncludedDestinations;
    }

    /**
     * @param staticallyIncludedDestinations The staticallyIncludedDestinations to set.
     */
    public void setStaticallyIncludedDestinations(ActiveMQDestination[] staticallyIncludedDestinations) {
        this.staticallyIncludedDestinations=staticallyIncludedDestinations;
    }

    /**
     * @return Returns the durableDestinations.
     */
    public ActiveMQDestination[] getDurableDestinations() {
        return durableDestinations;
    }

    /**
     * @param durableDestinations The durableDestinations to set.
     */
    public void setDurableDestinations(ActiveMQDestination[] durableDestinations) {
        this.durableDestinations=durableDestinations;
    }

    /**
     * @return Returns the localBroker.
     */
    public Transport getLocalBroker() {
        return localBroker;
    }

    /**
     * @return Returns the remoteBroker.
     */
    public Transport getRemoteBroker() {
        return remoteBroker;
    }
    
    /**
     * @return the createdByDuplex
     */
    public boolean isCreatedByDuplex(){
        return this.createdByDuplex;
    }
    
    /**
     * @param createdByDuplex the createdByDuplex to set
     */
    public void setCreatedByDuplex(boolean createdByDuplex){
        this.createdByDuplex=createdByDuplex;
    }
  
    public static boolean contains(BrokerId[] brokerPath, BrokerId brokerId) {
        if(brokerPath!=null){
            for(int i=0;i<brokerPath.length;i++){
                if(brokerId.equals(brokerPath[i]))
                    return true;
            }
        }
        return false;
    }

    protected BrokerId[] appendToBrokerPath(BrokerId[] brokerPath, BrokerId[] pathsToAppend) {
        if (brokerPath == null || brokerPath.length == 0)
            return pathsToAppend;
        BrokerId rc[] = new BrokerId[brokerPath.length + pathsToAppend.length];
        System.arraycopy(brokerPath, 0, rc, 0, brokerPath.length);
        System.arraycopy(pathsToAppend, 0, rc, brokerPath.length, pathsToAppend.length);
        return rc;
    }

    protected BrokerId[] appendToBrokerPath(BrokerId[] brokerPath, BrokerId idToAppend) {
        if (brokerPath == null || brokerPath.length == 0)
            return new BrokerId[] { idToAppend };
        BrokerId rc[] = new BrokerId[brokerPath.length + 1];
        System.arraycopy(brokerPath, 0, rc, 0, brokerPath.length);
        rc[brokerPath.length] = idToAppend;
        return rc;
    }

    protected boolean isPermissableDestination(ActiveMQDestination destination) {
    	// Are we not bridging temp destinations?
    	if( destination.isTemporary() && !configuration.isBridgeTempDestinations() ) {
    		return false;
        }
    	
        DestinationFilter filter=DestinationFilter.parseFilter(destination);
        ActiveMQDestination[] dests = excludedDestinations;
        if(dests!=null&&dests.length>0){
            for(int i=0;i<dests.length;i++){
                ActiveMQDestination match=dests[i];
                if(match!=null&&filter.matches(match)){
                    return false;
                }
            }
        }
        dests = dynamicallyIncludedDestinations;
        if(dests!=null&&dests.length>0){
            for(int i=0;i<dests.length;i++){
                ActiveMQDestination match=dests[i];
                if(match!=null&&filter.matches(match)){
                    return true;
                }
            }
            return false;
        }
    
        return true;
    }

    /**
     * Subscriptions for these desitnations are always created
     *
     */
    protected void setupStaticDestinations() {
        ActiveMQDestination[] dests = staticallyIncludedDestinations;
        if (dests != null){
            for(int i=0;i<dests.length;i++){
                ActiveMQDestination dest=dests[i];
                DemandSubscription sub = createDemandSubscription(dest);
                try{
                    addSubscription(sub);
                }catch(IOException e){
                   log.error("Failed to add static destination " + dest,e);
                }
                if(log.isTraceEnabled())
                    log.trace("Forwarding messages for static destination: " + dest);
            } 
        }
    }

    protected DemandSubscription createDemandSubscription(ConsumerInfo info) throws IOException {
        return doCreateDemandSubscription(info);
    }

    protected DemandSubscription doCreateDemandSubscription(ConsumerInfo info) throws IOException {
        DemandSubscription result=new DemandSubscription(info);
        result.getLocalInfo().setConsumerId(new ConsumerId(localSessionInfo.getSessionId(),consumerIdGenerator
                        .getNextSequenceId()));
        
        if( configuration.isDecreaseNetworkConsumerPriority() ) {
            byte priority=ConsumerInfo.NETWORK_CONSUMER_PRIORITY;
            if(priority>Byte.MIN_VALUE&&info.getBrokerPath()!=null&&info.getBrokerPath().length>1){
                // The longer the path to the consumer, the less it's consumer priority.
                priority-=info.getBrokerPath().length+1;
            }
            result.getLocalInfo().setPriority(priority);
        }
        configureDemandSubscription(info, result);
        return result;
    }

    protected DemandSubscription createDemandSubscription(ActiveMQDestination destination) {
        ConsumerInfo info = new ConsumerInfo();
        info.setDestination(destination);
        //the remote info held by the DemandSubscription holds the original consumerId,
        //the local info get's overwritten
        info.setConsumerId(new ConsumerId(localSessionInfo.getSessionId(),consumerIdGenerator
                        .getNextSequenceId()));
        DemandSubscription result=new DemandSubscription(info);
        result.getLocalInfo().setPriority(ConsumerInfo.NETWORK_CONSUMER_PRIORITY);
        return result;
    }

    protected void configureDemandSubscription(ConsumerInfo info, DemandSubscription sub) throws IOException {
        sub.getLocalInfo().setDispatchAsync(configuration.isDispatchAsync());
        sub.getLocalInfo().setPrefetchSize(configuration.getPrefetchSize());
        subscriptionMapByLocalId.put(sub.getLocalInfo().getConsumerId(),sub);
        subscriptionMapByRemoteId.put(sub.getRemoteInfo().getConsumerId(),sub);
    
        // This works for now since we use a VM connection to the local broker.
        // may need to change if we ever subscribe to a remote broker.
        sub.getLocalInfo().setAdditionalPredicate(createNetworkBridgeFilter(info));
    }

    protected void removeDemandSubscription(ConsumerId id) throws IOException {
        DemandSubscription sub=(DemandSubscription) subscriptionMapByRemoteId.remove(id);
        if (sub != null){
            removeSubscription(sub);
            if(log.isTraceEnabled())
                log.trace("removing sub on "+localBroker+" from "+remoteBrokerName+" :  "+sub.getRemoteInfo());
        }
    }

    protected void waitStarted() throws InterruptedException {
        startedLatch.await();
    }

    protected void clearDownSubscriptions() {
        subscriptionMapByLocalId.clear();
        subscriptionMapByRemoteId.clear();
    }

    protected abstract NetworkBridgeFilter createNetworkBridgeFilter(ConsumerInfo info) throws IOException;

    protected abstract void serviceLocalBrokerInfo(Command command) throws InterruptedException;

    protected abstract void addRemoteBrokerToBrokerPath(ConsumerInfo info) throws IOException;

    protected abstract void serviceRemoteBrokerInfo(Command command) throws IOException;
    
    protected abstract BrokerId[] getRemoteBrokerPath();
    
    public void setNetworkBridgeListener(NetworkBridgeListener listener){
        this.networkBridgeListener=listener;  
      }
      
      private void fireBridgeFailed() {
          NetworkBridgeListener l = this.networkBridgeListener;
          if (l!=null) {
              l.bridgeFailed();
          }
      }

	public String getRemoteAddress() {
 		return remoteBroker.getRemoteAddress();
 	}
 
 	public String getLocalAddress() {
 		return localBroker.getRemoteAddress();
 	}
 
 	public String getRemoteBrokerName() {
 		return remoteBrokerInfo == null ? null : remoteBrokerInfo.getBrokerName();
 	}
 	
	public String getLocalBrokerName() {
 		return localBrokerInfo == null ? null : localBrokerInfo.getBrokerName();
	}

 	public long getDequeueCounter() {
 		return dequeueCounter.get();
 	}
 
 	public long getEnqueueCounter() {
 		return enqueueCounter.get();
	}

}
