/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.network;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.ExceptionResponse;
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
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * A useful base class for implementing demand forwarding bridges.
 * 
 * @version $Revision$
 */
public abstract class DemandForwardingBridgeSupport implements Bridge {
    protected static final Log log = LogFactory.getLog(DemandForwardingBridge.class);
    protected final Transport localBroker;
    protected final Transport remoteBroker;
    protected IdGenerator idGenerator = new IdGenerator();
    protected LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    protected ConnectionInfo localConnectionInfo;
    protected ConnectionInfo remoteConnectionInfo;
    protected SessionInfo localSessionInfo;
    protected ProducerInfo producerInfo;
    protected String localBrokerName;
    protected String remoteBrokerName;
    protected String localClientId;
    protected int prefetchSize = 1000;
    protected boolean dispatchAsync;
    protected String destinationFilter = ">";
    protected String name = "bridge";
    protected ConsumerInfo demandConsumerInfo;
    protected int demandConsumerDispatched;
    protected AtomicBoolean localBridgeStarted = new AtomicBoolean(false);
    protected AtomicBoolean remoteBridgeStarted = new AtomicBoolean(false);
    protected boolean disposed = false;
    protected BrokerId localBrokerId;
    protected ActiveMQDestination[] excludedDestinations;
    protected ActiveMQDestination[] dynamicallyIncludedDestinations;
    protected ActiveMQDestination[] staticallyIncludedDestinations;
    protected ActiveMQDestination[] durableDestinations;
    protected ConcurrentHashMap subscriptionMapByLocalId = new ConcurrentHashMap();
    protected ConcurrentHashMap subscriptionMapByRemoteId = new ConcurrentHashMap();
    protected final BrokerId localBrokerPath[] = new BrokerId[] { null };
    protected CountDownLatch startedLatch = new CountDownLatch(2);
    protected boolean decreaseNetworkConsumerPriority;
    protected boolean shutDown;
    protected int networkTTL = 1;

    
    public DemandForwardingBridgeSupport(final Transport localBroker, final Transport remoteBroker) {
        this.localBroker = localBroker;
        this.remoteBroker = remoteBroker;
    }

    public void start() throws Exception {
        log.info("Starting a network connection between "+localBroker+" and "+remoteBroker+" has been established.");
        localBroker.setTransportListener(new DefaultTransportListener(){
            public void onCommand(Command command){
                serviceLocalCommand(command);
            }
    
            public void onException(IOException error){
                serviceLocalException(error);
            }
        });
        remoteBroker.setTransportListener(new TransportListener(){
            public void onCommand(Command command){
                serviceRemoteCommand(command);
            }
    
            public void onException(IOException error){
                serviceRemoteException(error);
            }
    
            public void transportInterupted(){
                //clear any subscriptions - to try and prevent the bridge from stalling the broker
                log.warn("Outbound transport to " + remoteBrokerName +  " interrupted ...");
                clearDownSubscriptions();
                
            }
    
            public void transportResumed(){
                //restart and static subscriptions - the consumer advisories will be replayed
                log.info("Outbound transport to " + remoteBrokerName + " resumed");
                setupStaticDestinations();
                
            }
        });
        localBroker.start();
        remoteBroker.start();
        triggerRemoteStartBridge();
    }

    protected void triggerLocalStartBridge() throws IOException {
        Thread thead=new Thread(){
            public void run(){
                try{
                    startLocalBridge();
                }catch(IOException e){
                    log.error("Failed to start network bridge: "+e,e);
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
                }catch(IOException e){
                    log.error("Failed to start network bridge: "+e,e);
                }
            }
        };
        thead.start();
    }

    protected void startLocalBridge() throws IOException {
        if(localBridgeStarted.compareAndSet(false,true)){
            localConnectionInfo=new ConnectionInfo();
            localConnectionInfo.setConnectionId(new ConnectionId(idGenerator.generateId()));
            localClientId="NC_"+remoteBrokerName+"_inbound"+name;
            localConnectionInfo.setClientId(localClientId);
            localBroker.oneway(localConnectionInfo);

            localSessionInfo=new SessionInfo(localConnectionInfo,1);
            localBroker.oneway(localSessionInfo);
            
            log.info("Network connection between "+localBroker+" and "+remoteBroker+"("+remoteBrokerName
                            +") has been established.");
            startedLatch.countDown();
            setupStaticDestinations();
        }
    }

    protected void startRemoteBridge() throws IOException {
        if(remoteBridgeStarted.compareAndSet(false,true)){

            remoteConnectionInfo=new ConnectionInfo();
            remoteConnectionInfo.setConnectionId(new ConnectionId(idGenerator.generateId()));
            remoteConnectionInfo.setClientId("NC_"+localBrokerName+"_outbound"+name);
            remoteBroker.oneway(remoteConnectionInfo);

            BrokerInfo brokerInfo=new BrokerInfo();
            brokerInfo.setBrokerName(localBrokerName);
            remoteBroker.oneway(brokerInfo);

            SessionInfo remoteSessionInfo=new SessionInfo(remoteConnectionInfo,1);
            remoteBroker.oneway(remoteSessionInfo);

            producerInfo=new ProducerInfo(remoteSessionInfo,1);
            producerInfo.setResponseRequired(false);
            remoteBroker.oneway(producerInfo);

            // Listen to consumer advisory messages on the remote broker to determine demand.
            demandConsumerInfo=new ConsumerInfo(remoteSessionInfo,1);
            demandConsumerInfo.setDispatchAsync(dispatchAsync);
            demandConsumerInfo.setDestination(new ActiveMQTopic(AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX
                            +destinationFilter));
            demandConsumerInfo.setPrefetchSize(prefetchSize);
            remoteBroker.oneway(demandConsumerInfo);

            startedLatch.countDown();
        }
    }

    public void stop() throws Exception {
        shutDown = true;
        doStop();
    }

    /**
     * stop the bridge
     * @throws Exception 
     */
    protected void doStop() throws Exception {
        log.debug(" stopping "+localBrokerName+ " bridge to " + remoteBrokerName + " is disposed already ? "+disposed);
        if(!disposed){
            try{
                disposed=true;
                localBridgeStarted.set(false);
                remoteBridgeStarted.set(false);
                if(!shutDown){
                   remoteBroker.oneway(new ShutdownInfo());
                    if(localConnectionInfo!=null){
                        localBroker.oneway(localConnectionInfo.createRemoveCommand());
                        remoteBroker.oneway(remoteConnectionInfo.createRemoveCommand());
                    }
                    localBroker.oneway(new ShutdownInfo());
                }
                localBroker.setTransportListener(null);
                remoteBroker.setTransportListener(null);
            }catch(IOException e){
                log.debug("Caught exception stopping",e);
            }finally{
                ServiceStopper ss=new ServiceStopper();
                ss.stop(localBroker);
                ss.stop(remoteBroker);
                ss.throwFirstException();
            }
        }
        log.debug(localBrokerName+ " bridge to " + remoteBrokerName + " stopped");
    }

    protected void serviceRemoteException(Exception error) {
        log.info("Network connection between "+localBroker+" and "+remoteBroker+" shutdown: "+error.getMessage(),error);
        ServiceSupport.dispose(this);
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
                    serviceRemoteBrokerInfo(command);
                }else{
                    switch(command.getDataStructureType()){
                    case WireFormatInfo.DATA_STRUCTURE_TYPE:
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
        if(data.getClass()==ConsumerInfo.class){
            // Create a new local subscription
            ConsumerInfo info=(ConsumerInfo) data;
            BrokerId[] path=info.getBrokerPath();
            if((path!=null&&path.length>= networkTTL)){
                if(log.isTraceEnabled())
                    log.trace("Ignoring Subscription " + info + " restricted to " + networkTTL + " network hops only");
                return;
            }
            if(contains(info.getBrokerPath(),localBrokerPath[0])){
                // Ignore this consumer as it's a consumer we locally sent to the broker.
                if(log.isTraceEnabled())
                    log.trace("Ignoring sub " + info + " already routed through this broker once");
                return;
            }
            if (!isPermissableDestination(info.getDestination())){
                //ignore if not in the permited or in the excluded list
                if(log.isTraceEnabled())
                    log.trace("Ignoring sub " + info + " destination " + info.getDestination() + " is not permiited");
                return;
            }
            // Update the packet to show where it came from.
            info=info.copy();
            addRemoteBrokerToBrokerPath(info);
            DemandSubscription sub=createDemandSubscription(info);
            if (sub != null){
                addSubscription(sub);
                if(log.isTraceEnabled())
                    log.trace("Forwarding sub on "+localBroker+" from "+remoteBrokerName+" :  "+info);
            }else {
                if(log.isTraceEnabled())
                    log.trace("Ignoring sub " + info + " already subscribed to matching destination");
            }
        }
        if(data.getClass()==RemoveInfo.class){
            ConsumerId id=(ConsumerId) ((RemoveInfo) data).getObjectId();
            removeDemandSubscription(id);
        }
    }

    protected void serviceLocalException(Throwable error) {
        log.info("Network connection between "+localBroker+" and "+remoteBroker+" shutdown: "+error.getMessage(),error);
        ServiceSupport.dispose(this);
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
        message.evictMarshlledForm();
        return message;
    }

    protected void serviceLocalCommand(Command command) {
        if(!disposed){
            final boolean trace=log.isTraceEnabled();
            try{
                if(command.isMessageDispatch()){
                    waitStarted();
                    MessageDispatch md=(MessageDispatch) command;
                    DemandSubscription sub=(DemandSubscription) subscriptionMapByLocalId.get(md.getConsumerId());
                    if(sub!=null){
                        Message message= configureMessage(md);
                        if(trace)
                            log.trace("bridging "+localBrokerName+" -> "+remoteBrokerName+": "+message);
                        if(!message.isPersistent()||!sub.getRemoteInfo().isDurable()){
                            remoteBroker.oneway(message);
                        }else{
                            Response response=remoteBroker.request(message);
                            if(response.isException()){
                                ExceptionResponse er=(ExceptionResponse) response;
                                serviceLocalException(er.getException());
                            }
                        }
                        
                      // Ack on every message since we don't know if the broker is blocked due to memory
                      // usage and is waiting for an Ack to un-block him. 
                      localBroker.oneway(new MessageAck(md,MessageAck.STANDARD_ACK_TYPE,1));

                      // Acking a range is more efficient, but also more prone to locking up a server
                      // Perhaps doing something like the following should be policy based.
//                        int dispatched = sub.incrementDispatched();
//                        if(dispatched>(sub.getLocalInfo().getPrefetchSize()*.75)){
//                            localBroker.oneway(new MessageAck(md,MessageAck.STANDARD_ACK_TYPE,dispatched));
//                            sub.setDispatched(0);
//                        }
                    }
                }else if(command.isBrokerInfo()){
                    serviceLocalBrokerInfo(command);
                }else if(command.isShutdownInfo()){
                    log.info(localBrokerName+" Shutting down");
                    shutDown = true;
                    doStop();
                   
                    
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
     * @return prefetch size
     */
    public int getPrefetchSize() {
        return prefetchSize;
    }

    /**
     * @param prefetchSize
     */
    public void setPrefetchSize(int prefetchSize) {
        this.prefetchSize=prefetchSize;
    }

    /**
     * @return true if dispatch async
     */
    public boolean isDispatchAsync() {
        return dispatchAsync;
    }

    /**
     * @param dispatchAsync
     */
    public void setDispatchAsync(boolean dispatchAsync) {
        this.dispatchAsync=dispatchAsync;
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
     * @return Returns the localBrokerName.
     */
    public String getLocalBrokerName() {
        return localBrokerName;
    }

    /**
     * @param localBrokerName
     *            The localBrokerName to set.
     */
    public void setLocalBrokerName(String localBrokerName) {
        this.localBrokerName=localBrokerName;
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
     * @return Returns the name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name The name to set.
     */
    public void setName(String name) {
        this.name=name;
    }

    /**
     * @return Returns the decreaseNetworkConsumerPriority.
     */
    public boolean isDecreaseNetworkConsumerPriority() {
        return decreaseNetworkConsumerPriority;
    }

    /**
     * @param decreaseNetworkConsumerPriority The decreaseNetworkConsumerPriority to set.
     */
    public void setDecreaseNetworkConsumerPriority(boolean decreaseNetworkConsumerPriority) {
        this.decreaseNetworkConsumerPriority=decreaseNetworkConsumerPriority;
    }

    /**
     * @return Returns the networkTTL.
     */
    public int getNetworkTTL() {
        return networkTTL;
    }

    /**
     * @param networkTTL The networkTTL to set.
     */
    public void setNetworkTTL(int networkTTL) {
        this.networkTTL=networkTTL;
    }

    /**
     * @return Returns the shutDown.
     */
    public boolean isShutDown() {
        return shutDown;
    }

    /**
     * @param shutDown The shutDown to set.
     */
    public void setShutDown(boolean shutDown) {
        this.shutDown=shutDown;
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
        
        if( decreaseNetworkConsumerPriority ) {
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
        sub.getLocalInfo().setDispatchAsync(dispatchAsync);
        sub.getLocalInfo().setPrefetchSize(prefetchSize);
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
        
    }

    protected abstract NetworkBridgeFilter createNetworkBridgeFilter(ConsumerInfo info) throws IOException;

    protected abstract void serviceLocalBrokerInfo(Command command) throws InterruptedException;

    protected abstract void addRemoteBrokerToBrokerPath(ConsumerInfo info) throws IOException;

    protected abstract void serviceRemoteBrokerInfo(Command command) throws IOException;

}
