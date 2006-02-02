/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.network;

import java.io.IOException;
import javax.jms.JMSException;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;
/**
 * Forwards messages from the local broker to the remote broker based on demand.
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision$
 */
public class DemandForwardingBridge implements Bridge{
    static final private Log log=LogFactory.getLog(DemandForwardingBridge.class);
    private final Transport localBroker;
    private final Transport remoteBroker;
    private IdGenerator idGenerator=new IdGenerator();
    private LongSequenceGenerator consumerIdGenerator=new LongSequenceGenerator();
    private ConnectionInfo localConnectionInfo;
    private ConnectionInfo remoteConnectionInfo;
    private SessionInfo localSessionInfo;
    private ProducerInfo producerInfo;
    private String localBrokerName;
    private String remoteBrokerName;
    private String localClientId;
    private int prefetchSize=1000;
    private boolean dispatchAsync;
    private String destinationFilter=">";
    private ConsumerInfo demandConsumerInfo;
    private int demandConsumerDispatched;
    private AtomicBoolean localBridgeStarted=new AtomicBoolean(false);
    private AtomicBoolean remoteBridgeStarted=new AtomicBoolean(false);
    private boolean disposed=false;
    BrokerId localBrokerId;
    BrokerId remoteBrokerId;
    private static class DemandSubscription{
        ConsumerInfo remoteInfo;
        ConsumerInfo localInfo;
        int dispatched;

        public DemandSubscription(ConsumerInfo info){
            remoteInfo=info;
            localInfo=info.copy();
        }
    }
    ConcurrentHashMap subscriptionMapByLocalId=new ConcurrentHashMap();
    ConcurrentHashMap subscriptionMapByRemoteId=new ConcurrentHashMap();
    protected final BrokerId localBrokerPath[]=new BrokerId[] { null };
    protected final BrokerId remoteBrokerPath[]=new BrokerId[] { null };
    private CountDownLatch startedLatch = new CountDownLatch(2);

    public DemandForwardingBridge(Transport localBroker,Transport remoteBroker){
        this.localBroker=localBroker;
        this.remoteBroker=remoteBroker;
    }

    public void start() throws Exception{
        log.info("Starting a network connection between "+localBroker+" and "+remoteBroker+" has been established.");
        localBroker.setTransportListener(new TransportListener(){
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
        });
        localBroker.start();
        remoteBroker.start();
        triggerRemoteStartBridge();
    }

    protected void triggerLocalStartBridge() throws IOException{
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

    protected void triggerRemoteStartBridge() throws IOException{
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

    protected void startLocalBridge() throws IOException{
        if(localBridgeStarted.compareAndSet(false,true)){
            localConnectionInfo=new ConnectionInfo();
            localConnectionInfo.setConnectionId(new ConnectionId(idGenerator.generateId()));
            localClientId="NC_"+remoteBrokerName+"_inbound";
            localConnectionInfo.setClientId(localClientId);
            localBroker.oneway(localConnectionInfo);
            localSessionInfo=new SessionInfo(localConnectionInfo,1);
            localBroker.oneway(localSessionInfo);
            log.info("Network connection between "+localBroker+" and "+remoteBroker+"("+remoteBrokerName
                            +") has been established.");
            startedLatch.countDown();
        }
    }

    protected void startRemoteBridge() throws IOException{
        if(remoteBridgeStarted.compareAndSet(false,true)){
            BrokerInfo brokerInfo=new BrokerInfo();
            brokerInfo.setBrokerName(localBrokerName);
            remoteBroker.oneway(brokerInfo);
            remoteConnectionInfo=new ConnectionInfo();
            remoteConnectionInfo.setConnectionId(new ConnectionId(idGenerator.generateId()));
            remoteConnectionInfo.setClientId("NC_"+localBrokerName+"_outbound");
            remoteBroker.oneway(remoteConnectionInfo);
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

    public void stop() throws Exception{
        if(!disposed){
            try{
                disposed=true;
                localBridgeStarted.set(false);
                remoteBridgeStarted.set(false);
                if(localConnectionInfo!=null){
                    localBroker.request(localConnectionInfo.createRemoveCommand());
                    remoteBroker.request(remoteConnectionInfo.createRemoveCommand());
                }
                localBroker.setTransportListener(null);
                remoteBroker.setTransportListener(null);
                remoteBroker.oneway(new ShutdownInfo());
                localBroker.oneway(new ShutdownInfo());
            }catch(IOException e){
                log.debug("Caught exception stopping",e);
            }finally{
                ServiceStopper ss=new ServiceStopper();
                ss.stop(localBroker);
                ss.stop(remoteBroker);
                ss.throwFirstException();
            }
        }
    }

    protected void serviceRemoteException(Exception error){
        log.info("Network connection between "+localBroker+" and "+remoteBroker+" shutdown: "+error.getMessage(),error);
        ServiceSupport.dispose(this);
    }

    protected void serviceRemoteCommand(Command command){
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
                    synchronized(this){
                        BrokerInfo remoteBrokerInfo=(BrokerInfo) command;
                        remoteBrokerId=remoteBrokerInfo.getBrokerId();
                        remoteBrokerPath[0]=remoteBrokerId;
                        remoteBrokerName=remoteBrokerInfo.getBrokerName();
                        if(localBrokerId!=null){
                            if(localBrokerId.equals(remoteBrokerId)){
                                log.info("Disconnecting loop back connection.");
                                waitStarted();
                                ServiceSupport.dispose(this);
                            }else{
                                triggerLocalStartBridge();
                            }
                        }
                    }
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

    private void serviceRemoteConsumerAdvisory(DataStructure data) throws IOException{
        if(data.getClass()==ConsumerInfo.class){
            // Create a new local subscription
            ConsumerInfo info=(ConsumerInfo) data;
            BrokerId[] path=info.getBrokerPath();
            if((path!=null&&path.length>0)||info.isNetworkSubscription()){
                // Ignore: We only support directly connected brokers for now.
                return;
            }
            if(contains(info.getBrokerPath(),localBrokerPath[0])){
                // Ignore this consumer as it's a consumer we locally sent to the broker.
                return;
            }
            if(log.isTraceEnabled())
                log.trace("Forwarding sub on "+localBroker+" from "+remoteBrokerName+" :  "+info);
            // Update the packet to show where it came from.
            info=info.copy();
            info.setBrokerPath(appendToBrokerPath(info.getBrokerPath(),remoteBrokerPath));
            DemandSubscription sub=new DemandSubscription(info);
            sub.localInfo.setConsumerId(new ConsumerId(localSessionInfo.getSessionId(),consumerIdGenerator
                            .getNextSequenceId()));
            sub.localInfo.setDispatchAsync(dispatchAsync);
            sub.localInfo.setPrefetchSize(prefetchSize);
            byte priority=ConsumerInfo.NETWORK_CONSUMER_PRIORITY;
            if(priority>Byte.MIN_VALUE&&info.getBrokerPath()!=null&&info.getBrokerPath().length>1){
                // The longer the path to the consumer, the less it's consumer priority.
                priority-=info.getBrokerPath().length+1;
            }
            sub.localInfo.setPriority(priority);
            subscriptionMapByLocalId.put(sub.localInfo.getConsumerId(),sub);
            subscriptionMapByRemoteId.put(sub.remoteInfo.getConsumerId(),sub);
            sub.localInfo.setBrokerPath(info.getBrokerPath());
            sub.localInfo.setNetworkSubscription(true);
            // This works for now since we use a VM connection to the local broker.
            // may need to change if we ever subscribe to a remote broker.
            sub.localInfo.setAdditionalPredicate(new BooleanExpression(){
                public boolean matches(MessageEvaluationContext message) throws JMSException{
                    try{
                        return matchesForwardingFilter(message.getMessage());
                    }catch(IOException e){
                        throw JMSExceptionSupport.create(e);
                    }
                }

                public Object evaluate(MessageEvaluationContext message) throws JMSException{
                    return matches(message)?Boolean.TRUE:Boolean.FALSE;
                }
            });
            localBroker.oneway(sub.localInfo);
        }
        if(data.getClass()==RemoveInfo.class){
            ConsumerId id=(ConsumerId) ((RemoveInfo) data).getObjectId();
            DemandSubscription sub=(DemandSubscription) subscriptionMapByRemoteId.remove(id);
            if(sub!=null){
                subscriptionMapByLocalId.remove(sub.localInfo.getConsumerId());
                localBroker.oneway(sub.localInfo.createRemoveCommand());
            }
        }
    }

    protected void serviceLocalException(Throwable error){
        log.info("Network connection between "+localBroker+" and "+remoteBroker+" shutdown: "+error.getMessage(),error);
        ServiceSupport.dispose(this);
    }

    boolean matchesForwardingFilter(Message message){
        if(message.isRecievedByDFBridge()||contains(message.getBrokerPath(),remoteBrokerPath[0]))
            return false;
        // Don't propagate advisory messages about network subscriptions
        if(message.isAdvisory()&&message.getDataStructure()!=null
                        &&message.getDataStructure().getDataStructureType()==CommandTypes.CONSUMER_INFO){
            ConsumerInfo info=(ConsumerInfo) message.getDataStructure();
            if(info.isNetworkSubscription()){
                return false;
            }
        }
        return true;
    }

    protected void serviceLocalCommand(Command command){
        if(!disposed){
            final boolean trace=log.isTraceEnabled();
            try{
                if(command.isMessageDispatch()){
                    waitStarted();
                    MessageDispatch md=(MessageDispatch) command;
                    Message message=md.getMessage();
                    DemandSubscription sub=(DemandSubscription) subscriptionMapByLocalId.get(md.getConsumerId());
                    if(sub!=null){
                        message=message.copy();
                        // Update the packet to show where it came from.
                        message.setBrokerPath(appendToBrokerPath(message.getBrokerPath(),localBrokerPath));
                        message.setProducerId(producerInfo.getProducerId());
                        message.setDestination(md.getDestination());
                        if(message.getOriginalTransactionId()==null)
                            message.setOriginalTransactionId(message.getTransactionId());
                        message.setTransactionId(null);
                        message.setRecievedByDFBridge(true);
                        message.evictMarshlledForm();
                        if(trace)
                            log.trace("bridging "+localBrokerName+" -> "+remoteBrokerName+": "+message);
                        if(!message.isPersistent()||!sub.remoteInfo.isDurable()){
                            remoteBroker.oneway(message);
                        }else{
                            Response response=remoteBroker.request(message);
                            if(response.isException()){
                                ExceptionResponse er=(ExceptionResponse) response;
                                serviceLocalException(er.getException());
                            }
                        }
                        sub.dispatched++;
                        if(sub.dispatched>(sub.localInfo.getPrefetchSize()*.75)){
                            localBroker.oneway(new MessageAck(md,MessageAck.STANDARD_ACK_TYPE,sub.dispatched));
                            sub.dispatched=0;
                        }
                    }
                }else if(command.isBrokerInfo()){
                    synchronized(this){
                        localBrokerId=((BrokerInfo) command).getBrokerId();
                        localBrokerPath[0]=localBrokerId;
                        if(remoteBrokerId!=null){
                            if(remoteBrokerId.equals(localBrokerId)){
                                log.info("Disconnecting loop back connection.");
                                waitStarted();
                                ServiceSupport.dispose(this);
                            }
                        }
                    }
                }else if(command.isShutdownInfo()){
                    log.info(localBrokerName+" Shutting down");
                    disposed = true;
                    stop();
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

    public int getPrefetchSize(){
        return prefetchSize;
    }

    public void setPrefetchSize(int prefetchSize){
        this.prefetchSize=prefetchSize;
    }

    public boolean isDispatchAsync(){
        return dispatchAsync;
    }

    public void setDispatchAsync(boolean dispatchAsync){
        this.dispatchAsync=dispatchAsync;
    }

    public String getDestinationFilter(){
        return destinationFilter;
    }

    public void setDestinationFilter(String destinationFilter){
        this.destinationFilter=destinationFilter;
    }

    /**
     * @return Returns the localBrokerName.
     */
    public String getLocalBrokerName(){
        return localBrokerName;
    }

    /**
     * @param localBrokerName
     *            The localBrokerName to set.
     */
    public void setLocalBrokerName(String localBrokerName){
        this.localBrokerName=localBrokerName;
    }

    private boolean contains(BrokerId[] brokerPath,BrokerId brokerId){
        if(brokerPath!=null){
            for(int i=0;i<brokerPath.length;i++){
                if(brokerId.equals(brokerPath[i]))
                    return true;
            }
        }
        return false;
    }

    private BrokerId[] appendToBrokerPath(BrokerId[] brokerPath,BrokerId pathsToAppend[]){
        if(brokerPath==null||brokerPath.length==0)
            return pathsToAppend;
        BrokerId rc[]=new BrokerId[brokerPath.length+pathsToAppend.length];
        System.arraycopy(brokerPath,0,rc,0,brokerPath.length);
        System.arraycopy(pathsToAppend,0,rc,brokerPath.length,pathsToAppend.length);
        return rc;
    }
    
    private void waitStarted() throws InterruptedException {
        startedLatch.await();
    }
}
