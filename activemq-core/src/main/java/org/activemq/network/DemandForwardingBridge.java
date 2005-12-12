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
package org.activemq.network;

import java.io.IOException;
import org.activemq.advisory.AdvisorySupport;
import org.activemq.command.ActiveMQTopic;
import org.activemq.command.BrokerId;
import org.activemq.command.BrokerInfo;
import org.activemq.command.Command;
import org.activemq.command.CommandTypes;
import org.activemq.command.ConnectionId;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.ConsumerId;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.DataStructure;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.MessageDispatch;
import org.activemq.command.ProducerInfo;
import org.activemq.command.RemoveInfo;
import org.activemq.command.SessionInfo;
import org.activemq.command.ShutdownInfo;
import org.activemq.transport.Transport;
import org.activemq.transport.TransportListener;
import org.activemq.util.IdGenerator;
import org.activemq.util.LongSequenceGenerator;
import org.activemq.util.ServiceStopper;
import org.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * Forwards messages from the local broker to the remote broker based on 
 * demand.
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision$
 */
public class DemandForwardingBridge implements Bridge {

    static final private Log log = LogFactory.getLog(DemandForwardingBridge.class);
    
    private final Transport localBroker;
    private final Transport remoteBroker;
    
    IdGenerator idGenerator = new IdGenerator();
    LongSequenceGenerator consumerIdGenerator = new LongSequenceGenerator();
    
    ConnectionInfo connectionInfo;
    SessionInfo sessionInfo;
    ProducerInfo producerInfo;
    
    private String clientId;
    private int prefetchSize=1000;
    private boolean dispatchAsync;
    private String destinationFilter = ">";
    
    private ConsumerInfo demandConsumerInfo;
    private int demandConsumerDispatched;
    
    BrokerId localBrokerId;
    BrokerId remoteBrokerId;
    
    private static class DemandSubscription {
        ConsumerInfo remoteInfo;
        ConsumerInfo localInfo;
        int dispatched;
        
        public DemandSubscription(ConsumerInfo info) {
            remoteInfo = info;
            localInfo = info.copy();
        }
    }
    
    ConcurrentHashMap subscriptionMapByLocalId = new ConcurrentHashMap();
    ConcurrentHashMap subscriptionMapByRemoteId = new ConcurrentHashMap();
    
    protected final BrokerId localBrokerPath[] = new BrokerId[] {null};
    protected final BrokerId remoteBrokerPath[] = new BrokerId[] {null};
    
    public DemandForwardingBridge(Transport localBroker, Transport remoteBroker) {
        this.localBroker = localBroker;
        this.remoteBroker = remoteBroker;
    }

    public void start() throws Exception {
        log.info("Starting a network connection between " + localBroker + " and " + remoteBroker + " has been established.");

        localBroker.setTransportListener(new TransportListener(){
            public void onCommand(Command command) {
                serviceLocalCommand(command);
            }
            public void onException(IOException error) {
                serviceLocalException(error);
            }
        });
        
        remoteBroker.setTransportListener(new TransportListener(){
            public void onCommand(Command command) {
                serviceRemoteCommand(command);
            }
            public void onException(IOException error) {
                serviceRemoteException(error);
            }
        });
        
        localBroker.start();
        remoteBroker.start();
        
    }

    protected void triggerStartBridge() throws IOException {
        Thread thead = new Thread() {
            public void run() {
                try {
                    startBridge();
                }
                catch (IOException e) {
                    log.error("Failed to start network bridge: " + e, e);
                }
            }
        };
        thead.start();
    }
    
    protected void startBridge() throws IOException {
        BrokerInfo brokerInfo = new BrokerInfo();
        remoteBroker.oneway(brokerInfo);
        connectionInfo = new ConnectionInfo();
        connectionInfo.setConnectionId(new ConnectionId(idGenerator.generateId()));
        connectionInfo.setClientId(clientId);
        localBroker.oneway(connectionInfo);
        remoteBroker.oneway(connectionInfo);

        sessionInfo=new SessionInfo(connectionInfo, 1);
        localBroker.oneway(sessionInfo);
        remoteBroker.oneway(sessionInfo);
        
        producerInfo = new ProducerInfo(sessionInfo, 1);
        producerInfo.setResponseRequired(false);
        remoteBroker.oneway(producerInfo);

        // Listen to consumer advisory messages on the remote broker to determine demand.
        demandConsumerInfo = new ConsumerInfo(sessionInfo, 1);
        demandConsumerInfo.setDispatchAsync(dispatchAsync);
        demandConsumerInfo.setDestination(new ActiveMQTopic(AdvisorySupport.CONSUMER_ADVISORY_TOPIC_PREFIX+destinationFilter));
        demandConsumerInfo.setPrefetchSize(prefetchSize);
        remoteBroker.oneway(demandConsumerInfo);
        
        log.info("Network connection between " + localBroker + " and " + remoteBroker + " has been established.");
    }
    
    public void stop() throws Exception{        
        try {
            if( connectionInfo!=null ) {
                localBroker.request(connectionInfo.createRemoveCommand());
                remoteBroker.request(connectionInfo.createRemoveCommand());
            }
            localBroker.setTransportListener(null);
            remoteBroker.setTransportListener(null);
            remoteBroker.oneway(new ShutdownInfo());
            localBroker.oneway(new ShutdownInfo());
        }catch(IOException e){
          log.debug("Caught exception stopping",e);
        } finally {
            ServiceStopper ss = new ServiceStopper();
            ss.stop(localBroker);
            ss.stop(remoteBroker);
            ss.throwFirstException();
        }
    }
    
    protected void serviceRemoteException(IOException error) {
        log.info("Network connection between " + localBroker + " and " + remoteBroker + " shutdown: "+error.getMessage(), error);
        ServiceSupport.dispose(this);
    }
    
    protected void serviceRemoteCommand(Command command) {
        try {
            if( command.isMessageDispatch() ) {
                MessageDispatch md = (MessageDispatch) command;
                serviceRemoteConsumerAdvisory(md.getMessage().getDataStructure());
                demandConsumerDispatched++;
                if( demandConsumerDispatched > (demandConsumerInfo.getPrefetchSize()*.75) ) {
                    remoteBroker.oneway(new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, demandConsumerDispatched));
                    demandConsumerDispatched=0;
                }
            } else if ( command.isBrokerInfo() ) {
                synchronized( this ) {
                    remoteBrokerId = ((BrokerInfo)command).getBrokerId();
                    remoteBrokerPath[0] = remoteBrokerId;
                    if( localBrokerId !=null) {
                        if( localBrokerId.equals(remoteBrokerId) ) {
                            log.info("Disconnecting loop back connection.");
                            ServiceSupport.dispose(this);
                        } else {
                            triggerStartBridge();                            
                        }
                    }
                }
            } else {
               log.warn("Unexpected remote command: "+command);
            }
        } catch (IOException e) {
            serviceRemoteException(e);
        }
    }

    private void serviceRemoteConsumerAdvisory(DataStructure data) throws IOException {
        if( data.getClass() == ConsumerInfo.class ) {
                       
            // Create a new local subscription
            ConsumerInfo info = (ConsumerInfo) data;
            BrokerId[] path = info.getBrokerPath();
            String pathStr = "{";
            for (int i =0; path != null && i < path.length; i++){
                pathStr += path[i] + " , ";
            }
            pathStr += "}";
            if( contains(info.getBrokerPath(), localBrokerPath[0]) ) {
                // Ignore this consumer as it's a consumer we locally sent to the broker.
                return;
            }
         
            
            // Update the packet to show where it came from.
            info.setBrokerPath( appendToBrokerPath(info.getBrokerPath(), remoteBrokerPath) );
                
            DemandSubscription sub  = new DemandSubscription(info);
            sub.localInfo.setConsumerId( new ConsumerId(sessionInfo.getSessionId(), consumerIdGenerator.getNextSequenceId()) );
            sub.localInfo.setDispatchAsync(dispatchAsync);
            sub.localInfo.setPrefetchSize(prefetchSize);
            byte priority = ConsumerInfo.NETWORK_CONSUMER_PRIORITY;
            if( priority > Byte.MIN_VALUE && info.getBrokerPath()!=null && info.getBrokerPath().length>1 ) {
                // The longer the path to the consumer, the less it's consumer priority.
                priority -= info.getBrokerPath().length+1;
            }
            sub.localInfo.setPriority(priority);
            subscriptionMapByLocalId.put(sub.localInfo.getConsumerId(), sub);
            subscriptionMapByRemoteId.put(sub.remoteInfo.getConsumerId(), sub);
            sub.localInfo.setBrokerPath(info.getBrokerPath());
            sub.localInfo.setNetworkSubscription(true);
            localBroker.oneway(sub.localInfo);            
        }
        if( data.getClass() == RemoveInfo.class ) {
            ConsumerId id = (ConsumerId) ((RemoveInfo)data).getObjectId();
            DemandSubscription sub = (DemandSubscription)subscriptionMapByRemoteId.remove(id);
            if( sub !=null ) {
                subscriptionMapByLocalId.remove(sub.localInfo.getConsumerId());
                localBroker.oneway(sub.localInfo.createRemoveCommand());
            }
        }
    }

    protected void serviceLocalException(IOException error) {
        log.info("Network connection between " + localBroker + " and " + remoteBroker + " shutdown: "+error.getMessage(), error);
        ServiceSupport.dispose(this);
    }
    
    protected void serviceLocalCommand(Command command) {
        try {
            if( command.isMessageDispatch() ) {
                MessageDispatch md = (MessageDispatch) command;
                Message message = md.getMessage();
                //only allow one network hop for this type of bridge
                if (message.isRecievedByDFBridge()){
                    return;
                }
                if (message.isAdvisory() && message.getDataStructure() != null &&  message.getDataStructure().getDataStructureType()==CommandTypes.CONSUMER_INFO){
                    ConsumerInfo info = (ConsumerInfo)message.getDataStructure();
                    if (info.isNetworkSubscription()){
                        //don't want to forward these
                        return;
                    }
                }
                DemandSubscription sub = (DemandSubscription)subscriptionMapByLocalId.get(md.getConsumerId());
                if( sub!=null ) {
                   
                    if( contains(message.getBrokerPath(), remoteBrokerPath[0]) ) {
                        // Don't send the message back to the originator 
                        return;
                    }
                    // Update the packet to show where it came from.
                    message.setBrokerPath( appendToBrokerPath(message.getBrokerPath(), localBrokerPath) );

                    message.setProducerId(producerInfo.getProducerId());
                    message.setDestination( md.getDestination() );
                    
                    if( message.getOriginalTransactionId()==null )
                        message.setOriginalTransactionId(message.getTransactionId());
                    message.setTransactionId(null);
                    message.evictMarshlledForm();

                    remoteBroker.oneway( message );
                    sub.dispatched++;
                    if( sub.dispatched > (sub.localInfo.getPrefetchSize()*.75) ) {
                        localBroker.oneway(new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, demandConsumerDispatched));
                        sub.dispatched=0;
                    }                    
                }
            } else if ( command.isBrokerInfo() ) {
                synchronized( this ) {
                    localBrokerId = ((BrokerInfo)command).getBrokerId();
                    localBrokerPath[0] = localBrokerId;
                    if( remoteBrokerId !=null  ) {
                        if( remoteBrokerId.equals(localBrokerId) ) {
                            log.info("Disconnecting loop back connection.");
                            ServiceSupport.dispose(this);
                        } else {
                            triggerStartBridge();                            
                        }
                    }
                }
            } else {
                log.warn("Unexpected local command: "+command);
            }
        } catch (IOException e) {
            serviceLocalException(e);
        }
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getPrefetchSize() {
        return prefetchSize;
    }

    public void setPrefetchSize(int prefetchSize) {
        this.prefetchSize = prefetchSize;
    }

    public boolean isDispatchAsync() {
        return dispatchAsync;
    }

    public void setDispatchAsync(boolean dispatchAsync) {
        this.dispatchAsync = dispatchAsync;
    }

    public String getDestinationFilter() {
        return destinationFilter;
    }
    public void setDestinationFilter(String destinationFilter) {
        this.destinationFilter = destinationFilter;
    }
    
    private boolean contains(BrokerId[] brokerPath, BrokerId brokerId) {
        if( brokerPath!=null ) {
            for (int i = 0; i < brokerPath.length; i++) {
                if( brokerId.equals(brokerPath[i]) )
                    return true;
            }
        }
        return false;
    }
    private BrokerId[] appendToBrokerPath(BrokerId[] brokerPath, BrokerId pathsToAppend[]) {
        if( brokerPath == null || brokerPath.length==0 )
            return pathsToAppend;
        
        BrokerId rc[] = new BrokerId[brokerPath.length+pathsToAppend.length];
        System.arraycopy(brokerPath,0,rc,0,brokerPath.length);
        System.arraycopy(pathsToAppend,0,rc,brokerPath.length,pathsToAppend.length);
        return rc;
    }

}
