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

import org.activemq.command.ActiveMQQueue;
import org.activemq.command.ActiveMQTopic;
import org.activemq.command.BrokerId;
import org.activemq.command.BrokerInfo;
import org.activemq.command.Command;
import org.activemq.command.ConnectionId;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.MessageDispatch;
import org.activemq.command.ProducerInfo;
import org.activemq.command.SessionInfo;
import org.activemq.command.ShutdownInfo;
import org.activemq.transport.Transport;
import org.activemq.transport.TransportListener;
import org.activemq.util.IdGenerator;
import org.activemq.util.ServiceStopper;
import org.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Forwards all messages from the local broker to the remote broker.
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision$
 */
public class ForwardingBridge implements Bridge {
    
    static final private Log log = LogFactory.getLog(ForwardingBridge.class);

    private final Transport localBroker;
    private final Transport remoteBroker;
    
    IdGenerator idGenerator = new IdGenerator();
    ConnectionInfo connectionInfo;
    SessionInfo sessionInfo;
    ProducerInfo producerInfo;
    ConsumerInfo queueConsumerInfo;
    ConsumerInfo topicConsumerInfo;
    
    private String clientId;
    private int prefetchSize=1000;
    private boolean dispatchAsync;
    private String destinationFilter = ">";
    
    private int queueDispatched;
    private int topicDispatched;
    
    BrokerId localBrokerId;
    BrokerId remoteBrokerId;

    public ForwardingBridge(Transport localBroker, Transport remoteBroker) {
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

    /**
     * @throws IOException
     */
    private void startBridge() throws IOException {
        connectionInfo = new ConnectionInfo();
        connectionInfo.setConnectionId(new ConnectionId(idGenerator.generateId()));
        connectionInfo.setClientId(clientId);
        localBroker.oneway(connectionInfo);
        remoteBroker.oneway(connectionInfo);

        sessionInfo=new SessionInfo(connectionInfo, 1);
        localBroker.oneway(sessionInfo);
        remoteBroker.oneway(sessionInfo);
        
        queueConsumerInfo = new ConsumerInfo(sessionInfo, 1);
        queueConsumerInfo.setDispatchAsync(dispatchAsync);
        queueConsumerInfo.setDestination(new ActiveMQQueue(destinationFilter));
        queueConsumerInfo.setPrefetchSize(prefetchSize);
        queueConsumerInfo.setPriority(ConsumerInfo.NETWORK_CONSUMER_PRIORITY);
        localBroker.oneway(queueConsumerInfo);
        
        producerInfo = new ProducerInfo(sessionInfo, 1);
        producerInfo.setResponseRequired(false);
        remoteBroker.oneway(producerInfo);
        
        if( connectionInfo.getClientId()!=null ) {
            topicConsumerInfo = new ConsumerInfo(sessionInfo, 2);
            topicConsumerInfo.setDispatchAsync(dispatchAsync);
            topicConsumerInfo.setSubcriptionName("topic-bridge");
            topicConsumerInfo.setRetroactive(true);
            topicConsumerInfo.setDestination(new ActiveMQTopic(destinationFilter));
            topicConsumerInfo.setPrefetchSize(prefetchSize);
            topicConsumerInfo.setPriority(ConsumerInfo.NETWORK_CONSUMER_PRIORITY);
            localBroker.oneway(topicConsumerInfo);
        }
        log.info("Network connection between " + localBroker + " and " + remoteBroker + " has been established.");
    }
    
    public void stop() throws Exception {
        try {
            if( connectionInfo!=null ) {
                localBroker.request(connectionInfo.createRemoveCommand());
                remoteBroker.request(connectionInfo.createRemoveCommand());
            }
            localBroker.setTransportListener(null);
            remoteBroker.setTransportListener(null);
            localBroker.oneway(new ShutdownInfo());
            remoteBroker.oneway(new ShutdownInfo());
        } finally {
            ServiceStopper ss = new ServiceStopper();
            ss.stop(localBroker);
            ss.stop(remoteBroker);
            ss.throwFirstException();
        }
    }
    
    protected void serviceRemoteException(IOException error) {
        System.out.println("Unexpected remote exception: "+error);
        error.printStackTrace();
    }
    
    protected void serviceRemoteCommand(Command command) {
        try {
            if(command.isBrokerInfo() ) {
                synchronized( this ) {
                    remoteBrokerId = ((BrokerInfo)command).getBrokerId();
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
                System.out.println("Unexpected remote command: "+command);
            }
        } catch (IOException e) {
            serviceLocalException(e);
        }
    }

    protected void serviceLocalException(IOException error) {
        System.out.println("Unexpected local exception: "+error);
        error.printStackTrace();
    }    
    protected void serviceLocalCommand(Command command) {
        try {
            if( command.isMessageDispatch() ) {
                MessageDispatch md = (MessageDispatch) command;
                Message message = md.getMessage();
                message.setProducerId(producerInfo.getProducerId());
                message.setDestination( md.getDestination() );
                
                if( message.getOriginalTransactionId()==null )
                    message.setOriginalTransactionId(message.getTransactionId());
                message.setTransactionId(null);
                message.evictMarshlledForm();

                remoteBroker.oneway( message );
                
                if( md.getConsumerId().equals(queueConsumerInfo.getConsumerId()) ) {
                    queueDispatched++;
                    if( queueDispatched > (queueConsumerInfo.getPrefetchSize()/2) ) {
                        localBroker.oneway(new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, queueDispatched));
                        queueDispatched=0;
                    }
                } else {
                    topicDispatched++;
                    if( topicDispatched > (topicConsumerInfo.getPrefetchSize()/2) ) {
                        localBroker.oneway(new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, topicDispatched));
                        topicDispatched=0;
                    }
                }
            } else if(command.isBrokerInfo() ) {
                synchronized( this ) {
                    localBrokerId = ((BrokerInfo)command).getBrokerId();
                    if( remoteBrokerId !=null) {
                        if( remoteBrokerId.equals(localBrokerId) ) {
                            log.info("Disconnecting loop back connection.");
                            ServiceSupport.dispose(this);
                        } else {
                            triggerStartBridge();                            
                        }
                    }
                }
            } else {
                System.out.println("Unexpected local command: "+command);
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
}
