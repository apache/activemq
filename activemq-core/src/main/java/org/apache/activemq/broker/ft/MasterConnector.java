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
package org.apache.activemq.broker.ft;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connects a Slave Broker to a Master when using <a
 * href="http://activemq.apache.org/masterslave.html">Master Slave</a>
 * for High Availability of messages.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class MasterConnector implements Service,BrokerServiceAware{

    private static final Log log=LogFactory.getLog(MasterConnector.class);
    private BrokerService broker;
    private URI remoteURI;
    private URI localURI;
    private Transport localBroker;
    private Transport remoteBroker;
    private TransportConnector connector;
    private AtomicBoolean started=new AtomicBoolean(false);
    private final IdGenerator idGenerator=new IdGenerator();
    private String userName;
    private String password;
    private ConnectionInfo connectionInfo;
    private SessionInfo sessionInfo;
    private ProducerInfo producerInfo;
    final AtomicBoolean masterActive = new AtomicBoolean();

    public MasterConnector(){
    }

    public MasterConnector(String remoteUri) throws URISyntaxException{
        remoteURI=new URI(remoteUri);
    }

    public void setBrokerService(BrokerService broker){
        this.broker=broker;
        if(localURI==null){
            localURI=broker.getVmConnectorURI();
        }
        if(connector==null){
            List transportConnectors=broker.getTransportConnectors();
            if(!transportConnectors.isEmpty()){
                connector=(TransportConnector)transportConnectors.get(0);
            }
        }
    }

    public boolean isSlave(){
        return masterActive.get();
    }

    public void start() throws Exception{
        if(!started.compareAndSet(false,true)){
            return;
        }
        if(remoteURI==null){
            throw new IllegalArgumentException("You must specify a remoteURI");
        }
        localBroker=TransportFactory.connect(localURI);
        remoteBroker=TransportFactory.connect(remoteURI);
        log.info("Starting a network connection between "+localBroker+" and "+remoteBroker+" has been established.");
        localBroker.setTransportListener(new DefaultTransportListener(){

            public void onCommand(Object command){
            }

            public void onException(IOException error){
                if(started.get()){
                    serviceLocalException(error);
                }
            }
        });
        remoteBroker.setTransportListener(new DefaultTransportListener(){

            public void onCommand(Object o){
                Command command=(Command)o;
                if(started.get()){
                    serviceRemoteCommand(command);
                }
            }

            public void onException(IOException error){
                if(started.get()){
                    serviceRemoteException(error);
                }
            }
        });
        masterActive.set(true);
        Thread thead=new Thread(){

            public void run(){
                try{
                    localBroker.start();
                    remoteBroker.start();
                    startBridge();
                }catch(Exception e){
                    masterActive.set(false);
                    log.error("Failed to start network bridge: "+e,e);
                }
            }
        };
        thead.start();
    }

    protected void startBridge() throws Exception{
        connectionInfo=new ConnectionInfo();
        connectionInfo.setConnectionId(new ConnectionId(idGenerator.generateId()));
        connectionInfo.setClientId(idGenerator.generateId());
        connectionInfo.setUserName(userName);
        connectionInfo.setPassword(password);
        localBroker.oneway(connectionInfo);
        ConnectionInfo remoteInfo=new ConnectionInfo();
        connectionInfo.copy(remoteInfo);
        remoteInfo.setBrokerMasterConnector(true);
        remoteBroker.oneway(connectionInfo);
        sessionInfo=new SessionInfo(connectionInfo,1);
        localBroker.oneway(sessionInfo);
        remoteBroker.oneway(sessionInfo);
        producerInfo=new ProducerInfo(sessionInfo,1);
        producerInfo.setResponseRequired(false);
        remoteBroker.oneway(producerInfo);
        BrokerInfo brokerInfo=null;
        if(connector!=null){
            brokerInfo=connector.getBrokerInfo();
        }else{
            brokerInfo=new BrokerInfo();
        }
        brokerInfo.setBrokerName(broker.getBrokerName());
        brokerInfo.setPeerBrokerInfos(broker.getBroker().getPeerBrokerInfos());
        brokerInfo.setSlaveBroker(true);
        remoteBroker.oneway(brokerInfo);
        log.info("Slave connection between "+localBroker+" and "+remoteBroker+" has been established.");
    }

    public void stop() throws Exception{
        if(!started.compareAndSet(true,false)){
            return;
        }
        masterActive.set(false);
        try{
            // if (connectionInfo!=null){
            // localBroker.request(connectionInfo.createRemoveCommand());
            // }
            // localBroker.setTransportListener(null);
            // remoteBroker.setTransportListener(null);
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

    protected void serviceRemoteException(IOException error){
        log
                .error("Network connection between "+localBroker+" and "+remoteBroker+" shutdown: "+error.getMessage(),
                        error);
        shutDown();
    }

    protected void serviceRemoteCommand(Command command){
        try{
            if(command.isMessageDispatch()){
                MessageDispatch md=(MessageDispatch)command;
                command=md.getMessage();
            }
            if(command.getDataStructureType()==CommandTypes.SHUTDOWN_INFO){
                log.warn("The Master has shutdown");
                shutDown();
            }else{
                boolean responseRequired=command.isResponseRequired();
                int commandId=command.getCommandId();
                localBroker.oneway(command);
                if(responseRequired){
                    Response response=new Response();
                    response.setCorrelationId(commandId);
                    remoteBroker.oneway(response);
                }
            }
        }catch(IOException e){
            serviceRemoteException(e);
        }
    }

    protected void serviceLocalException(Throwable error){
        log.info("Network connection between "+localBroker+" and "+remoteBroker+" shutdown: "+error.getMessage(),error);
        ServiceSupport.dispose(this);
    }

    /**
     * @return Returns the localURI.
     */
    public URI getLocalURI(){
        return localURI;
    }

    /**
     * @param localURI
     *            The localURI to set.
     */
    public void setLocalURI(URI localURI){
        this.localURI=localURI;
    }

    /**
     * @return Returns the remoteURI.
     */
    public URI getRemoteURI(){
        return remoteURI;
    }

    /**
     * @param remoteURI
     *            The remoteURI to set.
     */
    public void setRemoteURI(URI remoteURI){
        this.remoteURI=remoteURI;
    }

    /**
     * @return Returns the password.
     */
    public String getPassword(){
        return password;
    }

    /**
     * @param password
     *            The password to set.
     */
    public void setPassword(String password){
        this.password=password;
    }

    /**
     * @return Returns the userName.
     */
    public String getUserName(){
        return userName;
    }

    /**
     * @param userName
     *            The userName to set.
     */
    public void setUserName(String userName){
        this.userName=userName;
    }

    private void shutDown(){
        masterActive.set(false);
        broker.masterFailed();
        ServiceSupport.dispose(this);
    }
}
