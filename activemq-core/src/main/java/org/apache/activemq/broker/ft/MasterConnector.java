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
package org.apache.activemq.broker.ft;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.activemq.transport.TransportDisposedIOException;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Connects a Slave Broker to a Master when using <a
 * href="http://activemq.apache.org/masterslave.html">Master Slave</a> for High
 * Availability of messages.
 * 
 * @org.apache.xbean.XBean
 * @version $Revision$
 */
public class MasterConnector implements Service, BrokerServiceAware {

    private static final Log LOG = LogFactory.getLog(MasterConnector.class);
    private BrokerService broker;
    private URI remoteURI;
    private URI localURI;
    private Transport localBroker;
    private Transport remoteBroker;
    private TransportConnector connector;
    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean stoppedBeforeStart = new AtomicBoolean(false);
    private final IdGenerator idGenerator = new IdGenerator();
    private String userName;
    private String password;
    private ConnectionInfo connectionInfo;
    private SessionInfo sessionInfo;
    private ProducerInfo producerInfo;
    private final AtomicBoolean masterActive = new AtomicBoolean();
    private BrokerInfo brokerInfo;
    private boolean firstConnection=true;

    public MasterConnector() {
    }

    public MasterConnector(String remoteUri) throws URISyntaxException {
        remoteURI = new URI(remoteUri);
    }

    public void setBrokerService(BrokerService broker) {
        this.broker = broker;
        if (localURI == null) {
            localURI = broker.getVmConnectorURI();
        }
        if (connector == null) {
            List transportConnectors = broker.getTransportConnectors();
            if (!transportConnectors.isEmpty()) {
                connector = (TransportConnector)transportConnectors.get(0);
            }
        }
    }

    public boolean isSlave() {
        return masterActive.get();
    }

    protected void restartBridge() throws Exception {
        localBroker.oneway(connectionInfo);
        remoteBroker.oneway(connectionInfo);
        localBroker.oneway(sessionInfo);
        remoteBroker.oneway(sessionInfo);
        remoteBroker.oneway(producerInfo);
        remoteBroker.oneway(brokerInfo);
    }
    
    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        if (remoteURI == null) {
            throw new IllegalArgumentException("You must specify a remoteURI");
        }
        localBroker = TransportFactory.connect(localURI);
        remoteBroker = TransportFactory.connect(remoteURI);
        LOG.info("Starting a slave connection between " + localBroker + " and " + remoteBroker + " has been established.");
        localBroker.setTransportListener(new DefaultTransportListener() {

            public void onCommand(Object command) {
            }

            public void onException(IOException error) {
                if (started.get()) {
                    serviceLocalException(error);
                }
            }
        });
        remoteBroker.setTransportListener(new DefaultTransportListener() {

            public void onCommand(Object o) {
                Command command = (Command)o;
                if (started.get()) {
                    serviceRemoteCommand(command);
                }
            }

            public void onException(IOException error) {
                if (started.get()) {
                    serviceRemoteException(error);
                }
            }
            
            public void transportResumed() {
            	try{
            		if(!firstConnection){
	            		localBroker = TransportFactory.connect(localURI);
	            		localBroker.setTransportListener(new DefaultTransportListener() {
	
	                        public void onCommand(Object command) {
	                        }
	
	                        public void onException(IOException error) {
	                            if (started.get()) {
	                                serviceLocalException(error);
	                            }
	                        }
	                    });
	            		localBroker.start();
	            		restartBridge();
	            		LOG.info("Slave connection between " + localBroker + " and " + remoteBroker + " has been reestablished.");
            		}else{
            			firstConnection=false;
            		}
            	}catch(IOException e){
            		LOG.error("MasterConnector failed to send BrokerInfo in transportResumed:", e);
            	}catch(Exception e){
            		LOG.error("MasterConnector failed to restart localBroker in transportResumed:", e);
            	}
            	
            }
        });
        try {
            localBroker.start();
            remoteBroker.start();
            startBridge();
            masterActive.set(true);
        } catch (Exception e) {
            masterActive.set(false);
            if(!stoppedBeforeStart.get()){
            	LOG.error("Failed to start network bridge: " + e, e);
            }else{
            	LOG.info("Slave stopped before connected to the master.");
            }
        }    
    }

    protected void startBridge() throws Exception {
        connectionInfo = new ConnectionInfo();
        connectionInfo.setConnectionId(new ConnectionId(idGenerator.generateId()));
        connectionInfo.setClientId(idGenerator.generateId());
        connectionInfo.setUserName(userName);
        connectionInfo.setPassword(password);
        connectionInfo.setBrokerMasterConnector(true);
        sessionInfo = new SessionInfo(connectionInfo, 1);
        producerInfo = new ProducerInfo(sessionInfo, 1);
        producerInfo.setResponseRequired(false);
        if (connector != null) {
            brokerInfo = connector.getBrokerInfo();
        } else {
            brokerInfo = new BrokerInfo();
        }
        brokerInfo.setBrokerName(broker.getBrokerName());
        brokerInfo.setPeerBrokerInfos(broker.getBroker().getPeerBrokerInfos());
        brokerInfo.setSlaveBroker(true);
        restartBridge();
        LOG.info("Slave connection between " + localBroker + " and " + remoteBroker + " has been established.");
    }

    public void stop() throws Exception {
        if (!started.compareAndSet(true, false)||!masterActive.get()) {
            return;
        }
        masterActive.set(false);
        try {
            // if (connectionInfo!=null){
            // localBroker.request(connectionInfo.createRemoveCommand());
            // }
            // localBroker.setTransportListener(null);
            // remoteBroker.setTransportListener(null);
            remoteBroker.oneway(new ShutdownInfo());
            localBroker.oneway(new ShutdownInfo());
        } catch (IOException e) {
            LOG.debug("Caught exception stopping", e);
        } finally {
            ServiceStopper ss = new ServiceStopper();
            ss.stop(localBroker);
            ss.stop(remoteBroker);
            ss.throwFirstException();
        }
    }
    
    public void stopBeforeConnected()throws Exception{
        masterActive.set(false);
        started.set(false);
        stoppedBeforeStart.set(true);
        ServiceStopper ss = new ServiceStopper();
        ss.stop(localBroker);
        ss.stop(remoteBroker);
    }

    protected void serviceRemoteException(IOException error) {
        LOG.error("Network connection between " + localBroker + " and " + remoteBroker + " shutdown: " + error.getMessage(), error);
        shutDown();
    }

    protected void serviceRemoteCommand(Command command) {
        try {
            if (command.isMessageDispatch()) {
                MessageDispatch md = (MessageDispatch)command;
                command = md.getMessage();
            }
            if (command.getDataStructureType() == CommandTypes.SHUTDOWN_INFO) {
                LOG.warn("The Master has shutdown");
                shutDown();
            } else {
                boolean responseRequired = command.isResponseRequired();
                int commandId = command.getCommandId();
                if (responseRequired) {
                    Response response = (Response)localBroker.request(command);
                    response.setCorrelationId(commandId);
                    remoteBroker.oneway(response);
                } else {
                    localBroker.oneway(command);
                }
            }
        } catch (IOException e) {
            serviceRemoteException(e);
        }
    }

    protected void serviceLocalException(Throwable error) {
    	if (!(error instanceof TransportDisposedIOException) || localBroker.isDisposed()){
	        LOG.info("Network connection between " + localBroker + " and " + remoteBroker + " shutdown: " + error.getMessage(), error);
	        ServiceSupport.dispose(this);
    	}else{
    		LOG.info(error.getMessage());
    	}
    }

    /**
     * @return Returns the localURI.
     */
    public URI getLocalURI() {
        return localURI;
    }

    /**
     * @param localURI The localURI to set.
     */
    public void setLocalURI(URI localURI) {
        this.localURI = localURI;
    }

    /**
     * @return Returns the remoteURI.
     */
    public URI getRemoteURI() {
        return remoteURI;
    }

    /**
     * @param remoteURI The remoteURI to set.
     */
    public void setRemoteURI(URI remoteURI) {
        this.remoteURI = remoteURI;
    }

    /**
     * @return Returns the password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password The password to set.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return Returns the userName.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * @param userName The userName to set.
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    private void shutDown() {
        masterActive.set(false);
        broker.masterFailed();
        ServiceSupport.dispose(this);
    }

	public boolean isStoppedBeforeStart() {
		return stoppedBeforeStart.get();
	}

}
