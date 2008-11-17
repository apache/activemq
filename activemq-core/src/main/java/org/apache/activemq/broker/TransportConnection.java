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
package org.apache.activemq.broker;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.activemq.broker.ft.MasterBroker;
import org.apache.activemq.broker.region.ConnectionStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ControlCommand;
import org.apache.activemq.command.DataArrayResponse;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.FlushCommand;
import org.apache.activemq.command.IntegerResponse;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.network.DemandForwardingBridge;
import org.apache.activemq.network.NetworkBridgeConfiguration;
import org.apache.activemq.network.NetworkBridgeFactory;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.state.ConsumerState;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.state.SessionState;
import org.apache.activemq.state.TransactionState;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transaction.Transaction;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.util.ServiceSupport;
import org.apache.activemq.util.URISupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.8 $
 */
public class TransportConnection implements Connection, Task, CommandVisitor {

    private static final Log LOG = LogFactory.getLog(TransportConnection.class);
    private static final Log TRANSPORTLOG = LogFactory.getLog(TransportConnection.class.getName()
                                                              + ".Transport");
    private static final Log SERVICELOG = LogFactory.getLog(TransportConnection.class.getName() + ".Service");

    // Keeps track of the broker and connector that created this connection.
    protected final Broker broker;
    protected final TransportConnector connector;
    // Keeps track of the state of the connections.
    // protected final ConcurrentHashMap localConnectionStates=new
    // ConcurrentHashMap();
    protected final Map<ConnectionId, ConnectionState> brokerConnectionStates;
    // The broker and wireformat info that was exchanged.
    protected BrokerInfo brokerInfo;
    protected final List<Command> dispatchQueue = new LinkedList<Command>();
    protected TaskRunner taskRunner;
    protected final AtomicReference<IOException> transportException = new AtomicReference<IOException>();
    protected AtomicBoolean dispatchStopped = new AtomicBoolean(false);

    private MasterBroker masterBroker;
    private final Transport transport;
    private MessageAuthorizationPolicy messageAuthorizationPolicy;
    private WireFormatInfo wireFormatInfo;
    // Used to do async dispatch.. this should perhaps be pushed down into the
    // transport layer..
    private boolean inServiceException;
    private ConnectionStatistics statistics = new ConnectionStatistics();
    private boolean manageable;
    private boolean slow;
    private boolean markedCandidate;
    private boolean blockedCandidate;
    private boolean blocked;
    private boolean connected;
    private boolean active;
    private boolean starting;
    private boolean pendingStop;
    private long timeStamp;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private CountDownLatch stopped = new CountDownLatch(1);
    private final AtomicBoolean asyncException = new AtomicBoolean(false);
    private final Map<ProducerId, ProducerBrokerExchange> producerExchanges = new HashMap<ProducerId, ProducerBrokerExchange>();
    private final Map<ConsumerId, ConsumerBrokerExchange> consumerExchanges = new HashMap<ConsumerId, ConsumerBrokerExchange>();
    private CountDownLatch dispatchStoppedLatch = new CountDownLatch(1);
    private ConnectionContext context;
    private boolean networkConnection;
    private boolean faultTolerantConnection;
    private AtomicInteger protocolVersion = new AtomicInteger(CommandTypes.PROTOCOL_VERSION);
    private DemandForwardingBridge duplexBridge;
    private final TaskRunnerFactory taskRunnerFactory;
    private TransportConnectionStateRegister connectionStateRegister = new SingleTransportConnectionStateRegister();
    
    private final ReentrantReadWriteLock serviceLock = new ReentrantReadWriteLock();
    
    
    /**
     * @param connector
     * @param transport
     * @param broker
     * @param taskRunnerFactory - can be null if you want direct dispatch to the
     *                transport else commands are sent async.
     */
    public TransportConnection(TransportConnector connector, final Transport transport, Broker broker,
                               TaskRunnerFactory taskRunnerFactory) {
        this.connector = connector;
        this.broker = broker;
        this.messageAuthorizationPolicy = connector.getMessageAuthorizationPolicy();
        RegionBroker rb = (RegionBroker)broker.getAdaptor(RegionBroker.class);
        brokerConnectionStates = rb.getConnectionStates();
        if (connector != null) {
            this.statistics.setParent(connector.getStatistics());
        }
        this.taskRunnerFactory = taskRunnerFactory;
        this.transport = transport;
        this.transport.setTransportListener(new DefaultTransportListener() {

            public void onCommand(Object o) {
                serviceLock.readLock().lock();
                try {
                    Command command = (Command)o;
                    Response response = service(command);
                    if (response != null) {
                        dispatchSync(response);
                    }
                } finally {
                    serviceLock.readLock().unlock();
                }
            }

            public void onException(IOException exception) {
                serviceLock.readLock().lock();
                try {
                    serviceTransportException(exception);
                } finally {
                    serviceLock.readLock().unlock();
                }
            }
        });
        connected = true;
    }

    /**
     * Returns the number of messages to be dispatched to this connection
     * 
     * @return size of dispatch queue
     */
    public int getDispatchQueueSize() {
        synchronized(dispatchQueue) {
            return dispatchQueue.size();
        }
    }

    public void serviceTransportException(IOException e) {
    	BrokerService bService=connector.getBrokerService();
    	if(bService.isShutdownOnSlaveFailure()){
	    	if(brokerInfo!=null){
		    	if(brokerInfo.isSlaveBroker()){
		        	LOG.error("Slave has exception: " + e.getMessage()+" shutting down master now.", e);
		            try {
		                broker.stop();
		                bService.stop();
		        	}catch(Exception ex){
		                LOG.warn("Failed to stop the master",ex);
		            }
		        }
	    	}
    	}
        if (!stopping.get()) {
            transportException.set(e);
            if (TRANSPORTLOG.isDebugEnabled()) {
                TRANSPORTLOG.debug("Transport failed: " + e, e);
            }
            stopAsync();
        }
    }

    /**
     * Calls the serviceException method in an async thread. Since handling a
     * service exception closes a socket, we should not tie up broker threads
     * since client sockets may hang or cause deadlocks.
     * 
     * @param e
     */
    public void serviceExceptionAsync(final IOException e) {
        if (asyncException.compareAndSet(false, true)) {
            new Thread("Async Exception Handler") {
                public void run() {
                    serviceException(e);
                }
            }.start();
        }
    }

    /**
     * Closes a clients connection due to a detected error. Errors are ignored
     * if: the client is closing or broker is closing. Otherwise, the connection
     * error transmitted to the client before stopping it's transport.
     */
    public void serviceException(Throwable e) {

        // are we a transport exception such as not being able to dispatch
        // synchronously to a transport
        if (e instanceof IOException) {
            serviceTransportException((IOException)e);
        } else if (e.getClass() == BrokerStoppedException.class) {
            // Handle the case where the broker is stopped
            // But the client is still connected.

            if (!stopping.get()) {
                if (SERVICELOG.isDebugEnabled()) {
                    SERVICELOG
                        .debug("Broker has been stopped.  Notifying client and closing his connection.");
                }

                ConnectionError ce = new ConnectionError();
                ce.setException(e);
                dispatchSync(ce);
                // Wait a little bit to try to get the output buffer to flush
                // the exption notification to the client.
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                // Worst case is we just kill the connection before the
                // notification gets to him.
                stopAsync();
            }
        } else if (!stopping.get() && !inServiceException) {
            inServiceException = true;
            try {
                SERVICELOG.error("Async error occurred: " + e, e);
                ConnectionError ce = new ConnectionError();
                ce.setException(e);
                dispatchAsync(ce);
            } finally {
                inServiceException = false;
            }
        }
    }

    public Response service(Command command) {
        Response response = null;
        boolean responseRequired = command.isResponseRequired();
        int commandId = command.getCommandId();
        try {
            response = command.visit(this);
        } catch (Throwable e) {
            if (SERVICELOG.isDebugEnabled() && e.getClass() != BrokerStoppedException.class) {
                SERVICELOG.debug("Error occured while processing "
                        + (responseRequired ? "sync": "async")
                        + " command: " + command + ", exception: " + e, e);
            }
            if (responseRequired) {
                response = new ExceptionResponse(e);
            } else {
                serviceException(e);
            }
        }
        if (responseRequired) {
            if (response == null) {
                response = new Response();
            }
            response.setCorrelationId(commandId);
        }
        // The context may have been flagged so that the response is not
        // sent.
        if (context != null) {
            if (context.isDontSendReponse()) {
                context.setDontSendReponse(false);
                response = null;
            }
            context = null;
        }
        return response;
    }

    public Response processKeepAlive(KeepAliveInfo info) throws Exception {
        return null;
    }

    public Response processRemoveSubscription(RemoveSubscriptionInfo info) throws Exception {
        broker.removeSubscription(lookupConnectionState(info.getConnectionId()).getContext(), info);
        return null;
    }

    public Response processWireFormat(WireFormatInfo info) throws Exception {
        wireFormatInfo = info;
        protocolVersion.set(info.getVersion());
        return null;
    }

    public Response processShutdown(ShutdownInfo info) throws Exception {
        stopAsync();
        return null;
    }

    public Response processFlush(FlushCommand command) throws Exception {
        return null;
    }

    public Response processBeginTransaction(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = null;
        if (cs != null) {
            context = cs.getContext();
        }
        if (cs == null) {
            throw new NullPointerException("Context is null");
        }
        // Avoid replaying dup commands
        if (cs.getTransactionState(info.getTransactionId()) == null) {
            cs.addTransactionState(info.getTransactionId());
            broker.beginTransaction(context, info.getTransactionId());
        }
        return null;
    }

    public Response processEndTransaction(TransactionInfo info) throws Exception {
        // No need to do anything. This packet is just sent by the client
        // make sure he is synced with the server as commit command could
        // come from a different connection.
        return null;
    }

    public Response processPrepareTransaction(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = null;
        if (cs != null) {
            context = cs.getContext();
        }
        if (cs == null) {
            throw new NullPointerException("Context is null");
        }
        TransactionState transactionState = cs.getTransactionState(info.getTransactionId());
        if (transactionState == null) {
            throw new IllegalStateException("Cannot prepare a transaction that had not been started: "
                                            + info.getTransactionId());
        }
        // Avoid dups.
        if (!transactionState.isPrepared()) {
            transactionState.setPrepared(true);
            int result = broker.prepareTransaction(context, info.getTransactionId());
            transactionState.setPreparedResult(result);
            IntegerResponse response = new IntegerResponse(result);
            return response;
        } else {
            IntegerResponse response = new IntegerResponse(transactionState.getPreparedResult());
            return response;
        }
    }

    public Response processCommitTransactionOnePhase(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = cs.getContext();
        cs.removeTransactionState(info.getTransactionId());
        broker.commitTransaction(context, info.getTransactionId(), true);
        return null;
    }

    public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = cs.getContext();
        cs.removeTransactionState(info.getTransactionId());
        broker.commitTransaction(context, info.getTransactionId(), false);
        return null;
    }

    public Response processRollbackTransaction(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = cs.getContext();
        cs.removeTransactionState(info.getTransactionId());
        broker.rollbackTransaction(context, info.getTransactionId());
        return null;
    }

    public Response processForgetTransaction(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = cs.getContext();
        broker.forgetTransaction(context, info.getTransactionId());
        return null;
    }

    public Response processRecoverTransactions(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = cs.getContext();
        TransactionId[] preparedTransactions = broker.getPreparedTransactions(context);
        return new DataArrayResponse(preparedTransactions);
    }

    public Response processMessage(Message messageSend) throws Exception {
        ProducerId producerId = messageSend.getProducerId();
        ProducerBrokerExchange producerExchange = getProducerBrokerExchange(producerId);
        broker.send(producerExchange, messageSend);
        return null;
    }

    public Response processMessageAck(MessageAck ack) throws Exception {
        ConsumerBrokerExchange consumerExchange = getConsumerBrokerExchange(ack.getConsumerId());
        broker.acknowledge(consumerExchange, ack);
        return null;
    }

    public Response processMessagePull(MessagePull pull) throws Exception {
        return broker.messagePull(lookupConnectionState(pull.getConsumerId()).getContext(), pull);
    }

    public Response processMessageDispatchNotification(MessageDispatchNotification notification)
        throws Exception {
        broker.processDispatchNotification(notification);
        return null;
    }

    public Response processAddDestination(DestinationInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        broker.addDestinationInfo(cs.getContext(), info);
        if (info.getDestination().isTemporary()) {
            cs.addTempDestination(info);
        }
        return null;
    }

    public Response processRemoveDestination(DestinationInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        broker.removeDestinationInfo(cs.getContext(), info);
        if (info.getDestination().isTemporary()) {
            cs.removeTempDestination(info.getDestination());
        }
        return null;
    }

    public Response processAddProducer(ProducerInfo info) throws Exception {
        SessionId sessionId = info.getProducerId().getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if (ss == null) {
            throw new IllegalStateException(
                                            "Cannot add a producer to a session that had not been registered: "
                                                + sessionId);
        }
        // Avoid replaying dup commands
        if (!ss.getProducerIds().contains(info.getProducerId())) {
            broker.addProducer(cs.getContext(), info);
            try {
                ss.addProducer(info);
            } catch (IllegalStateException e) {
                broker.removeProducer(cs.getContext(), info);
            }
        }
        return null;
    }

    public Response processRemoveProducer(ProducerId id) throws Exception {
        SessionId sessionId = id.getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if (ss == null) {
            throw new IllegalStateException(
                                            "Cannot remove a producer from a session that had not been registered: "
                                                + sessionId);
        }
        ProducerState ps = ss.removeProducer(id);
        if (ps == null) {
            throw new IllegalStateException("Cannot remove a producer that had not been registered: " + id);
        }
        removeProducerBrokerExchange(id);
        broker.removeProducer(cs.getContext(), ps.getInfo());
        return null;
    }

    public Response processAddConsumer(ConsumerInfo info) throws Exception {
        SessionId sessionId = info.getConsumerId().getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if (ss == null) {
            throw new IllegalStateException(
                                            broker.getBrokerName() + " Cannot add a consumer to a session that had not been registered: "
                                                + sessionId);
        }
        // Avoid replaying dup commands
        if (!ss.getConsumerIds().contains(info.getConsumerId())) {
            broker.addConsumer(cs.getContext(), info);
            try {
                ss.addConsumer(info);
            } catch (IllegalStateException e) {
                broker.removeConsumer(cs.getContext(), info);
            }
        }
        return null;
    }

    public Response processRemoveConsumer(ConsumerId id) throws Exception {
        SessionId sessionId = id.getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if (ss == null) {
            throw new IllegalStateException(
                                            "Cannot remove a consumer from a session that had not been registered: "
                                                + sessionId);
        }
        ConsumerState consumerState = ss.removeConsumer(id);
        if (consumerState == null) {
            throw new IllegalStateException("Cannot remove a consumer that had not been registered: " + id);
        }
        broker.removeConsumer(cs.getContext(), consumerState.getInfo());
        removeConsumerBrokerExchange(id);
        return null;
    }

    public Response processAddSession(SessionInfo info) throws Exception {
        ConnectionId connectionId = info.getSessionId().getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        // Avoid replaying dup commands
        if (!cs.getSessionIds().contains(info.getSessionId())) {
            broker.addSession(cs.getContext(), info);
            try {
                cs.addSession(info);
            } catch (IllegalStateException e) {
            	e.printStackTrace();
                broker.removeSession(cs.getContext(), info);
            }
        }
        return null;
    }

    public Response processRemoveSession(SessionId id) throws Exception {
        ConnectionId connectionId = id.getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        SessionState session = cs.getSessionState(id);
        if (session == null) {
            throw new IllegalStateException("Cannot remove session that had not been registered: " + id);
        }
        // Don't let new consumers or producers get added while we are closing
        // this down.
        session.shutdown();
        // Cascade the connection stop to the consumers and producers.
        for (Iterator iter = session.getConsumerIds().iterator(); iter.hasNext();) {
            ConsumerId consumerId = (ConsumerId)iter.next();
            try {
                processRemoveConsumer(consumerId);
            } catch (Throwable e) {
                LOG.warn("Failed to remove consumer: " + consumerId + ". Reason: " + e, e);
            }
        }
        for (Iterator iter = session.getProducerIds().iterator(); iter.hasNext();) {
            ProducerId producerId = (ProducerId)iter.next();
            try {
                processRemoveProducer(producerId);
            } catch (Throwable e) {
                LOG.warn("Failed to remove producer: " + producerId + ". Reason: " + e, e);
            }
        }
        cs.removeSession(id);
        broker.removeSession(cs.getContext(), session.getInfo());
        return null;
    }

    public Response processAddConnection(ConnectionInfo info) throws Exception {
    	//if the broker service has slave attached, wait for the slave to be attached to allow client connection. slave connection is fine
    	if(!info.isBrokerMasterConnector()&&connector.getBrokerService().isWaitForSlave()&&connector.getBrokerService().getSlaveStartSignal().getCount()==1){
    			ServiceSupport.dispose(transport);
    			return new ExceptionResponse(new Exception("Master's slave not attached yet."));
    	}
        // Older clients should have been defaulting this field to true.. but they were not. 
        if( wireFormatInfo!=null && wireFormatInfo.getVersion() <= 2 ) {
            info.setClientMaster(true);
        }
        
        TransportConnectionState state;

        // Make sure 2 concurrent connections by the same ID only generate 1
        // TransportConnectionState object.
        synchronized (brokerConnectionStates) {
            state = (TransportConnectionState)brokerConnectionStates.get(info.getConnectionId());
            if (state == null) {
                state = new TransportConnectionState(info, this);
                brokerConnectionStates.put(info.getConnectionId(), state);
            }
            state.incrementReference();
        }

        // If there are 2 concurrent connections for the same connection id,
        // then last one in wins, we need to sync here
        // to figure out the winner.
        synchronized (state.getConnectionMutex()) {
            if (state.getConnection() != this) {
                LOG.debug("Killing previous stale connection: " + state.getConnection().getRemoteAddress());
                state.getConnection().stop();
                LOG.debug("Connection " + getRemoteAddress() + " taking over previous connection: "
                          + state.getConnection().getRemoteAddress());
                state.setConnection(this);
                state.reset(info);
            }
        }

        registerConnectionState(info.getConnectionId(), state);

        LOG.debug("Setting up new connection: " + getRemoteAddress());
        // Setup the context.
        String clientId = info.getClientId();
        context = new ConnectionContext();
        context.setBroker(broker);
        context.setClientId(clientId);
        context.setClientMaster(info.isClientMaster());
        context.setConnection(this);
        context.setConnectionId(info.getConnectionId());
        context.setConnector(connector);
        context.setMessageAuthorizationPolicy(getMessageAuthorizationPolicy());
        context.setNetworkConnection(networkConnection);
        context.setFaultTolerant(faultTolerantConnection);
        context.setTransactions(new ConcurrentHashMap<TransactionId, Transaction>());
        context.setUserName(info.getUserName());
        context.setWireFormatInfo(wireFormatInfo);
        this.manageable = info.isManageable();
        state.setContext(context);
        state.setConnection(this);

        try {
        broker.addConnection(context, info);
        }catch(Exception e){
        	brokerConnectionStates.remove(info);
        	LOG.warn("Failed to add Connection",e);
        	throw e;
        }
        if (info.isManageable() && broker.isFaultTolerantConfiguration()) {
            // send ConnectionCommand
            ConnectionControl command = new ConnectionControl();
            command.setFaultTolerant(broker.isFaultTolerantConfiguration());
            dispatchAsync(command);
        }
        return null;
    }

    public synchronized Response processRemoveConnection(ConnectionId id) throws InterruptedException {
        TransportConnectionState cs = lookupConnectionState(id);
        if (cs != null) {
            // Don't allow things to be added to the connection state while we are
            // shutting down.
            cs.shutdown();
            
            // Cascade the connection stop to the sessions.
            for (Iterator iter = cs.getSessionIds().iterator(); iter.hasNext();) {
                SessionId sessionId = (SessionId)iter.next();
                try {
                    processRemoveSession(sessionId);
                } catch (Throwable e) {
                    SERVICELOG.warn("Failed to remove session " + sessionId, e);
                }
            }
            // Cascade the connection stop to temp destinations.
            for (Iterator iter = cs.getTempDesinations().iterator(); iter.hasNext();) {
                DestinationInfo di = (DestinationInfo)iter.next();
                try {
                    broker.removeDestination(cs.getContext(), di.getDestination(), 0);
                } catch (Throwable e) {
                    SERVICELOG.warn("Failed to remove tmp destination " + di.getDestination(), e);
                }
                iter.remove();
            }
            try {
                broker.removeConnection(cs.getContext(), cs.getInfo(), null);
            } catch (Throwable e) {
                SERVICELOG.warn("Failed to remove connection " + cs.getInfo(), e);
            }
    
            TransportConnectionState state = unregisterConnectionState(id);
            if (state != null) {
                synchronized (brokerConnectionStates) {
                    // If we are the last reference, we should remove the state
                    // from the broker.
                    if (state.decrementReference() == 0) {
                        brokerConnectionStates.remove(id);
                    }
                }
            }
        }
        return null;
    }

    public Response processProducerAck(ProducerAck ack) throws Exception {
        // A broker should not get ProducerAck messages.
        return null;
    }

    public Connector getConnector() {
        return connector;
    }

    public void dispatchSync(Command message) {
        //getStatistics().getEnqueues().increment();
        try {
            processDispatch(message);
        } catch (IOException e) {
            serviceExceptionAsync(e);
        }
    }

    public void dispatchAsync(Command message) {
        if (!stopping.get()) {
            //getStatistics().getEnqueues().increment();
            if (taskRunner == null) {
                dispatchSync(message);
            } else {
                synchronized(dispatchQueue) {
                    dispatchQueue.add(message);
                }
                try {
                    taskRunner.wakeup();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } else {
            if (message.isMessageDispatch()) {
                MessageDispatch md = (MessageDispatch)message;
                Runnable sub = md.getTransmitCallback();
                broker.postProcessDispatch(md);
                if (sub != null) {
                    sub.run();
                }
            }
        }
    }

    protected void processDispatch(Command command) throws IOException {
        final MessageDispatch messageDispatch = (MessageDispatch)(command.isMessageDispatch()
            ? command : null);
        try {
            if (!stopping.get()) {
                if (messageDispatch != null) {
                    broker.preProcessDispatch(messageDispatch);
                }
                dispatch(command);
            }
        } finally {
            if (messageDispatch != null) {
                Runnable sub = messageDispatch.getTransmitCallback();
                broker.postProcessDispatch(messageDispatch);
                if (sub != null) {
                    sub.run();
                }
            }
            //getStatistics().getDequeues().increment();
        }
    }

    public boolean iterate() {
        try {
            if (stopping.get()) {
                if (dispatchStopped.compareAndSet(false, true)) {
                    if (transportException.get() == null) {
                        try {
                            dispatch(new ShutdownInfo());
                        } catch (Throwable ignore) {
                        }
                    }
                    dispatchStoppedLatch.countDown();
                }
                return false;
            }

            if (!dispatchStopped.get()) {
                Command command = null;
                synchronized(dispatchQueue) {
                    if (dispatchQueue.isEmpty()) {
                        return false;
                    }
                    command = dispatchQueue.remove(0);
                }
                processDispatch(command);
                return true;
            }
            return false;

        } catch (IOException e) {
            if (dispatchStopped.compareAndSet(false, true)) {
                dispatchStoppedLatch.countDown();
            }
            serviceExceptionAsync(e);
            return false;
        }
    }

    /**
     * Returns the statistics for this connection
     */
    public ConnectionStatistics getStatistics() {
        return statistics;
    }

    public MessageAuthorizationPolicy getMessageAuthorizationPolicy() {
        return messageAuthorizationPolicy;
    }

    public void setMessageAuthorizationPolicy(MessageAuthorizationPolicy messageAuthorizationPolicy) {
        this.messageAuthorizationPolicy = messageAuthorizationPolicy;
    }

    public boolean isManageable() {
        return manageable;
    }

    public void start() throws Exception {
        starting = true;
        try {
               synchronized(this) {
                   if (taskRunnerFactory != null) {
                       taskRunner = taskRunnerFactory.createTaskRunner(this, "ActiveMQ Connection Dispatcher: "
                                                                             + getRemoteAddress());
                   } else {
                       taskRunner = null;
                   }
                   transport.start();

                   active = true;
                   dispatchAsync(connector.getBrokerInfo());
                   connector.onStarted(this);
               }
        } catch (Exception e) {
            // Force clean up on an error starting up.
            stop();
            throw e;
        } finally {
            // stop() can be called from within the above block,
            // but we want to be sure start() completes before
            // stop() runs, so queue the stop until right now:
            starting = false;
            if (pendingStop) {
                LOG.debug("Calling the delayed stop()");
                stop();
            }
        }
    }
    public void stop() throws Exception {
        synchronized (this) {
            pendingStop = true;
            if (starting) {
                LOG.debug("stop() called in the middle of start(). Delaying...");
                return;
            }
        }
        stopAsync();
        while( !stopped.await(5, TimeUnit.SECONDS) ) {
            LOG.info("The connection to '" + transport.getRemoteAddress()+ "' is taking a long time to shutdown.");
        }
    }
    
    public void stopAsync() {
        // If we're in the middle of starting
        // then go no further... for now.
        if (stopping.compareAndSet(false, true)) {
            
            // Let all the connection contexts know we are shutting down
            // so that in progress operations can notice and unblock.
            List<TransportConnectionState> connectionStates = listConnectionStates();
            for (TransportConnectionState cs : connectionStates) {
                cs.getContext().getStopping().set(true);
            }

            new Thread("ActiveMQ Transport Stopper: "+ transport.getRemoteAddress()) {
                @Override
                public void run() {
                    serviceLock.writeLock().lock();
                    try {
                        doStop();
                    } catch (Throwable e) {
                        LOG.debug("Error occured while shutting down a connection to '" + transport.getRemoteAddress()+ "': ", e);
                    } finally {
                        stopped.countDown();
                        serviceLock.writeLock().unlock();
                    }
                }
            }.start();
        }
    }

    @Override
    public String toString() {
        return  "Transport Connection to: "+transport.getRemoteAddress();
    }
    
    protected void doStop() throws Exception, InterruptedException {
        LOG.debug("Stopping connection: " + transport.getRemoteAddress());
        connector.onStopped(this);
        try {
            synchronized (this) {
                if (masterBroker != null) {
                    masterBroker.stop();
                }
                if (duplexBridge != null) {
                    duplexBridge.stop();
                }
            }

        } catch (Exception ignore) {
            LOG.trace("Exception caught stopping", ignore);
        }

        try {
            transport.stop();
            LOG.debug("Stopped transport: " + transport.getRemoteAddress());
        } catch (Exception e) {
            LOG.debug("Could not stop transport: " + e, e);
        }

        if (taskRunner != null) {
            taskRunner.shutdown(1);
        }
        
        active = false;

        // Run the MessageDispatch callbacks so that message references get
        // cleaned up.
        synchronized(dispatchQueue) {
            for (Iterator<Command> iter = dispatchQueue.iterator(); iter.hasNext();) {
                Command command = iter.next();
                if (command.isMessageDispatch()) {
                    MessageDispatch md = (MessageDispatch)command;
                    Runnable sub = md.getTransmitCallback();
                    broker.postProcessDispatch(md);
                    if (sub != null) {
                        sub.run();
                    }
                }
            }
            dispatchQueue.clear();
        }
        //
        // Remove all logical connection associated with this connection
        // from the broker.

        if (!broker.isStopped()) {
            
            List<TransportConnectionState> connectionStates = listConnectionStates();
            connectionStates = listConnectionStates();
            for (TransportConnectionState cs : connectionStates) {
                cs.getContext().getStopping().set(true);
                try {
                    LOG.debug("Cleaning up connection resources: " + getRemoteAddress());
                    processRemoveConnection(cs.getInfo().getConnectionId());
                } catch (Throwable ignore) {
                    ignore.printStackTrace();
                }
            }

            if (brokerInfo != null) {
                broker.removeBroker(this, brokerInfo);
            }
        }
        LOG.debug("Connection Stopped: " + getRemoteAddress());
    }

    /**
     * @return Returns the blockedCandidate.
     */
    public boolean isBlockedCandidate() {
        return blockedCandidate;
    }

    /**
     * @param blockedCandidate The blockedCandidate to set.
     */
    public void setBlockedCandidate(boolean blockedCandidate) {
        this.blockedCandidate = blockedCandidate;
    }

    /**
     * @return Returns the markedCandidate.
     */
    public boolean isMarkedCandidate() {
        return markedCandidate;
    }

    /**
     * @param markedCandidate The markedCandidate to set.
     */
    public void setMarkedCandidate(boolean markedCandidate) {
        this.markedCandidate = markedCandidate;
        if (!markedCandidate) {
            timeStamp = 0;
            blockedCandidate = false;
        }
    }

    /**
     * @param slow The slow to set.
     */
    public void setSlow(boolean slow) {
        this.slow = slow;
    }

    /**
     * @return true if the Connection is slow
     */
    public boolean isSlow() {
        return slow;
    }

    /**
     * @return true if the Connection is potentially blocked
     */
    public boolean isMarkedBlockedCandidate() {
        return markedCandidate;
    }

    /**
     * Mark the Connection, so we can deem if it's collectable on the next sweep
     */
    public void doMark() {
        if (timeStamp == 0) {
            timeStamp = System.currentTimeMillis();
        }
    }

    /**
     * @return if after being marked, the Connection is still writing
     */
    public boolean isBlocked() {
        return blocked;
    }

    /**
     * @return true if the Connection is connected
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * @param blocked The blocked to set.
     */
    public void setBlocked(boolean blocked) {
        this.blocked = blocked;
    }

    /**
     * @param connected The connected to set.
     */
    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    /**
     * @return true if the Connection is active
     */
    public boolean isActive() {
        return active;
    }

    /**
     * @param active The active to set.
     */
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * @return true if the Connection is starting
     */
    public synchronized boolean isStarting() {
        return starting;
    }
    
    public synchronized boolean isNetworkConnection() {
        return networkConnection;
    }

    protected synchronized void setStarting(boolean starting) {
        this.starting = starting;
    }

    /**
     * @return true if the Connection needs to stop
     */
    public synchronized boolean isPendingStop() {
        return pendingStop;
    }

    protected synchronized void setPendingStop(boolean pendingStop) {
        this.pendingStop = pendingStop;
    }

    public Response processBrokerInfo(BrokerInfo info) {
        if (info.isSlaveBroker()) {
            // stream messages from this broker (the master) to
            // the slave
            MutableBrokerFilter parent = (MutableBrokerFilter)broker.getAdaptor(MutableBrokerFilter.class);
            masterBroker = new MasterBroker(parent, transport);
            masterBroker.startProcessing();
            LOG.info("Slave Broker " + info.getBrokerName() + " is attached");
            BrokerService bService=connector.getBrokerService();
            bService.slaveConnectionEstablished();
            
        } else if (info.isNetworkConnection() && info.isDuplexConnection()) {
            // so this TransportConnection is the rear end of a network bridge
            // We have been requested to create a two way pipe ...
            try {
                Properties properties = MarshallingSupport.stringToProperties(info.getNetworkProperties());
                Map<String, String> props = createMap(properties);
                NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
                IntrospectionSupport.setProperties(config, props, "");
                config.setBrokerName(broker.getBrokerName());
                URI uri = broker.getVmConnectorURI();
                HashMap<String, String> map = new HashMap<String, String>(URISupport.parseParamters(uri));
                map.put("network", "true");
                map.put("async", "false");
                uri = URISupport.createURIWithQuery(uri, URISupport.createQueryString(map));
                Transport localTransport = TransportFactory.connect(uri);
                Transport remoteBridgeTransport = new ResponseCorrelator(transport);
                duplexBridge = NetworkBridgeFactory.createBridge(config, localTransport,
                                                                 remoteBridgeTransport);
                // now turn duplex off this side
                info.setDuplexConnection(false);
                duplexBridge.setCreatedByDuplex(true);
                duplexBridge.duplexStart(this,brokerInfo, info);

                LOG.info("Created Duplex Bridge back to " + info.getBrokerName());
                return null;
            } catch (Exception e) {
                LOG.error("Creating duplex network bridge", e);
            }
        }
        // We only expect to get one broker info command per connection
        if (this.brokerInfo != null) {
            LOG.warn("Unexpected extra broker info command received: " + info);
        }
        this.brokerInfo = info;
        broker.addBroker(this, info);
        networkConnection = true;

        List<TransportConnectionState> connectionStates = listConnectionStates();
        for (TransportConnectionState cs : connectionStates) {
            cs.getContext().setNetworkConnection(true);
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private HashMap<String, String> createMap(Properties properties) {
        return new HashMap(properties);
    }

    protected void dispatch(Command command) throws IOException {
        try {
            setMarkedCandidate(true);
            transport.oneway(command);
        } finally {
            setMarkedCandidate(false);
        }
    }

    public String getRemoteAddress() {
        return transport.getRemoteAddress();
    }

    public String getConnectionId() {
        List<TransportConnectionState> connectionStates = listConnectionStates();
        for (TransportConnectionState cs : connectionStates) {
            if (cs.getInfo().getClientId() != null) {
                return cs.getInfo().getClientId();
            }
            return cs.getInfo().getConnectionId().toString();
        }
        return null;
    }

    private ProducerBrokerExchange getProducerBrokerExchange(ProducerId id) {
        ProducerBrokerExchange result = producerExchanges.get(id);
        if (result == null) {
            synchronized (producerExchanges) {
                result = new ProducerBrokerExchange();
                TransportConnectionState state = lookupConnectionState(id);
                context = state.getContext();
                result.setConnectionContext(context);
                SessionState ss = state.getSessionState(id.getParentId());
                if (ss != null) {
                    result.setProducerState(ss.getProducerState(id));
                    ProducerState producerState = ss.getProducerState(id);
                    if (producerState != null && producerState.getInfo() != null) {
                        ProducerInfo info = producerState.getInfo();
                        result.setMutable(info.getDestination() == null
                                          || info.getDestination().isComposite());
                    }
                }
                producerExchanges.put(id, result);
            }
        } else {
            context = result.getConnectionContext();
        }
        return result;
    }

    private void removeProducerBrokerExchange(ProducerId id) {
        synchronized (producerExchanges) {
            producerExchanges.remove(id);
        }
    }

    private ConsumerBrokerExchange getConsumerBrokerExchange(ConsumerId id) {
        ConsumerBrokerExchange result = consumerExchanges.get(id);
        if (result == null) {
            synchronized (consumerExchanges) {
                result = new ConsumerBrokerExchange();
                TransportConnectionState state = lookupConnectionState(id);
                context = state.getContext();
                result.setConnectionContext(context);
                SessionState ss = state.getSessionState(id.getParentId());
                if (ss != null) {
                    ConsumerState cs = ss.getConsumerState(id);
                    if (cs != null) {
                        ConsumerInfo info = cs.getInfo();
                        if (info != null) {
                            if (info.getDestination() != null && info.getDestination().isPattern()) {
                                result.setWildcard(true);
                            }
                        }
                    }
                }
                consumerExchanges.put(id, result);
            }
        }
        return result;
    }

    private void removeConsumerBrokerExchange(ConsumerId id) {
        synchronized (consumerExchanges) {
            consumerExchanges.remove(id);
        }
    }

    public int getProtocolVersion() {
        return protocolVersion.get();
    }

    public Response processControlCommand(ControlCommand command) throws Exception {
        String control = command.getCommand();
        if (control != null && control.equals("shutdown")) {
            System.exit(0);
        }
        return null;
    }

    public Response processMessageDispatch(MessageDispatch dispatch) throws Exception {
        return null;
    }

    public Response processConnectionControl(ConnectionControl control) throws Exception {
        if(control != null) {
            faultTolerantConnection=control.isFaultTolerant();
        }
        return null;
    }

    public Response processConnectionError(ConnectionError error) throws Exception {
        return null;
    }

    public Response processConsumerControl(ConsumerControl control) throws Exception {
        return null;
    }

    protected synchronized TransportConnectionState registerConnectionState(ConnectionId connectionId,TransportConnectionState state) {
        TransportConnectionState cs = null;
        if (!connectionStateRegister.isEmpty() && !connectionStateRegister.doesHandleMultipleConnectionStates()){
        	//swap implementations
        	TransportConnectionStateRegister newRegister = new MapTransportConnectionStateRegister();
        	newRegister.intialize(connectionStateRegister);
        	connectionStateRegister = newRegister;
        }
    	cs= connectionStateRegister.registerConnectionState(connectionId, state);
    	return cs;
    }

    protected synchronized TransportConnectionState unregisterConnectionState(ConnectionId connectionId) {
        return connectionStateRegister.unregisterConnectionState(connectionId);
    }

    protected synchronized List<TransportConnectionState> listConnectionStates() {
        return connectionStateRegister.listConnectionStates();
    }

    protected synchronized TransportConnectionState lookupConnectionState(String connectionId) {
    	  return connectionStateRegister.lookupConnectionState(connectionId);
    }

    protected synchronized TransportConnectionState lookupConnectionState(ConsumerId id) {
    	  return connectionStateRegister.lookupConnectionState(id);
    }

    protected synchronized TransportConnectionState lookupConnectionState(ProducerId id) {
    	  return connectionStateRegister.lookupConnectionState(id);
    }

    protected synchronized TransportConnectionState lookupConnectionState(SessionId id) {
        return connectionStateRegister.lookupConnectionState(id);
    }

    protected synchronized TransportConnectionState lookupConnectionState(ConnectionId connectionId) {
        return connectionStateRegister.lookupConnectionState(connectionId);
    }

}
