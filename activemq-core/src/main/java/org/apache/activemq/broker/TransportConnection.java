/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.broker;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.activemq.Service;
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
import org.apache.activemq.state.ConsumerState;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.state.SessionState;
import org.apache.activemq.state.TransactionState;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
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
public class TransportConnection implements Service,Connection,Task,CommandVisitor{

    private static final Log log=LogFactory.getLog(TransportConnection.class);
    private static final Log transportLog=LogFactory.getLog(TransportConnection.class.getName()+".Transport");
    private static final Log serviceLog=LogFactory.getLog(TransportConnection.class.getName()+".Service");
    // Keeps track of the broker and connector that created this connection.
    protected final Broker broker;
    private MasterBroker masterBroker;
    protected final TransportConnector connector;
    private final Transport transport;
    private MessageAuthorizationPolicy messageAuthorizationPolicy;
    // Keeps track of the state of the connections.
    protected final ConcurrentHashMap localConnectionStates=new ConcurrentHashMap();
    protected final Map brokerConnectionStates;
    // The broker and wireformat info that was exchanged.
    protected BrokerInfo brokerInfo;
    private WireFormatInfo wireFormatInfo;
    // Used to do async dispatch.. this should perhaps be pushed down into the transport layer..
    protected final List <Command>dispatchQueue=Collections.synchronizedList(new LinkedList<Command>());
    protected TaskRunner taskRunner;
    protected final AtomicReference transportException = new AtomicReference();
    private boolean inServiceException=false;
    private ConnectionStatistics statistics=new ConnectionStatistics();
    private boolean manageable;
    private boolean slow;
    private boolean markedCandidate;
    private boolean blockedCandidate;
    private boolean blocked;
    private boolean connected;
    private boolean active;
    private boolean starting;
    private boolean pendingStop;
    private long timeStamp=0;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
	private final AtomicBoolean transportDisposed = new AtomicBoolean();
    private final AtomicBoolean disposed=new AtomicBoolean(false);
    private CountDownLatch stopLatch=new CountDownLatch(1);
    private final AtomicBoolean asyncException=new AtomicBoolean(false);
    private final Map<ProducerId,ProducerBrokerExchange>producerExchanges = new HashMap<ProducerId,ProducerBrokerExchange>();
    private final Map<ConsumerId,ConsumerBrokerExchange>consumerExchanges = new HashMap<ConsumerId,ConsumerBrokerExchange>();
    private CountDownLatch dispatchStoppedLatch = new CountDownLatch(1);
    protected AtomicBoolean dispatchStopped=new AtomicBoolean(false);
    private ConnectionContext context;
    private boolean networkConnection;
    private AtomicInteger protocolVersion=new AtomicInteger(CommandTypes.PROTOCOL_VERSION);
    private DemandForwardingBridge duplexBridge = null;
	final private TaskRunnerFactory taskRunnerFactory;
    
    static class ConnectionState extends org.apache.activemq.state.ConnectionState{

        private final ConnectionContext context;
        TransportConnection connection;

        public ConnectionState(ConnectionInfo info,ConnectionContext context,TransportConnection connection){
            super(info);
            this.context=context;
            this.connection=connection;
        }

        public ConnectionContext getContext(){
            return context;
        }

        public TransportConnection getConnection(){
            return connection;
        }
    }

    /**
     * @param connector
     * @param transport
     * @param broker
     * @param taskRunnerFactory - can be null if you want direct dispatch to the transport else commands are sent async.
     */
    public TransportConnection(TransportConnector connector,final Transport transport,Broker broker,
            TaskRunnerFactory taskRunnerFactory){
        this.connector=connector;
        this.broker=broker;
        RegionBroker rb=(RegionBroker)broker.getAdaptor(RegionBroker.class);
        brokerConnectionStates=rb.getConnectionStates();
        if(connector!=null){
            this.statistics.setParent(connector.getStatistics());
        }
        this.taskRunnerFactory=taskRunnerFactory;
        connector.setBrokerName(broker.getBrokerName());
        this.transport=transport;
        this.transport.setTransportListener(new DefaultTransportListener(){

            public void onCommand(Object o){
                Command command=(Command)o;
                Response response=service(command);
                if(response!=null){
                	dispatchSync(response);
                }
            }

            public void onException(IOException exception){
                serviceTransportException(exception);
            }
        });
        connected=true;
    }

    /**
     * Returns the number of messages to be dispatched to this connection
     * @return size of dispatch queue
     */
    public int getDispatchQueueSize(){
        return dispatchQueue.size();
    }

    public void serviceTransportException(IOException e){
        if(!disposed.get()){
        	transportException.set(e);
            if(transportLog.isDebugEnabled())
                transportLog.debug("Transport failed: "+e,e);
            ServiceSupport.dispose(this);
        }
    }

    /**
     * Calls the serviceException method in an async thread. Since handling a service exception closes a socket, we
     * should not tie up broker threads since client sockets may hang or cause deadlocks.
     * 
     * @param e
     */
    public void serviceExceptionAsync(final IOException e){
        if(asyncException.compareAndSet(false,true)){
            new Thread("Async Exception Handler"){

                public void run(){
                    serviceException(e);
                }
            }.start();
        }
    }

    /**
     * Closes a clients connection due to a detected error.
     * 
     * Errors are ignored if: the client is closing or broker is closing. Otherwise, the connection error transmitted to
     * the client before stopping it's transport.
     */
    public void serviceException(Throwable e){
        // are we a transport exception such as not being able to dispatch
        // synchronously to a transport
        if(e instanceof IOException){
            serviceTransportException((IOException)e);
        }
        // Handle the case where the broker is stopped
        // But the client is still connected.
        else if(e.getClass()==BrokerStoppedException.class){
            if(!disposed.get()){
                if(serviceLog.isDebugEnabled())
                    serviceLog.debug("Broker has been stopped.  Notifying client and closing his connection.");
                ConnectionError ce=new ConnectionError();
                ce.setException(e);
                dispatchSync(ce);
                // Wait a little bit to try to get the output buffer to flush the exption notification to the client.
                try{
                    Thread.sleep(500);
                }catch(InterruptedException ie){
                    Thread.currentThread().interrupt();
                }
                // Worst case is we just kill the connection before the notification gets to him.
                ServiceSupport.dispose(this);
            }
        }else if(!disposed.get()&&!inServiceException){
            inServiceException=true;
            try{
                serviceLog.error("Async error occurred: "+e,e);
                ConnectionError ce=new ConnectionError();
                ce.setException(e);
                dispatchAsync(ce);
            }finally{
                inServiceException=false;
            }
        }
    }

    public Response service(Command command){
        Response response=null;
        boolean responseRequired=command.isResponseRequired();
        int commandId=command.getCommandId();
        try{
            response=command.visit(this);
        }catch(Throwable e){
            if(responseRequired){
                if(serviceLog.isDebugEnabled()&&e.getClass()!=BrokerStoppedException.class)
                    serviceLog.debug("Error occured while processing sync command: "+e,e);
                response=new ExceptionResponse(e);
            }else{
                serviceException(e);
            }
        }
        if(responseRequired){
            if(response==null){
                response=new Response();
            }
            response.setCorrelationId(commandId);
        }
        
        // The context may have been flagged so that the response is not sent.
        if( context!=null ) {
        	if( context.isDontSendReponse() ) {
        		context.setDontSendReponse(false);
        		response=null;
        	}
            context=null;
        }
        
        return response;
    }

    protected ConnectionState lookupConnectionState(ConsumerId id){
        ConnectionState cs=(ConnectionState)localConnectionStates.get(id.getParentId().getParentId());
        if(cs==null)
            throw new IllegalStateException("Cannot lookup a consumer from a connection that had not been registered: "
                    +id.getParentId().getParentId());
        return cs;
    }

    protected ConnectionState lookupConnectionState(ProducerId id){
        ConnectionState cs=(ConnectionState)localConnectionStates.get(id.getParentId().getParentId());
        if(cs==null)
            throw new IllegalStateException("Cannot lookup a producer from a connection that had not been registered: "
                    +id.getParentId().getParentId());
        return cs;
    }

    protected ConnectionState lookupConnectionState(SessionId id){
        ConnectionState cs=(ConnectionState)localConnectionStates.get(id.getParentId());
        if(cs==null)
            throw new IllegalStateException("Cannot lookup a session from a connection that had not been registered: "
                    +id.getParentId());
        return cs;
    }

    protected ConnectionState lookupConnectionState(ConnectionId connectionId){
        ConnectionState cs=(ConnectionState)localConnectionStates.get(connectionId);
        if(cs==null)
            throw new IllegalStateException("Cannot lookup a connection that had not been registered: "+connectionId);
        return cs;
    }

    public Response processKeepAlive(KeepAliveInfo info) throws Exception{
        return null;
    }

    public Response processRemoveSubscription(RemoveSubscriptionInfo info) throws Exception{
        broker.removeSubscription(lookupConnectionState(info.getConnectionId()).getContext(),info);
        return null;
    }

    public synchronized Response processWireFormat(WireFormatInfo info) throws Exception{
        wireFormatInfo=info;
    	protocolVersion.set(info.getVersion());
        return null;
    }

    public Response processShutdown(ShutdownInfo info) throws Exception{
        stop();
        return null;
    }

    public Response processFlush(FlushCommand command) throws Exception{
        return null;
    }

    synchronized public Response processBeginTransaction(TransactionInfo info) throws Exception{
        ConnectionState cs=(ConnectionState)localConnectionStates.get(info.getConnectionId());
        context=null;
        if(cs!=null){
            context=cs.getContext();
        }
        if (cs == null) {
            throw new NullPointerException("Context is null");
        }
        // Avoid replaying dup commands
        if(cs.getTransactionState(info.getTransactionId())==null){
            cs.addTransactionState(info.getTransactionId());
            broker.beginTransaction(context,info.getTransactionId());
        }
        return null;
    }

    synchronized public Response processEndTransaction(TransactionInfo info) throws Exception{
        // No need to do anything. This packet is just sent by the client
        // make sure he is synced with the server as commit command could
        // come from a different connection.
        return null;
    }

    synchronized public Response processPrepareTransaction(TransactionInfo info) throws Exception{
        ConnectionState cs=(ConnectionState)localConnectionStates.get(info.getConnectionId());
        context=null;
        if(cs!=null){
            context=cs.getContext();
        }
        if (cs == null) {
            throw new NullPointerException("Context is null");
        }
        TransactionState transactionState=cs.getTransactionState(info.getTransactionId());
        if(transactionState==null)
            throw new IllegalStateException("Cannot prepare a transaction that had not been started: "
                    +info.getTransactionId());
        // Avoid dups.
        if(!transactionState.isPrepared()){
            transactionState.setPrepared(true);
            int result=broker.prepareTransaction(context,info.getTransactionId());
            transactionState.setPreparedResult(result);
            IntegerResponse response=new IntegerResponse(result);
            return response;
        }else{
            IntegerResponse response=new IntegerResponse(transactionState.getPreparedResult());
            return response;
        }
    }

    synchronized public Response processCommitTransactionOnePhase(TransactionInfo info) throws Exception{
        ConnectionState cs=(ConnectionState)localConnectionStates.get(info.getConnectionId());
        context=null;
        if(cs!=null){
            context=cs.getContext();
        }
        if (cs == null) {
            throw new NullPointerException("Context is null");
        }
        cs.removeTransactionState(info.getTransactionId());
        broker.commitTransaction(context,info.getTransactionId(),true);
        return null;
    }

    synchronized public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Exception{
        ConnectionState cs=(ConnectionState)localConnectionStates.get(info.getConnectionId());
        context=null;
        if(cs!=null){
            context=cs.getContext();
        }
        if (cs == null) {
            throw new NullPointerException("Context is null");
        }
        cs.removeTransactionState(info.getTransactionId());
        broker.commitTransaction(context,info.getTransactionId(),false);
        return null;
    }

    synchronized public Response processRollbackTransaction(TransactionInfo info) throws Exception{
        ConnectionState cs=(ConnectionState)localConnectionStates.get(info.getConnectionId());
        context=null;
        if(cs!=null){
            context=cs.getContext();
        }
        if (cs == null) {
            throw new NullPointerException("Context is null");
        }
        cs.removeTransactionState(info.getTransactionId());
        broker.rollbackTransaction(context,info.getTransactionId());
        return null;
    }

    synchronized public Response processForgetTransaction(TransactionInfo info) throws Exception{
        ConnectionState cs=(ConnectionState)localConnectionStates.get(info.getConnectionId());
        context=null;
        if(cs!=null){
            context=cs.getContext();
        }
        broker.forgetTransaction(context,info.getTransactionId());
        return null;
    }

    synchronized public Response processRecoverTransactions(TransactionInfo info) throws Exception{
        ConnectionState cs=(ConnectionState)localConnectionStates.get(info.getConnectionId());
        context=null;
        if(cs!=null){
            context=cs.getContext();
        }
        TransactionId[] preparedTransactions=broker.getPreparedTransactions(context);
        return new DataArrayResponse(preparedTransactions);
    }

    public Response processMessage(Message messageSend) throws Exception{
        ProducerId producerId=messageSend.getProducerId();
        ProducerBrokerExchange producerExchange=getProducerBrokerExchange(producerId);
        broker.send(producerExchange,messageSend);
        return null;
    }

    public Response processMessageAck(MessageAck ack) throws Exception{
        ConsumerBrokerExchange consumerExchange = getConsumerBrokerExchange(ack.getConsumerId());
        broker.acknowledge(consumerExchange,ack);
        return null;
    }

    public Response processMessagePull(MessagePull pull) throws Exception{
        return broker.messagePull(lookupConnectionState(pull.getConsumerId()).getContext(),pull);
    }

    public Response processMessageDispatchNotification(MessageDispatchNotification notification) throws Exception{
        broker.processDispatchNotification(notification);
        return null;
    }

    synchronized public Response processAddDestination(DestinationInfo info) throws Exception{
        ConnectionState cs=lookupConnectionState(info.getConnectionId());
        broker.addDestinationInfo(cs.getContext(),info);
        if(info.getDestination().isTemporary()){
            cs.addTempDestination(info);
        }
        return null;
    }

    synchronized public Response processRemoveDestination(DestinationInfo info) throws Exception{
        ConnectionState cs=lookupConnectionState(info.getConnectionId());
        broker.removeDestinationInfo(cs.getContext(),info);
        if(info.getDestination().isTemporary()){
            cs.removeTempDestination(info.getDestination());
        }
        return null;
    }

    synchronized public Response processAddProducer(ProducerInfo info) throws Exception{
        SessionId sessionId=info.getProducerId().getParentId();
        ConnectionId connectionId=sessionId.getParentId();
        ConnectionState cs=lookupConnectionState(connectionId);
        SessionState ss=cs.getSessionState(sessionId);
        if(ss==null)
            throw new IllegalStateException("Cannot add a producer to a session that had not been registered: "
                    +sessionId);
        // Avoid replaying dup commands
        if(!ss.getProducerIds().contains(info.getProducerId())){
            broker.addProducer(cs.getContext(),info);
            try{
                ss.addProducer(info);
            }catch(IllegalStateException e){
                broker.removeProducer(cs.getContext(),info);
            }
        }
        return null;
    }

    synchronized public Response processRemoveProducer(ProducerId id) throws Exception{
        SessionId sessionId=id.getParentId();
        ConnectionId connectionId=sessionId.getParentId();
        ConnectionState cs=lookupConnectionState(connectionId);
        SessionState ss=cs.getSessionState(sessionId);
        if(ss==null)
            throw new IllegalStateException("Cannot remove a producer from a session that had not been registered: "
                    +sessionId);
        ProducerState ps=ss.removeProducer(id);
        if(ps==null)
            throw new IllegalStateException("Cannot remove a producer that had not been registered: "+id);
        removeProducerBrokerExchange(id);
        broker.removeProducer(cs.getContext(),ps.getInfo());
        return null;
    }

    synchronized public Response processAddConsumer(ConsumerInfo info) throws Exception{
        SessionId sessionId=info.getConsumerId().getParentId();
        ConnectionId connectionId=sessionId.getParentId();
        ConnectionState cs=lookupConnectionState(connectionId);
        SessionState ss=cs.getSessionState(sessionId);
        if(ss==null)
            throw new IllegalStateException("Cannot add a consumer to a session that had not been registered: "
                    +sessionId);
        // Avoid replaying dup commands
        if(!ss.getConsumerIds().contains(info.getConsumerId())){
            broker.addConsumer(cs.getContext(),info);
            try{
                ss.addConsumer(info);
            }catch(IllegalStateException e){
                broker.removeConsumer(cs.getContext(),info);
            }
        }
        return null;
    }

    synchronized public Response processRemoveConsumer(ConsumerId id) throws Exception{
        SessionId sessionId=id.getParentId();
        ConnectionId connectionId=sessionId.getParentId();
        ConnectionState cs=lookupConnectionState(connectionId);
        SessionState ss=cs.getSessionState(sessionId);
        if(ss==null)
            throw new IllegalStateException("Cannot remove a consumer from a session that had not been registered: "
                    +sessionId);
        ConsumerState consumerState=ss.removeConsumer(id);
        if(consumerState==null)
            throw new IllegalStateException("Cannot remove a consumer that had not been registered: "+id);
        broker.removeConsumer(cs.getContext(),consumerState.getInfo());
        removeConsumerBrokerExchange(id);
        return null;
    }

    synchronized public Response processAddSession(SessionInfo info) throws Exception{
        ConnectionId connectionId=info.getSessionId().getParentId();
        ConnectionState cs=lookupConnectionState(connectionId);
        // Avoid replaying dup commands
        if(!cs.getSessionIds().contains(info.getSessionId())){
            broker.addSession(cs.getContext(),info);
            try{
                cs.addSession(info);
            }catch(IllegalStateException e){
                broker.removeSession(cs.getContext(),info);
            }
        }
        return null;
    }

    synchronized public Response processRemoveSession(SessionId id) throws Exception{
        ConnectionId connectionId=id.getParentId();
        ConnectionState cs=lookupConnectionState(connectionId);
        SessionState session=cs.getSessionState(id);
        if(session==null)
            throw new IllegalStateException("Cannot remove session that had not been registered: "+id);
        // Don't let new consumers or producers get added while we are closing this down.
        session.shutdown();
        // Cascade the connection stop to the consumers and producers.
        for(Iterator iter=session.getConsumerIds().iterator();iter.hasNext();){
            ConsumerId consumerId=(ConsumerId)iter.next();
            try{
                processRemoveConsumer(consumerId);
            }catch(Throwable e){
                log.warn("Failed to remove consumer: "+consumerId+". Reason: "+e,e);
            }
        }
        for(Iterator iter=session.getProducerIds().iterator();iter.hasNext();){
            ProducerId producerId=(ProducerId)iter.next();
            try{
                processRemoveProducer(producerId);
            }catch(Throwable e){
                log.warn("Failed to remove producer: "+producerId+". Reason: "+e,e);
            }
        }
        cs.removeSession(id);
        broker.removeSession(cs.getContext(),session.getInfo());
        return null;
    }

    synchronized public Response processAddConnection(ConnectionInfo info) throws Exception{
        ConnectionState state=(ConnectionState)brokerConnectionStates.get(info.getConnectionId());
        if(state!=null){
            // ConnectionInfo replay?? Chances are that it's a client reconnecting,
            // and we have not detected that that old connection died.. Kill the old connection
            // to make sure our state is in sync with the client.
            if(this!=state.getConnection()){
                log.debug("Killing previous stale connection: "+state.getConnection());
                state.getConnection().stop();
                if(!state.getConnection().stopLatch.await(15,TimeUnit.SECONDS)){
                    throw new Exception("Previous connection could not be clean up.");
                }
            }
        }
        log.debug("Setting up new connection: "+this);
        // Setup the context.
        String clientId=info.getClientId();
        context=new ConnectionContext();
        context.setConnection(this);
        context.setBroker(broker);
        context.setConnector(connector);
        context.setTransactions(new ConcurrentHashMap());
        context.setClientId(clientId);
        context.setUserName(info.getUserName());
        context.setConnectionId(info.getConnectionId());
        context.setClientMaster(info.isClientMaster());
        context.setWireFormatInfo(wireFormatInfo);
        context.setNetworkConnection(networkConnection);
        context.incrementReference();
        this.manageable=info.isManageable();
        state=new ConnectionState(info,context,this);
        brokerConnectionStates.put(info.getConnectionId(),state);
        localConnectionStates.put(info.getConnectionId(),state);
        broker.addConnection(context,info);
        if(info.isManageable()&&broker.isFaultTolerantConfiguration()){
            // send ConnectionCommand
            ConnectionControl command=new ConnectionControl();
            command.setFaultTolerant(broker.isFaultTolerantConfiguration());
            dispatchAsync(command);
        }
        return null;
    }

    synchronized public Response processRemoveConnection(ConnectionId id){
        ConnectionState cs=lookupConnectionState(id);
        // Don't allow things to be added to the connection state while we are shutting down.
        cs.shutdown();
        // Cascade the connection stop to the sessions.
        for(Iterator iter=cs.getSessionIds().iterator();iter.hasNext();){
            SessionId sessionId=(SessionId)iter.next();
            try{
                processRemoveSession(sessionId);
            }catch(Throwable e){
                serviceLog.warn("Failed to remove session "+sessionId,e);
            }
        }
        // Cascade the connection stop to temp destinations.
        for(Iterator iter=cs.getTempDesinations().iterator();iter.hasNext();){
            DestinationInfo di=(DestinationInfo)iter.next();
            try{
                broker.removeDestination(cs.getContext(),di.getDestination(),0);
            }catch(Throwable e){
                serviceLog.warn("Failed to remove tmp destination "+di.getDestination(),e);
            }
            iter.remove();
        }
        try{
            broker.removeConnection(cs.getContext(),cs.getInfo(),null);
        }catch(Throwable e){
            serviceLog.warn("Failed to remove connection "+cs.getInfo(),e);
        }
        ConnectionState state=(ConnectionState)localConnectionStates.remove(id);
        if(state!=null){
            // If we are the last reference, we should remove the state
            // from the broker.
            if(state.getContext().decrementReference()==0){
                brokerConnectionStates.remove(id);
            }
        }
        return null;
    }

    
    public Response processProducerAck(ProducerAck ack) throws Exception {
		// A broker should not get ProducerAck messages.
		return null;
	}    

    public Connector getConnector(){
        return connector;
    }

    public void dispatchSync(Command message){
        getStatistics().getEnqueues().increment();
        try {
            processDispatch(message);
        } catch (IOException e) {
            serviceExceptionAsync(e);
        }
    }

    public void dispatchAsync(Command message){
        if( !disposed.get() ) {
            getStatistics().getEnqueues().increment();
            if( taskRunner==null ) {
                dispatchSync( message );
            } else {
                dispatchQueue.add(message);
                try {
                    taskRunner.wakeup();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } else {
            if(message.isMessageDispatch()) {
                MessageDispatch md=(MessageDispatch) message;
                Runnable sub=md.getTransmitCallback();
                broker.processDispatch(md);
                if(sub!=null){
                    sub.run();
                }
             }
        }
    }

    protected void processDispatch(Command command) throws IOException {
        try {
            if( !disposed.get() ) {
                 dispatch(command);
            }
       } finally {

            if(command.isMessageDispatch()){
                MessageDispatch md=(MessageDispatch) command;
                Runnable sub=md.getTransmitCallback();
                broker.processDispatch(md);
                if(sub!=null){
                    sub.run();
                }
            }

            getStatistics().getDequeues().increment();
        }
     }   



    public boolean iterate(){
        try {
            if( disposed.get() ) {
                 if( dispatchStopped.compareAndSet(false, true)) {                                                             
                     if( transportException.get()==null ) {
                         try {
                             dispatch(new ShutdownInfo());
                         } catch (Throwable ignore) {
                         }
                     }
                     dispatchStoppedLatch.countDown();
                 }
                 return false;                           
             } 

             if( !dispatchStopped.get() )  {

                 if( dispatchQueue.isEmpty() ) {
                     return false;
                 }
                Command command = dispatchQueue.remove(0);
                 processDispatch( command );
                 return true;
             }
            return false;

         } catch (IOException e) {
             if( dispatchStopped.compareAndSet(false, true)) {                                                                     
                 dispatchStoppedLatch.countDown();
             }
             serviceExceptionAsync(e);
             return false;                           
         }
    }

    /**
     * Returns the statistics for this connection
     */
    public ConnectionStatistics getStatistics(){
        return statistics;
    }

    public MessageAuthorizationPolicy getMessageAuthorizationPolicy(){
        return messageAuthorizationPolicy;
    }

    public void setMessageAuthorizationPolicy(MessageAuthorizationPolicy messageAuthorizationPolicy){
        this.messageAuthorizationPolicy=messageAuthorizationPolicy;
    }

    public boolean isManageable(){
        return manageable;
    }

    public synchronized void start() throws Exception{
        starting=true;
        try{
            transport.start();
        	
        	if (taskRunnerFactory != null) {
				taskRunner = taskRunnerFactory.createTaskRunner(this, "ActiveMQ Connection Dispatcher: " + getRemoteAddress());
			} else {
				taskRunner = null;
			} 

            active=true;
            this.processDispatch(connector.getBrokerInfo());
            connector.onStarted(this);
        }finally{
            // stop() can be called from within the above block,
            // but we want to be sure start() completes before
            // stop() runs, so queue the stop until right now:
            starting=false;
            if(pendingStop){
                log.debug("Calling the delayed stop()");
                stop();
            }
        }
    }

    public void stop() throws Exception{
        // If we're in the middle of starting
        // then go no further... for now.
        synchronized(this){
            pendingStop=true;
            if(starting){
                log.debug("stop() called in the middle of start(). Delaying...");
                return;
            }
        }
        if(stopped.compareAndSet(false,true)){
            log.debug("Stopping connection: "+transport.getRemoteAddress());
            connector.onStopped(this);
            try{
                synchronized(this){
                    if(masterBroker!=null){
                        masterBroker.stop();
                    }
                    if(duplexBridge!=null){
                        duplexBridge.stop();
                    }
                    // If the transport has not failed yet,
                    // notify the peer that we are doing a normal shutdown.
                    if(transportException==null){
                        transport.oneway(new ShutdownInfo());
                    }
                }
                
            }catch(Exception ignore){
                log.trace("Exception caught stopping",ignore);
            }
            transport.stop();
            active=false;
            if(disposed.compareAndSet(false,true)){

                // Let all the connection contexts know we are shutting down
                // so that in progress operations can notice and unblock.
                 ArrayList l=new ArrayList(localConnectionStates.values());
                 for(Iterator iter=l.iterator();iter.hasNext();){
                     ConnectionState cs=(ConnectionState) iter.next();
                     cs.getContext().getStopping().set(true);
                 }            	
            	
 		        if( taskRunner!=null ) {
                    taskRunner.wakeup();
                    // Give it a change to stop gracefully.
                    dispatchStoppedLatch.await(5, TimeUnit.SECONDS);
                    disposeTransport();
		            taskRunner.shutdown();
                } else {
                    disposeTransport();
                }

		        if( taskRunner!=null )
		            taskRunner.shutdown();
		        
                // Run the MessageDispatch callbacks so that message references get cleaned up.
                for (Iterator iter = dispatchQueue.iterator(); iter.hasNext();) {
                    Command command = (Command) iter.next();
                    if(command.isMessageDispatch()) {
                        MessageDispatch md=(MessageDispatch) command;
                        Runnable sub=md.getTransmitCallback();
                        broker.processDispatch(md);
                        if(sub!=null){
                            sub.run();
                        }
                    }
                } 
                //
                // Remove all logical connection associated with this connection
                // from the broker.
                if(!broker.isStopped()){
                	l=new ArrayList(localConnectionStates.keySet());
                    for(Iterator iter=l.iterator();iter.hasNext();){
                        ConnectionId connectionId=(ConnectionId)iter.next();
                        try{
                            log.debug("Cleaning up connection resources.");
                            processRemoveConnection(connectionId);
                        }catch(Throwable ignore){
                            ignore.printStackTrace();
                        }
                    }
                    if(brokerInfo!=null){
                        broker.removeBroker(this,brokerInfo);
                    }
                }
                stopLatch.countDown();
            }
        }
    }

    /**
     * @return Returns the blockedCandidate.
     */
    public boolean isBlockedCandidate(){
        return blockedCandidate;
    }

    /**
     * @param blockedCandidate The blockedCandidate to set.
     */
    public void setBlockedCandidate(boolean blockedCandidate){
        this.blockedCandidate=blockedCandidate;
    }

    /**
     * @return Returns the markedCandidate.
     */
    public boolean isMarkedCandidate(){
        return markedCandidate;
    }

    /**
     * @param markedCandidate The markedCandidate to set.
     */
    public void setMarkedCandidate(boolean markedCandidate){
        this.markedCandidate=markedCandidate;
        if(!markedCandidate){
            timeStamp=0;
            blockedCandidate=false;
        }
    }

    /**
     * @param slow The slow to set.
     */
    public void setSlow(boolean slow){
        this.slow=slow;
    }

    /**
     * @return true if the Connection is slow
     */
    public boolean isSlow(){
        return slow;
    }

    /**
     * @return true if the Connection is potentially blocked
     */
    public boolean isMarkedBlockedCandidate(){
        return markedCandidate;
    }

    /**
     * Mark the Connection, so we can deem if it's collectable on the next sweep
     */
    public void doMark(){
        if(timeStamp==0){
            timeStamp=System.currentTimeMillis();
        }
    }

    /**
     * @return if after being marked, the Connection is still writing
     */
    public boolean isBlocked(){
        return blocked;
    }

    /**
     * @return true if the Connection is connected
     */
    public boolean isConnected(){
        return connected;
    }

    /**
     * @param blocked The blocked to set.
     */
    public void setBlocked(boolean blocked){
        this.blocked=blocked;
    }

    /**
     * @param connected The connected to set.
     */
    public void setConnected(boolean connected){
        this.connected=connected;
    }

    /**
     * @return true if the Connection is active
     */
    public boolean isActive(){
        return active;
    }

    /**
     * @param active The active to set.
     */
    public void setActive(boolean active){
        this.active=active;
    }

    /**
     * @return true if the Connection is starting
     */
    public synchronized boolean isStarting(){
        return starting;
    }

    synchronized protected void setStarting(boolean starting){
        this.starting=starting;
    }

    /**
     * @return true if the Connection needs to stop
     */
    public synchronized boolean isPendingStop(){
        return pendingStop;
    }

    protected synchronized void setPendingStop(boolean pendingStop){
        this.pendingStop=pendingStop;
    }

    public synchronized Response processBrokerInfo(BrokerInfo info){
        if(info.isSlaveBroker()){
            // stream messages from this broker (the master) to
            // the slave
            MutableBrokerFilter parent=(MutableBrokerFilter)broker.getAdaptor(MutableBrokerFilter.class);
            masterBroker=new MasterBroker(parent,transport);
            masterBroker.startProcessing();
            log.info("Slave Broker "+info.getBrokerName()+" is attached");
        }else if (info.isNetworkConnection() && info.isDuplexConnection()) {
            //so this TransportConnection is the rear end of a network bridge
            //We have been requested to create a two way pipe ...
            try{
                Properties props = MarshallingSupport.stringToProperties(info.getNetworkProperties());
                NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
                IntrospectionSupport.setProperties(config,props,"");
                config.setBrokerName(broker.getBrokerName());
                URI uri = broker.getVmConnectorURI();
                HashMap map = new HashMap(URISupport.parseParamters(uri));
                map.put("network", "true");
                map.put("async","false");
                uri = URISupport.createURIWithQuery(uri, URISupport.createQueryString(map));
                Transport localTransport = TransportFactory.connect(uri);
                Transport remoteBridgeTransport = new ResponseCorrelator(transport);
                duplexBridge = NetworkBridgeFactory.createBridge(config,localTransport,remoteBridgeTransport);
                //now turn duplex off this side
                info.setDuplexConnection(false);
                duplexBridge.setCreatedByDuplex(true);
                duplexBridge.duplexStart(brokerInfo,info);
                
                log.info("Created Duplex Bridge back to " + info.getBrokerName());
                return null;
            }catch(Exception e){
               log.error("Creating duplex network bridge",e);
            }
        }
        // We only expect to get one broker info command per connection
        if(this.brokerInfo!=null){
            log.warn("Unexpected extra broker info command received: "+info);
        }
        this.brokerInfo=info;
        broker.addBroker(this,info);
        networkConnection = true;
        for (Iterator iter = localConnectionStates.values().iterator(); iter.hasNext();) {
            ConnectionState cs = (ConnectionState) iter.next();
            cs.getContext().setNetworkConnection(true);
        }   
        
        return null;
    }

    protected void dispatch(Command command) throws IOException{
        try{
            setMarkedCandidate(true);
            transport.oneway(command);
        }finally{
            setMarkedCandidate(false);
        }
    }

    public String getRemoteAddress(){
        return transport.getRemoteAddress();
    }
    
    public String getConnectionId() {
        Iterator iterator = localConnectionStates.values().iterator();
        ConnectionState object = (ConnectionState) iterator.next();
        if( object == null ) {
            return null;
        }
        if( object.getInfo().getClientId() !=null )
            return object.getInfo().getClientId();
        return object.getInfo().getConnectionId().toString();
    }    
    
    private ProducerBrokerExchange getProducerBrokerExchange(ProducerId id){
        ProducerBrokerExchange result=producerExchanges.get(id);
        if(result==null){
            synchronized(producerExchanges){
                result=new ProducerBrokerExchange();
                ConnectionState state=lookupConnectionState(id);
                context=state.getContext();
                result.setConnectionContext(context);
                SessionState ss=state.getSessionState(id.getParentId());
                if(ss!=null){
                    result.setProducerState(ss.getProducerState(id));
                    ProducerState producerState=ss.getProducerState(id);
                    if(producerState!=null&&producerState.getInfo()!=null){
                        ProducerInfo info=producerState.getInfo();
                        result.setMutable(info.getDestination()==null||info.getDestination().isComposite());
                    }
                }
                producerExchanges.put(id,result);
            }
        } else {
        	context = result.getConnectionContext();
        }
        return result;
    }
    
    private void removeProducerBrokerExchange(ProducerId id) {
        synchronized(producerExchanges) {
            producerExchanges.remove(id);
        }
    }
    
    private ConsumerBrokerExchange getConsumerBrokerExchange(ConsumerId id){
        ConsumerBrokerExchange result=consumerExchanges.get(id);
        if(result==null){
            synchronized(consumerExchanges){
                result=new ConsumerBrokerExchange();
                ConnectionState state=lookupConnectionState(id);
                context=state.getContext();
                result.setConnectionContext(context);
                SessionState ss=state.getSessionState(id.getParentId());
                if(ss!=null){
                    ConsumerState cs=ss.getConsumerState(id);
                    if(cs!=null){
                        ConsumerInfo info=cs.getInfo();
                        if(info!=null){
                            if(info.getDestination()!=null&&info.getDestination().isPattern()){
                                result.setWildcard(true);
                            }
                        }
                    }
                }
                consumerExchanges.put(id,result);
            }
        }
        return result;
    }
    
    private void removeConsumerBrokerExchange(ConsumerId id) {
        synchronized(consumerExchanges) {
            consumerExchanges.remove(id);
        }
    }
    
	protected void disposeTransport(){
        if(transportDisposed.compareAndSet(false,true)){
            try{
                transport.stop();
                active=false;
                log.debug("Stopped connection: "+transport.getRemoteAddress());
            }catch(Exception e){
                log.debug("Could not stop transport: "+e,e);
            }
        }
    }
    
   	
	public int getProtocolVersion() {
		return protocolVersion.get();
	}

	public Response processControlCommand(ControlCommand command) throws Exception {
        String control = command.getCommand();
	    if (control != null && control.equals("shutdown"))
	        System.exit(0);
         return null;
	}

	public Response processMessageDispatch(MessageDispatch dispatch) throws Exception {
		return null;
	}

	public Response processConnectionControl(ConnectionControl control) throws Exception {
		return null;
	}

	public Response processConnectionError(ConnectionError error) throws Exception {
		return null;
	}

	public Response processConsumerControl(ConsumerControl control) throws Exception {
		return null;
	}

}
