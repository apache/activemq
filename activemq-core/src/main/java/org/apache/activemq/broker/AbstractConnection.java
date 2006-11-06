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
package org.apache.activemq.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.activemq.Service;
import org.apache.activemq.broker.region.ConnectionStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
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
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.state.ConsumerState;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.state.SessionState;
import org.apache.activemq.state.TransactionState;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;


/**
 * @version $Revision: 1.26 $
 */
public abstract class AbstractConnection implements Service, Connection, Task, CommandVisitor {

    private static final Log log = LogFactory.getLog(AbstractConnection.class.getName());
    private static final Log transportLog = LogFactory.getLog(AbstractConnection.class.getName() + ".Transport");
    private static final Log serviceLog = LogFactory.getLog(AbstractConnection.class.getName() + ".Service");
    
    protected final Broker broker;
    private MessageAuthorizationPolicy messageAuthorizationPolicy;
    protected final List dispatchQueue = Collections.synchronizedList(new LinkedList());
    protected final TaskRunner taskRunner;
    protected final TransportConnector connector;
    protected BrokerInfo brokerInfo;
    private ConnectionStatistics statistics = new ConnectionStatistics();
    private boolean inServiceException=false;
    private boolean manageable;

    protected final ConcurrentHashMap localConnectionStates = new ConcurrentHashMap();
    protected final Map brokerConnectionStates;
    
    private WireFormatInfo wireFormatInfo;    
    protected final AtomicBoolean disposed=new AtomicBoolean(false);
    protected IOException transportException;
    private CountDownLatch stopLatch = new CountDownLatch(1);
    
    static class ConnectionState extends org.apache.activemq.state.ConnectionState {
        private final ConnectionContext context;
        AbstractConnection connection;

        public ConnectionState(ConnectionInfo info, ConnectionContext context, AbstractConnection connection) {
            super(info);
            this.context = context;
            this.connection=connection;
        }
        
        public ConnectionContext getContext() {
            return context;
        }
        
        public AbstractConnection getConnection() {
            return connection;
        }
        
    }

    
    /**
     * @param connector
     * @param transport
     * @param broker
     * @param taskRunnerFactory - can be null if you want direct dispatch to the transport else commands are sent async.
     */
    public AbstractConnection(TransportConnector connector, Broker broker, TaskRunnerFactory taskRunnerFactory) {
        
        this.connector = connector;
        this.broker = broker;
        
        RegionBroker rb = (RegionBroker) broker.getAdaptor(RegionBroker.class);
        brokerConnectionStates = rb.getConnectionStates();
        
        if (connector != null) {
            this.statistics.setParent(connector.getStatistics());
        }
        
        if( taskRunnerFactory != null ) {
            taskRunner = taskRunnerFactory.createTaskRunner( this, "ActiveMQ Connection Dispatcher: "+System.identityHashCode(this) );
        }
        else { 
            taskRunner = null;
        }
        
    }

    /**
     * Returns the number of messages to be dispatched to this connection
     */
    public int getDispatchQueueSize() {
        return dispatchQueue.size();
    }
    
    public void start() throws Exception {
        this.processDispatch(connector.getBrokerInfo());
    }

    public void stop() throws Exception{
        if(disposed.compareAndSet(false, true)) {
	        
	        if( taskRunner!=null )
	            taskRunner.shutdown();
	        
	        // Clear out the dispatch queue to release any memory that
	        // is being held on to.
	        dispatchQueue.clear();
	        
	        //
	        // Remove all logical connection associated with this connection
	        // from the broker.
	        if(!broker.isStopped()){
	            ArrayList l=new ArrayList(localConnectionStates.keySet());
	            for(Iterator iter=l.iterator();iter.hasNext();){
	                ConnectionId connectionId=(ConnectionId) iter.next();
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
    
    public void serviceTransportException(IOException e) {
        if( !disposed.get() ) {
            transportException = e; 
            if( transportLog.isDebugEnabled() )
                transportLog.debug("Transport failed: "+e,e);
            ServiceSupport.dispose(this);
        }
    }
    
    /**
     * Calls the serviceException method in an async thread.  Since 
     * handling a service exception closes a socket, we should not tie 
     * up broker threads since client sockets may hang or cause deadlocks.
     * 
     * @param e
     */
	public void serviceExceptionAsync(final IOException e) {
		new Thread("Async Exception Handler") {
			public void run() {
				serviceException(e);
			}
		}.start();
	}

	/**
	 * Closes a clients connection due to a detected error.
	 * 
	 * Errors are ignored if: the client is closing or broker is closing.
	 * Otherwise, the connection error transmitted to the client before stopping it's
	 * transport.
	 */
    public void serviceException(Throwable e) {
        // are we a transport exception such as not being able to dispatch
        // synchronously to a transport
        if (e instanceof IOException) {
            serviceTransportException((IOException) e);
        }
        
        // Handle the case where the broker is stopped 
        // But the client is still connected.
        else if (e.getClass() == BrokerStoppedException.class ) {
            if( !disposed.get() ) {
                if( serviceLog.isDebugEnabled() )
                    serviceLog.debug("Broker has been stopped.  Notifying client and closing his connection.");
                
                ConnectionError ce = new ConnectionError();
                ce.setException(e);
                dispatchSync(ce);
                
                // Wait a little bit to try to get the output buffer to flush the exption notification to the client.
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                
                // Worst case is we just kill the connection before the notification gets to him.
                ServiceSupport.dispose(this);
            }
        }
        
        else if( !disposed.get() && !inServiceException ) {
            inServiceException = true;
                try {
                if( serviceLog.isDebugEnabled() )
                    serviceLog.debug("Async error occurred: "+e,e);
                ConnectionError ce = new ConnectionError();
                ce.setException(e);
                dispatchAsync(ce);
            } finally {
                inServiceException = false;
            }
        } 
    }

    public Response service(Command command) {
        
        Response response=null;
        boolean responseRequired = command.isResponseRequired();
        int commandId = command.getCommandId();
        try {
            response = command.visit(this);
        } catch ( Throwable e ) {
            if( responseRequired ) {
                if( serviceLog.isDebugEnabled() && e.getClass()!=BrokerStoppedException.class )
                    serviceLog.debug("Error occured while processing sync command: "+e,e);
                response = new ExceptionResponse(e);
            } else {
                serviceException(e);
            }
        }        
        if( responseRequired ) {
            if( response == null ) {
                response = new Response();                
            }
            response.setCorrelationId(commandId);
        }
        return response;
        
    }
    
    protected ConnectionState lookupConnectionState(ConsumerId id) {
        ConnectionState cs = (ConnectionState) localConnectionStates.get(id.getParentId().getParentId());
        if( cs== null )
            throw new IllegalStateException("Cannot lookup a consumer from a connection that had not been registered: "+id.getParentId().getParentId());
        return cs;
    }
    protected ConnectionState lookupConnectionState(ProducerId id) {
        ConnectionState cs = (ConnectionState) localConnectionStates.get(id.getParentId().getParentId());
        if( cs== null )
            throw new IllegalStateException("Cannot lookup a producer from a connection that had not been registered: "+id.getParentId().getParentId());        
        return cs;
    }
    protected ConnectionState lookupConnectionState(SessionId id) {
        ConnectionState cs = (ConnectionState) localConnectionStates.get(id.getParentId());
        if( cs== null )
            throw new IllegalStateException("Cannot lookup a session from a connection that had not been registered: "+id.getParentId());        
        return cs;
    }
    protected ConnectionState lookupConnectionState(ConnectionId connectionId) {
        ConnectionState cs = (ConnectionState) localConnectionStates.get(connectionId);
        if( cs== null )
            throw new IllegalStateException("Cannot lookup a connection that had not been registered: "+connectionId);
        return cs;
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
        return null;
    }
    
    public Response processShutdown(ShutdownInfo info) throws Exception {
        stop();
        return null;
    }
     
    public Response processFlush(FlushCommand command) throws Exception {
        return null;
    }

    public Response processBeginTransaction(TransactionInfo info) throws Exception {
        ConnectionState cs = (ConnectionState) localConnectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        
        // Avoid replaying dup commands
        if( cs.getTransactionState(info.getTransactionId())==null ) {
            cs.addTransactionState(info.getTransactionId());
            broker.beginTransaction(context, info.getTransactionId());
        }
        return null;
    }
    
    public Response processEndTransaction(TransactionInfo info) throws Exception {
        // No need to do anything.  This packet is just sent by the client
        // make sure he is synced with the server as commit command could
        // come from a different connection.
        return null;
    }
    
    public Response processPrepareTransaction(TransactionInfo info) throws Exception {
        ConnectionState cs = (ConnectionState) localConnectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        
        TransactionState transactionState = cs.getTransactionState(info.getTransactionId());
        if( transactionState == null )
            throw new IllegalStateException("Cannot prepare a transaction that had not been started: "+info.getTransactionId());

        // Avoid dups.
        if( !transactionState.isPrepared() ) {
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
        ConnectionState cs = (ConnectionState) localConnectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        
        cs.removeTransactionState(info.getTransactionId());
        broker.commitTransaction(context, info.getTransactionId(), true);

        return null;
        
    }

    public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Exception {
        ConnectionState cs = (ConnectionState) localConnectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        
        cs.removeTransactionState(info.getTransactionId());
        broker.commitTransaction(context, info.getTransactionId(), false);
        return null;
    }

    public Response processRollbackTransaction(TransactionInfo info) throws Exception {
        ConnectionState cs = (ConnectionState) localConnectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        
        cs.removeTransactionState(info.getTransactionId());
        broker.rollbackTransaction(context, info.getTransactionId());
        return null;
    }
    
    public Response processForgetTransaction(TransactionInfo info) throws Exception {
        ConnectionState cs = (ConnectionState) localConnectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        broker.forgetTransaction(context, info.getTransactionId());
        return null;
    }
    
    public Response processRecoverTransactions(TransactionInfo info) throws Exception {
        ConnectionState cs = (ConnectionState) localConnectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        TransactionId[] preparedTransactions = broker.getPreparedTransactions(context);
        return new DataArrayResponse(preparedTransactions);
    }


    public Response processMessage(Message messageSend) throws Exception {
        
        ProducerId producerId = messageSend.getProducerId();
        ConnectionState state = lookupConnectionState(producerId);
        ConnectionContext context = state.getContext();
        
        // If the message originates from this client connection, 
        // then, finde the associated producer state so we can do some dup detection.
        ProducerState producerState=null;        
        if( messageSend.getMessageId().getProducerId().equals( messageSend.getProducerId() ) ) {
            SessionState ss = state.getSessionState(producerId.getParentId());
            if( ss == null )
                throw new IllegalStateException("Cannot send from a session that had not been registered: "+producerId.getParentId());
            producerState = ss.getProducerState(producerId); 
        }
        
        if( producerState == null ) {
            broker.send(context, messageSend);
        } else {
            // Avoid Dups.
            long seq = messageSend.getMessageId().getProducerSequenceId();
            if( seq > producerState.getLastSequenceId() ) {
                producerState.setLastSequenceId(seq);
                broker.send(context, messageSend);
            }
        }
        
        return null;
    }

    public Response processMessageAck(MessageAck ack) throws Exception {
        broker.acknowledge(lookupConnectionState(ack.getConsumerId()).getContext(), ack);
        return null;
    }
    
    public Response processMessagePull(MessagePull pull) throws Exception {
        return broker.messagePull(lookupConnectionState(pull.getConsumerId()).getContext(), pull);
    }

    public Response processMessageDispatchNotification(MessageDispatchNotification notification) throws Exception{
        broker.processDispatchNotification(notification);
        return null;
    }

    public Response processBrokerInfo(BrokerInfo info) {
        
        // We only expect to get one broker info command per connection
        if( this.brokerInfo!=null ) {
            log.warn("Unexpected extra broker info command received: "+info);
        }
        
        this.brokerInfo = info;
        broker.addBroker(this, info);
        return null;
    }

    public Response processAddDestination(DestinationInfo info) throws Exception {
        ConnectionState cs = lookupConnectionState(info.getConnectionId());
        broker.addDestinationInfo(cs.getContext(), info);
        if( info.getDestination().isTemporary() ) {
            cs.addTempDestination(info);
        }
        return null;
    }

    public Response processRemoveDestination(DestinationInfo info) throws Exception {
        ConnectionState cs = lookupConnectionState(info.getConnectionId());
        broker.removeDestinationInfo(cs.getContext(), info);
        if( info.getDestination().isTemporary() ) {
            cs.removeTempDestination(info.getDestination());
        }
        return null;
    }


    public Response processAddProducer(ProducerInfo info) throws Exception {
        SessionId sessionId = info.getProducerId().getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        
        ConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if( ss == null )
            throw new IllegalStateException("Cannot add a producer to a session that had not been registered: "+sessionId);

        // Avoid replaying dup commands
        if( !ss.getProducerIds().contains(info.getProducerId()) ) {
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
        
        ConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if( ss == null )
            throw new IllegalStateException("Cannot remove a producer from a session that had not been registered: "+sessionId);
        ProducerState ps = ss.removeProducer(id);
        if( ps == null )
            throw new IllegalStateException("Cannot remove a producer that had not been registered: "+id);
        
        broker.removeProducer(cs.getContext(), ps.getInfo());
        return null;
    }

    public Response processAddConsumer(ConsumerInfo info) throws Exception {
        SessionId sessionId = info.getConsumerId().getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        
        ConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if( ss == null )
            throw new IllegalStateException("Cannot add a consumer to a session that had not been registered: "+sessionId);

        // Avoid replaying dup commands
        if( !ss.getConsumerIds().contains(info.getConsumerId()) ) {
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
        
        ConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if( ss == null )
            throw new IllegalStateException("Cannot remove a consumer from a session that had not been registered: "+sessionId);
        ConsumerState consumerState = ss.removeConsumer(id);
        if( consumerState == null )
            throw new IllegalStateException("Cannot remove a consumer that had not been registered: "+id);
        
        broker.removeConsumer(cs.getContext(), consumerState.getInfo());
        return null;
    }
    
    public Response processAddSession(SessionInfo info) throws Exception {
        ConnectionId connectionId = info.getSessionId().getParentId();
        ConnectionState cs = lookupConnectionState(connectionId);
        
        // Avoid replaying dup commands
        if( !cs.getSessionIds().contains(info.getSessionId()) ) {
            broker.addSession(cs.getContext(), info);
            try {
                cs.addSession(info);
            } catch (IllegalStateException e) {
                broker.removeSession(cs.getContext(), info);
            }
        }
        return null;
    }
    
    public Response processRemoveSession(SessionId id) throws Exception {
        
        ConnectionId connectionId = id.getParentId();
        
        ConnectionState cs = lookupConnectionState(connectionId);
        SessionState session = cs.getSessionState(id);

        if( session == null )
            throw new IllegalStateException("Cannot remove session that had not been registered: "+id);

        // Don't let new consumers or producers get added while we are closing this down.
        session.shutdown();
        
        // Cascade the connection stop to the consumers and producers.
        for (Iterator iter = session.getConsumerIds().iterator(); iter.hasNext();) {
            ConsumerId consumerId = (ConsumerId) iter.next();
            try {
                processRemoveConsumer(consumerId);
            }
            catch (Throwable e) {
                log.warn("Failed to remove consumer: " + consumerId + ". Reason: " + e, e);
            }
        }
        for (Iterator iter = session.getProducerIds().iterator(); iter.hasNext();) {
            ProducerId producerId = (ProducerId) iter.next();
            try {
                processRemoveProducer(producerId);
            }
            catch (Throwable e) {
                log.warn("Failed to remove producer: " + producerId + ". Reason: " + e, e);
            }
        }
        cs.removeSession(id);
        broker.removeSession(cs.getContext(), session.getInfo());
        return null;
    }
    
    public Response processAddConnection(ConnectionInfo info) throws Exception {

    	ConnectionState state = (ConnectionState) brokerConnectionStates.get(info.getConnectionId());
    	
    	if( state !=null ) {
    		// ConnectionInfo replay??  Chances are that it's a client reconnecting,
    		// and we have not detected that that old connection died.. Kill the old connection
    		// to make sure our state is in sync with the client.
    		if( this != state.getConnection() ) {
    			log.debug("Killing previous stale connection: "+state.getConnection());
    			state.getConnection().stop();
    			if( !state.getConnection().stopLatch.await(15, TimeUnit.SECONDS) ) {
    				throw new Exception("Previous connection could not be clean up.");
    			}
    		}
    	}
    	
		log.debug("Setting up new connection: "+this);

    	
        // Setup the context.
        String clientId = info.getClientId();
        ConnectionContext context = new ConnectionContext();
        context.setConnection(this);
        context.setBroker(broker);
        context.setConnector(connector);
        context.setTransactions(new ConcurrentHashMap());
        context.setClientId(clientId);
        context.setUserName(info.getUserName());
        context.setConnectionId(info.getConnectionId());
        context.setWireFormatInfo(wireFormatInfo);
        context.incrementReference();
        this.manageable = info.isManageable();
        
        state = new ConnectionState(info, context, this);
        brokerConnectionStates.put(info.getConnectionId(), state);
        localConnectionStates.put(info.getConnectionId(), state);           
        
        broker.addConnection(context, info);
        if (info.isManageable() && broker.isFaultTolerantConfiguration()){
            //send ConnectionCommand
            ConnectionControl command = new ConnectionControl();
            command.setFaultTolerant(broker.isFaultTolerantConfiguration());
            dispatchAsync(command);
        }

        return null;
    }
    
    public Response processRemoveConnection(ConnectionId id)  {
        
        ConnectionState cs = lookupConnectionState(id);
        
        // Don't allow things to be added to the connection state while we are shutting down.
        cs.shutdown();
        
        // Cascade the connection stop to the sessions.
        for (Iterator iter = cs.getSessionIds().iterator(); iter.hasNext();) {
           
                SessionId sessionId = (SessionId) iter.next();
                try{
                processRemoveSession(sessionId);
            }catch(Throwable e){
                serviceLog.warn("Failed to remove session " + sessionId,e);
            }
        }
        
        // Cascade the connection stop to temp destinations.
        for (Iterator iter = cs.getTempDesinations().iterator(); iter.hasNext();) {
            DestinationInfo di = (DestinationInfo) iter.next();
            try{
                broker.removeDestination(cs.getContext(), di.getDestination(), 0);
            }catch(Throwable e){
               serviceLog.warn("Failed to remove tmp destination " + di.getDestination(), e);
            }
            iter.remove();
        }
        
        try{
            broker.removeConnection(cs.getContext(), cs.getInfo(), null);
        }catch(Throwable e){
            serviceLog.warn("Failed to remove connection " +  cs.getInfo(),e);
        }
        ConnectionState state = (ConnectionState) localConnectionStates.remove(id);
        if( state != null ) {
            // If we are the last reference, we should remove the state
            // from the broker.
            if( state.getContext().decrementReference() == 0 ){ 
                brokerConnectionStates.remove(id);
            }
        }
        return null;
    }

    
    public Connector getConnector() {
        return connector;
    }

    public void dispatchSync(Command message) {
        processDispatch(message);
    }
    
    
    public void dispatchAsync(Command message) {
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
    }
    
    protected void processDispatch(Command command){
        if(command.isMessageDispatch()){
            MessageDispatch md=(MessageDispatch) command;
            Runnable sub=(Runnable) md.getConsumer();
            broker.processDispatch(md);
            try{
                dispatch(command);
            }finally{
                if(sub!=null){
                    sub.run();
                }
            }
        }else{
            dispatch(command);
        }
    }       
    
    public boolean iterate() {
        if( dispatchQueue.isEmpty() || broker.isStopped()) {
            return false;
        } else {
            Command command = (Command) dispatchQueue.remove(0);
            processDispatch( command );
            return true;
        }
    }    
    
    /**
     * @return true if the Connection is slow
     */
    public boolean isSlow() {
        return false;
    }
    
    /**
     * @return if after being marked, the Connection is still writing
     */
    public boolean isBlocked() {
        return false;
    }
    
    
    /**
     * @return true if the Connection is connected
     */
    public boolean isConnected() {
        return !disposed.get();
    }
    
    /**
     * @return true if the Connection is active
     */
    public boolean isActive() {
        return !disposed.get();
    }
    
    
    
    abstract protected void dispatch(Command command);

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
    
    public boolean isManageable(){
        return manageable;
    }

    
}
