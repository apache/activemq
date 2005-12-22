/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activemq.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.activemq.Service;
import org.activemq.broker.region.ConnectionStatistics;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.BrokerInfo;
import org.activemq.command.Command;
import org.activemq.command.ConnectionId;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.ConsumerId;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.DataArrayResponse;
import org.activemq.command.DestinationInfo;
import org.activemq.command.ExceptionResponse;
import org.activemq.command.FlushCommand;
import org.activemq.command.KeepAliveInfo;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.MessageDispatch;
import org.activemq.command.ProducerId;
import org.activemq.command.ProducerInfo;
import org.activemq.command.RemoveSubscriptionInfo;
import org.activemq.command.Response;
import org.activemq.command.SessionId;
import org.activemq.command.SessionInfo;
import org.activemq.command.ShutdownInfo;
import org.activemq.command.TransactionId;
import org.activemq.command.TransactionInfo;
import org.activemq.command.WireFormatInfo;
import org.activemq.state.CommandVisitor;
import org.activemq.state.ConsumerState;
import org.activemq.state.ProducerState;
import org.activemq.state.SessionState;
import org.activemq.thread.Task;
import org.activemq.thread.TaskRunner;
import org.activemq.thread.TaskRunnerFactory;
import org.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;


/**
 * @version $Revision: 1.26 $
 */
public abstract class AbstractConnection implements Service, Connection, Task, CommandVisitor {

    private static final Log log = LogFactory.getLog(AbstractConnection.class);
    
    protected final Broker broker;
    
    protected final List dispatchQueue = Collections.synchronizedList(new LinkedList());
    protected final TaskRunner taskRunner;
    protected final Connector connector;
    private ConnectionStatistics statistics = new ConnectionStatistics();

    protected final ConcurrentHashMap connectionStates = new ConcurrentHashMap();
    
    private WireFormatInfo wireFormatInfo;    
    protected boolean disposed=false;

    
    static class ConnectionState extends org.activemq.state.ConnectionState {
        private final ConnectionContext context;

        public ConnectionState(ConnectionInfo info, ConnectionContext context) {
            super(info);
            this.context = context;
        }
        
        public ConnectionContext getContext() {
            return context;
        }
    }

    
    /**
     * @param connector
     * @param transport
     * @param broker
     * @param taskRunnerFactory - can be null if you want direct dispatch to the transport else commands are sent async.
     */
    public AbstractConnection(Connector connector, Broker broker, TaskRunnerFactory taskRunnerFactory) {
        
        this.connector = connector;
        this.broker = broker;
        if (connector != null) {
            this.statistics.setParent(connector.getStatistics());
        }
        
        if( taskRunnerFactory != null ) {
            taskRunner = taskRunnerFactory.createTaskRunner( this );
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
        this.dispatch(connector.getBrokerInfo());
    }

    public void stop() throws Exception {
        if( disposed) 
            return;
        
        disposed=true;            
        //
        // Remove all logical connection associated with this connection
        // from the broker.
        ArrayList l = new ArrayList(connectionStates.keySet());
        for (Iterator iter = l.iterator(); iter.hasNext();) {
            ConnectionId connectionId = (ConnectionId) iter.next();
            try {
                processRemoveConnection(connectionId);
            } catch (Throwable ignore) {
            }
        }
    }
    
    public void serviceTransportException(IOException e) {
        if( !disposed ) {
            if( log.isDebugEnabled() )
                log.debug("Transport failed: "+e,e);
            
            log.debug("Transport failed: "+e,e);
            ServiceSupport.dispose(this);
        }
    }
        
    public void serviceException(Throwable e) {
        if( !disposed ) {
            if( log.isDebugEnabled() )
                log.debug("Async error occurred: "+e,e);
            // TODO: think about how to handle this.  Should we send the error down to the client
            // so that he can report it to a registered error listener?
            // Should we terminate the connection?
        } 
    }

    public Response service(Command command) {
        
        Response response=null;
        boolean responseRequired = command.isResponseRequired();
        short commandId = command.getCommandId();
        try {
            response = command.visit(this);
        } catch ( Throwable e ) {
            if( responseRequired ) {
                if( log.isDebugEnabled() )
                    log.debug("Sync error occurred: "+e,e);
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
        ConnectionState cs = (ConnectionState) connectionStates.get(id.getParentId().getParentId());
        if( cs== null )
            throw new IllegalStateException("Cannot lookup a consumer from a connection that had not been registered: "+id.getParentId().getParentId());
        return cs;
    }
    protected ConnectionState lookupConnectionState(ProducerId id) {
        ConnectionState cs = (ConnectionState) connectionStates.get(id.getParentId().getParentId());
        if( cs== null )
            throw new IllegalStateException("Cannot lookup a producer from a connection that had not been registered: "+id.getParentId().getParentId());        
        return cs;
    }
    protected ConnectionState lookupConnectionState(SessionId id) {
        ConnectionState cs = (ConnectionState) connectionStates.get(id.getParentId());
        if( cs== null )
            throw new IllegalStateException("Cannot lookup a session from a connection that had not been registered: "+id.getParentId());        
        return cs;
    }
    protected ConnectionState lookupConnectionState(ConnectionId connectionId) {
        ConnectionState cs = (ConnectionState) connectionStates.get(connectionId);
        if( cs== null )
            throw new IllegalStateException("Cannot lookup a connection that had not been registered: "+connectionId);
        return cs;
    }

    public Response processKeepAlive(KeepAliveInfo info) throws Throwable {
        return null;
    }

    public Response processRemoveSubscription(RemoveSubscriptionInfo info) throws Throwable {
        broker.removeSubscription(lookupConnectionState(info.getConnectionId()).getContext(), info);
        return null;
    }
    
    public Response processWireFormat(WireFormatInfo info) throws Throwable {
        wireFormatInfo = info;
        return null;
    }
    
    public Response processShutdown(ShutdownInfo info) throws Throwable {
        stop();
        return null;
    }
     
    public Response processFlush(FlushCommand command) throws Throwable {
        return null;
    }

    public Response processBeginTransaction(TransactionInfo info) throws Throwable {
        ConnectionState cs = (ConnectionState) connectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        broker.beginTransaction(context, info.getTransactionId());
        return null;
    }
    
    public Response processEndTransaction(TransactionInfo info) throws Throwable {
        // No need to do anything.  This packet is just sent by the client
        // make sure he is synced with the server as commit command could
        // come from a different connection.
        return null;
    }
    
    public Response processPrepareTransaction(TransactionInfo info) throws Throwable {
        ConnectionState cs = (ConnectionState) connectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        broker.prepareTransaction(context, info.getTransactionId());
        return null;
    }

    public Response processCommitTransactionOnePhase(TransactionInfo info) throws Throwable {
        ConnectionState cs = (ConnectionState) connectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        broker.commitTransaction(context, info.getTransactionId(), true);
        return null;
    }

    public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Throwable {
        ConnectionState cs = (ConnectionState) connectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        broker.commitTransaction(context, info.getTransactionId(), false);
        return null;
    }

    public Response processRollbackTransaction(TransactionInfo info) throws Throwable {
        ConnectionState cs = (ConnectionState) connectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        broker.rollbackTransaction(context, info.getTransactionId());
        return null;
    }
    
    public Response processForgetTransaction(TransactionInfo info) throws Throwable {
        ConnectionState cs = (ConnectionState) connectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        broker.forgetTransaction(context, info.getTransactionId());
        return null;
    }
    
    public Response processRecoverTransactions(TransactionInfo info) throws Throwable {
        ConnectionState cs = (ConnectionState) connectionStates.get(info.getConnectionId());
        ConnectionContext context=null;
        if( cs!=null ) {
           context = cs.getContext();
        }
        TransactionId[] preparedTransactions = broker.getPreparedTransactions(context);
        return new DataArrayResponse(preparedTransactions);
    }


    public Response processMessage(Message messageSend) throws Throwable {
        broker.send(lookupConnectionState(messageSend.getProducerId()).getContext(), messageSend);
        return null;
    }

    public Response processMessageAck(MessageAck ack) throws Throwable {
        broker.acknowledge(lookupConnectionState(ack.getConsumerId()).getContext(), ack);
        return null;
    }

    public Response processBrokerInfo(BrokerInfo info) {
        return null;
    }

    public Response processAddDestination(DestinationInfo info) throws Throwable {
        ConnectionState cs = lookupConnectionState(info.getConnectionId());
        broker.addDestination(cs.getContext(), info.getDestination());
        if( info.getDestination().isTemporary() ) {
            cs.addTempDestination(info.getDestination());
        }
        return null;
    }

    public Response processRemoveDestination(DestinationInfo info) throws Throwable {
        ConnectionState cs = lookupConnectionState(info.getConnectionId());
        broker.removeDestination(cs.getContext(), info.getDestination(), info.getTimeout());
        if( info.getDestination().isTemporary() ) {
            cs.removeTempDestination(info.getDestination());
        }
        return null;
    }


    public Response processAddProducer(ProducerInfo info) throws Throwable {
        SessionId sessionId = info.getProducerId().getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        
        ConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if( ss == null )
            throw new IllegalStateException("Cannot add a producer to a session that had not been registered: "+sessionId);
        broker.addProducer(cs.getContext(), info);
        ss.addProducer(info);
        return null;
    }
    
    public Response processRemoveProducer(ProducerId id) throws Throwable {
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

    public Response processAddConsumer(ConsumerInfo info) throws Throwable {
        SessionId sessionId = info.getConsumerId().getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        
        ConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if( ss == null )
            throw new IllegalStateException("Cannot add a consumer to a session that had not been registered: "+sessionId);

        broker.addConsumer(cs.getContext(), info);
        ss.addConsumer(info);
        return null;
    }
    
    public Response processRemoveConsumer(ConsumerId id) throws Throwable {
        
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
    
    public Response processAddSession(SessionInfo info) throws Throwable {
        ConnectionId connectionId = info.getSessionId().getParentId();
        
        ConnectionState cs = lookupConnectionState(connectionId);
        broker.addSession(cs.getContext(), info);
        cs.addSession(info);
        return null;
    }
    
    public Response processRemoveSession(SessionId id) throws Throwable {
        
        ConnectionId connectionId = id.getParentId();
        
        ConnectionState cs = lookupConnectionState(connectionId);
        SessionState session = cs.getSessionState(id);
        if( session == null )
            throw new IllegalStateException("Cannot remove session that had not been registered: "+id);
        
        // Cascade the connection stop to the consumers and producers.
        for (Iterator iter = session.getConsumerIds().iterator(); iter.hasNext();) {
            processRemoveConsumer((ConsumerId) iter.next());
        }
        for (Iterator iter = session.getProducerIds().iterator(); iter.hasNext();) {
            processRemoveProducer((ProducerId) iter.next());
        }
        cs.removeSession(id);
        broker.removeSession(cs.getContext(), session.getInfo());
        return null;
    }
    
    public Response processAddConnection(ConnectionInfo info) throws Throwable {
        // Setup the context.
        ConnectionContext context = new ConnectionContext();
        context.setConnection(this);
        context.setBroker(broker);
        context.setConnector(connector);
        context.setTransactions(new ConcurrentHashMap());
        String clientId = info.getClientId();
        context.setClientId(clientId);
        context.setUserName(info.getUserName());
        context.setConnectionId(info.getConnectionId());
        context.setWireFormatInfo(wireFormatInfo);
        connectionStates.put(info.getConnectionId(), new ConnectionState(info, context));
        
        broker.addConnection(context, info);
        return null;
    }
    
    public Response processRemoveConnection(ConnectionId id) throws Throwable {
        
        ConnectionState cs = lookupConnectionState(id);
        
        // Cascade the connection stop to the sessions.
        for (Iterator iter = cs.getSessionIds().iterator(); iter.hasNext();) {
            processRemoveSession((SessionId) iter.next());
        }
        
        // Cascade the connection stop to temp destinations.
        for (Iterator iter = cs.getTempDesinations().iterator(); iter.hasNext();) {
            broker.removeDestination(cs.getContext(), (ActiveMQDestination) iter.next(), 0);
            iter.remove();
        }
        
        broker.removeConnection(cs.getContext(), cs.getInfo(), null);
        connectionStates.remove(id);
        
        return null;
    }

    
    public Connector getConnector() {
        return connector;
    }

    public void dispatchSync(Command command) {
        
        if( command.isMessageDispatch() ) {
            
            MessageDispatch md = (MessageDispatch) command;
            Runnable sub = (Runnable) md.getConsumer();
            
            try {
                dispatch( command );
            } finally {
                if( sub != null ) {
                    sub.run();
                }
            }
            
        } else {
            dispatch( command );
        }
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
    
    public boolean iterate() {
        if( dispatchQueue.isEmpty() ) {
            return false;
        } else {
            Command command = (Command) dispatchQueue.remove(0);
            dispatch( command );
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
        return !disposed;
    }
    
    /**
     * @return true if the Connection is active
     */
    public boolean isActive() {
        return !disposed;
    }
    
    
    
    abstract protected void dispatch(Command command);

    /**
     * Returns the statistics for this connection
     */
    public ConnectionStatistics getStatistics() {
        return statistics;
    }

}
