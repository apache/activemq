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
package org.activemq.state;

import java.io.IOException;
import java.util.Iterator;

import org.activemq.command.BrokerInfo;
import org.activemq.command.Command;
import org.activemq.command.ConnectionId;
import org.activemq.command.ConnectionInfo;
import org.activemq.command.ConsumerId;
import org.activemq.command.ConsumerInfo;
import org.activemq.command.DestinationInfo;
import org.activemq.command.FlushCommand;
import org.activemq.command.KeepAliveInfo;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.ProducerId;
import org.activemq.command.ProducerInfo;
import org.activemq.command.RemoveSubscriptionInfo;
import org.activemq.command.Response;
import org.activemq.command.SessionId;
import org.activemq.command.SessionInfo;
import org.activemq.command.ShutdownInfo;
import org.activemq.command.TransactionInfo;
import org.activemq.command.WireFormatInfo;
import org.activemq.transport.Transport;
import org.activemq.util.IOExceptionSupport;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the state of a connection so a newly established transport can 
 * be re-initialized to the state that was tracked.
 * 
 * @version $Revision$
 */
public class ConnectionStateTracker implements CommandVisitor {

    private final static Response TRACKED_RESPONSE_MARKER = new Response();
    
    boolean trackTransactions = false;
    boolean trackMessages = false;
    boolean trackAcks = false;
    
    private boolean restoreSessions=true;
    boolean restoreConsumers=true;
    private boolean restoreProducers=true;
    
    protected final ConcurrentHashMap connectionStates = new ConcurrentHashMap();
    
    public boolean track(Command command) throws IOException {
        try {
            return command.visit(this)!=null;
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }   
    
    public void restore( Transport transport ) throws IOException {
        // Restore the connections.
        for (Iterator iter = connectionStates.values().iterator(); iter.hasNext();) {
            ConnectionState connectionState = (ConnectionState) iter.next();
            transport.oneway(connectionState.getInfo());
            restoreTempDestinations(transport, connectionState);
            
            if( restoreSessions )
                restoreSessions(transport, connectionState);
        }
    }

    /**
     * @param transport
     * @param connectionState
     * @throws IOException
     */
    protected void restoreSessions(Transport transport, ConnectionState connectionState) throws IOException {
        // Restore the connection's sessions
        for (Iterator iter2 = connectionState.getSessionStates().iterator(); iter2.hasNext();) {
            SessionState sessionState = (SessionState) iter2.next();
            transport.oneway(sessionState.getInfo());

            if( restoreProducers )
                restoreProducers(transport, sessionState);
            
            if( restoreConsumers )
                restoreConsumers(transport, sessionState);
        }
    }

    /**
     * @param transport
     * @param sessionState
     * @throws IOException
     */
    protected void restoreConsumers(Transport transport, SessionState sessionState) throws IOException {
        // Restore the session's consumers
        for (Iterator iter3 = sessionState.getConsumerStates().iterator(); iter3.hasNext();) {
            ConsumerState consumerState = (ConsumerState) iter3.next();
            transport.oneway(consumerState.getInfo());
        }
    }

    /**
     * @param transport
     * @param sessionState
     * @throws IOException
     */
    protected void restoreProducers(Transport transport, SessionState sessionState) throws IOException {
        // Restore the session's producers
        for (Iterator iter3 = sessionState.getProducerStates().iterator(); iter3.hasNext();) {
            ProducerState producerState = (ProducerState) iter3.next();
            transport.oneway(producerState.getInfo());
        }
    }

    /**
     * @param transport
     * @param connectionState
     * @throws IOException
     */
    protected void restoreTempDestinations(Transport transport, ConnectionState connectionState) throws IOException {
        // Restore the connection's temp destinations.
        for (Iterator iter2 = connectionState.getTempDesinations().iterator(); iter2.hasNext();) {
            transport.oneway((DestinationInfo) iter2.next());
        }
    }

    public Response processAddDestination(DestinationInfo info) throws Throwable {
        ConnectionState cs = (ConnectionState) connectionStates.get(info.getConnectionId());
        if( info.getDestination().isTemporary() ) {
            cs.addTempDestination(info.getDestination());
        }
        return TRACKED_RESPONSE_MARKER;
    }

    public Response processRemoveDestination(DestinationInfo info) throws Throwable {
        ConnectionState cs = (ConnectionState) connectionStates.get(info.getConnectionId());
        if( info.getDestination().isTemporary() ) {
            cs.removeTempDestination(info.getDestination());
        }
        return TRACKED_RESPONSE_MARKER;
    }


    public Response processAddProducer(ProducerInfo info) throws Throwable {
        SessionId sessionId = info.getProducerId().getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        ConnectionState cs = (ConnectionState) connectionStates.get(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        ss.addProducer(info);
        return TRACKED_RESPONSE_MARKER;
    }
    
    public Response processRemoveProducer(ProducerId id) throws Throwable {
        SessionId sessionId = id.getParentId();
        ConnectionId connectionId = sessionId.getParentId();        
        ConnectionState cs = (ConnectionState) connectionStates.get(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        ss.removeProducer(id);        
        return TRACKED_RESPONSE_MARKER;
    }

    public Response processAddConsumer(ConsumerInfo info) throws Throwable {
        SessionId sessionId = info.getConsumerId().getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        ConnectionState cs = (ConnectionState) connectionStates.get(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        ss.addConsumer(info);
        return TRACKED_RESPONSE_MARKER;
    }
    
    public Response processRemoveConsumer(ConsumerId id) throws Throwable {
        SessionId sessionId = id.getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        ConnectionState cs = (ConnectionState) connectionStates.get(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        ss.removeConsumer(id);
        return TRACKED_RESPONSE_MARKER;
    }
    
    public Response processAddSession(SessionInfo info) throws Throwable {
        ConnectionId connectionId = info.getSessionId().getParentId();
        ConnectionState cs = (ConnectionState) connectionStates.get(connectionId);
        cs.addSession(info);
        return TRACKED_RESPONSE_MARKER;
    }
    
    public Response processRemoveSession(SessionId id) throws Throwable {        
        ConnectionId connectionId = id.getParentId();
        ConnectionState cs = (ConnectionState) connectionStates.get(connectionId);
        cs.removeSession(id);
        return TRACKED_RESPONSE_MARKER;
    }
    
    public Response processAddConnection(ConnectionInfo info) throws Throwable {
        connectionStates.put(info.getConnectionId(), new ConnectionState(info));        
        return TRACKED_RESPONSE_MARKER;
    }
    
    public Response processRemoveConnection(ConnectionId id) throws Throwable {
        connectionStates.remove(id);
        return TRACKED_RESPONSE_MARKER;
    }

    public Response processRemoveSubscription(RemoveSubscriptionInfo info) throws Throwable {
        return null;
    }
    public Response processMessage(Message send) throws Throwable {
        return null;
    }
    public Response processMessageAck(MessageAck ack) throws Throwable {
        return null;
    }
    public Response processBeginTransaction(TransactionInfo info) throws Throwable {
        return null;
    }
    public Response processPrepareTransaction(TransactionInfo info) throws Throwable {
        return null;
    }
    public Response processCommitTransactionOnePhase(TransactionInfo info) throws Throwable {
        return null;
    }
    public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Throwable {
        return null;
    }
    public Response processRollbackTransaction(TransactionInfo info) throws Throwable {
        return null;
    }
    public Response processWireFormat(WireFormatInfo info) throws Throwable {
        return null;
    }
    public Response processKeepAlive(KeepAliveInfo info) throws Throwable {
        return null;
    }
    public Response processShutdown(ShutdownInfo info) throws Throwable {
        return null;
    }
    public Response processBrokerInfo(BrokerInfo info) throws Throwable {
        return null;
    }

    public Response processRecoverTransactions(TransactionInfo info) {
        return null;
    }

    public Response processForgetTransaction(TransactionInfo info) throws Throwable {
        return null;
    }

    public Response processEndTransaction(TransactionInfo info) throws Throwable {
        return null;
    }

    public Response processFlush(FlushCommand command) throws Throwable {
        return null;
    }

    public boolean isRestoreConsumers() {
        return restoreConsumers;
    }

    public void setRestoreConsumers(boolean restoreConsumers) {
        this.restoreConsumers = restoreConsumers;
    }

    public boolean isRestoreProducers() {
        return restoreProducers;
    }

    public void setRestoreProducers(boolean restoreProducers) {
        this.restoreProducers = restoreProducers;
    }

    public boolean isRestoreSessions() {
        return restoreSessions;
    }

    public void setRestoreSessions(boolean restoreSessions) {
        this.restoreSessions = restoreSessions;
    }
}
