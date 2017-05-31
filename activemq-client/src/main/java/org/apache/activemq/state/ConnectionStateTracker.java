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
package org.apache.activemq.state;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.jms.TransactionRolledBackException;
import javax.transaction.xa.XAResource;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.IntegerResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the state of a connection so a newly established transport can be
 * re-initialized to the state that was tracked.
 *
 *
 */
public class ConnectionStateTracker extends CommandVisitorAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionStateTracker.class);

    private static final Tracked TRACKED_RESPONSE_MARKER = new Tracked(null);
    private static final int MESSAGE_PULL_SIZE = 400;
    protected final ConcurrentMap<ConnectionId, ConnectionState> connectionStates = new ConcurrentHashMap<>();

    private boolean trackTransactions;
    private boolean restoreSessions = true;
    private boolean restoreConsumers = true;
    private boolean restoreProducers = true;
    private boolean restoreTransaction = true;
    private boolean trackMessages = true;
    private boolean trackTransactionProducers = true;
    private int maxCacheSize = 128 * 1024;
    private long currentCacheSize;  // use long to prevent overflow for folks who set high max.

    private final Map<Object,Command> messageCache = new LinkedHashMap<Object,Command>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<Object,Command> eldest) {
            boolean result = currentCacheSize > maxCacheSize;
            if (result) {
                if (eldest.getValue() instanceof Message) {
                    currentCacheSize -= ((Message)eldest.getValue()).getSize();
                } else if (eldest.getValue() instanceof MessagePull) {
                    currentCacheSize -= MESSAGE_PULL_SIZE;
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("removing tracked message: " + eldest.getKey());
                }
            }
            return result;
        }
    };

    private class RemoveTransactionAction implements ResponseHandler {
        private final TransactionInfo info;

        public RemoveTransactionAction(TransactionInfo info) {
            this.info = info;
        }

        @Override
        public void onResponse(Command response) {
            ConnectionId connectionId = info.getConnectionId();
            ConnectionState cs = connectionStates.get(connectionId);
            if (cs != null) {
                cs.removeTransactionState(info.getTransactionId());
            }
        }
    }

    private final class ExceptionResponseCheckAction implements ResponseHandler {
        private final Command tracked;

        public ExceptionResponseCheckAction(Command tracked) {
            this.tracked = tracked;
        }

        @Override
        public void onResponse(Command response) {
            if (ExceptionResponse.DATA_STRUCTURE_TYPE == response.getDataStructureType()) {
                if (tracked.getDataStructureType() == ConsumerInfo.DATA_STRUCTURE_TYPE) {
                    processRemoveConsumer(((ConsumerInfo) tracked).getConsumerId(), 0l);
                } else if (tracked.getDataStructureType() == ProducerInfo.DATA_STRUCTURE_TYPE) {
                    processRemoveProducer(((ProducerInfo) tracked).getProducerId());
                }
            }
        }
    }

    private class PrepareReadonlyTransactionAction extends RemoveTransactionAction {
        public PrepareReadonlyTransactionAction(TransactionInfo info) {
            super(info);
        }

        @Override
        public void onResponse(Command command) {
            if (command instanceof IntegerResponse) {
                IntegerResponse response = (IntegerResponse) command;
                if (XAResource.XA_RDONLY == response.getResult()) {
                    // all done, no commit or rollback from TM
                    super.onResponse(command);
                }
            }
        }
    }

    /**
     * Entry point for all tracked commands in the tracker.  Commands should be tracked before
     * there is an attempt to send them on the wire.  Upon a successful send of a command it is
     * necessary to call the trackBack method to complete the tracking of the given command.
     *
     * @param command
     *      The command that is to be tracked by this tracker.
     *
     * @return null if the command is not state tracked.
     *
     * @throws IOException if an error occurs during setup of the tracking operation.
     */
    public Tracked track(Command command) throws IOException {
        try {
            return (Tracked)command.visit(this);
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }

    /**
     * Completes the two phase tracking operation for a command that is sent on the wire.  Once
     * the command is sent successfully to complete the tracking operation or otherwise update
     * the state of the tracker.
     *
     * @param command
     *      The command that was previously provided to the track method.
     */
    public void trackBack(Command command) {
        if (command != null) {
            if (trackMessages && command.isMessage()) {
                Message message = (Message) command;
                if (message.getTransactionId()==null) {
                    currentCacheSize = currentCacheSize +  message.getSize();
                }
            } else if (command instanceof MessagePull) {
                // We only track one MessagePull per consumer so only add to cache size
                // when the command has been marked as tracked.
                if (((MessagePull)command).isTracked()) {
                    // just needs to be a rough estimate of size, ~4 identifiers
                    currentCacheSize += MESSAGE_PULL_SIZE;
                }
            }
        }
    }

    public void restore(Transport transport) throws IOException {
        // Restore the connections.
        for (Iterator<ConnectionState> iter = connectionStates.values().iterator(); iter.hasNext();) {
            ConnectionState connectionState = iter.next();
            connectionState.getInfo().setFailoverReconnect(true);
            if (LOG.isDebugEnabled()) {
                LOG.debug("conn: " + connectionState.getInfo().getConnectionId());
            }
            transport.oneway(connectionState.getInfo());
            restoreTempDestinations(transport, connectionState);

            if (restoreSessions) {
                restoreSessions(transport, connectionState);
            }

            if (restoreTransaction) {
                restoreTransactions(transport, connectionState);
            }
        }

        // now flush messages and MessagePull commands.
        for (Command msg : messageCache.values()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("command: " + (msg.isMessage() ? ((Message) msg).getMessageId() : msg));
            }
            transport.oneway(msg);
        }
    }

    private void restoreTransactions(Transport transport, ConnectionState connectionState) throws IOException {
        Vector<TransactionInfo> toRollback = new Vector<>();
        for (TransactionState transactionState : connectionState.getTransactionStates()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("tx: " + transactionState.getId());
            }

            // rollback any completed transactions - no way to know if commit got there
            // or if reply went missing
            //
            if (!transactionState.getCommands().isEmpty()) {
                Command lastCommand = transactionState.getCommands().get(transactionState.getCommands().size() - 1);
                if (lastCommand instanceof TransactionInfo) {
                    TransactionInfo transactionInfo = (TransactionInfo) lastCommand;
                    if (transactionInfo.getType() == TransactionInfo.COMMIT_ONE_PHASE) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("rolling back potentially completed tx: " + transactionState.getId());
                        }
                        toRollback.add(transactionInfo);
                        continue;
                    }
                }
            }

            // replay short lived producers that may have been involved in the transaction
            for (ProducerState producerState : transactionState.getProducerStates().values()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("tx replay producer :" + producerState.getInfo());
                }
                transport.oneway(producerState.getInfo());
            }

            for (Command command : transactionState.getCommands()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("tx replay: " + command);
                }
                transport.oneway(command);
            }

            for (ProducerState producerState : transactionState.getProducerStates().values()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("tx remove replayed producer :" + producerState.getInfo());
                }
                transport.oneway(producerState.getInfo().createRemoveCommand());
            }
        }

        for (TransactionInfo command: toRollback) {
            // respond to the outstanding commit
            ExceptionResponse response = new ExceptionResponse();
            response.setException(new TransactionRolledBackException("Transaction completion in doubt due to failover. Forcing rollback of " + command.getTransactionId()));
            response.setCorrelationId(command.getCommandId());
            transport.getTransportListener().onCommand(response);
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
            SessionState sessionState = (SessionState)iter2.next();
            if (LOG.isDebugEnabled()) {
                LOG.debug("session: " + sessionState.getInfo().getSessionId());
            }
            transport.oneway(sessionState.getInfo());

            if (restoreProducers) {
                restoreProducers(transport, sessionState);
            }

            if (restoreConsumers) {
                restoreConsumers(transport, sessionState);
            }
        }
    }

    /**
     * @param transport
     * @param sessionState
     * @throws IOException
     */
    protected void restoreConsumers(Transport transport, SessionState sessionState) throws IOException {
        // Restore the session's consumers but possibly in pull only (prefetch 0 state) till recovery complete
        final ConnectionState connectionState = connectionStates.get(sessionState.getInfo().getSessionId().getParentId());
        final boolean connectionInterruptionProcessingComplete = connectionState.isConnectionInterruptProcessingComplete();
        for (ConsumerState consumerState : sessionState.getConsumerStates()) {
            ConsumerInfo infoToSend = consumerState.getInfo();
            if (!connectionInterruptionProcessingComplete && infoToSend.getPrefetchSize() > 0) {
                infoToSend = consumerState.getInfo().copy();
                connectionState.getRecoveringPullConsumers().put(infoToSend.getConsumerId(), consumerState.getInfo());
                infoToSend.setPrefetchSize(0);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("restore consumer: " + infoToSend.getConsumerId() + " in pull mode pending recovery, overriding prefetch: " + consumerState.getInfo().getPrefetchSize());
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("consumer: " + infoToSend.getConsumerId());
            }
            transport.oneway(infoToSend);
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
            ProducerState producerState = (ProducerState)iter3.next();
            if (LOG.isDebugEnabled()) {
                LOG.debug("producer: " + producerState.getInfo().getProducerId());
            }
            transport.oneway(producerState.getInfo());
        }
    }

    /**
     * @param transport
     * @param connectionState
     * @throws IOException
     */
    protected void restoreTempDestinations(Transport transport, ConnectionState connectionState)
        throws IOException {
        // Restore the connection's temp destinations.
        for (Iterator iter2 = connectionState.getTempDestinations().iterator(); iter2.hasNext();) {
            DestinationInfo info = (DestinationInfo)iter2.next();
            transport.oneway(info);
            if (LOG.isDebugEnabled()) {
                LOG.debug("tempDest: " + info.getDestination());
            }
        }
    }

    @Override
    public Response processAddDestination(DestinationInfo info) {
        if (info != null) {
            ConnectionState cs = connectionStates.get(info.getConnectionId());
            if (cs != null && info.getDestination().isTemporary()) {
                cs.addTempDestination(info);
            }
        }
        return TRACKED_RESPONSE_MARKER;
    }

    @Override
    public Response processRemoveDestination(DestinationInfo info) {
        if (info != null) {
            ConnectionState cs = connectionStates.get(info.getConnectionId());
            if (cs != null && info.getDestination().isTemporary()) {
                cs.removeTempDestination(info.getDestination());
            }
        }
        return TRACKED_RESPONSE_MARKER;
    }

    @Override
    public Response processAddProducer(ProducerInfo info) {
        if (info != null && info.getProducerId() != null) {
            SessionId sessionId = info.getProducerId().getParentId();
            if (sessionId != null) {
                ConnectionId connectionId = sessionId.getParentId();
                if (connectionId != null) {
                    ConnectionState cs = connectionStates.get(connectionId);
                    if (cs != null) {
                        SessionState ss = cs.getSessionState(sessionId);
                        if (ss != null) {
                            ss.addProducer(info);
                            if (info.isResponseRequired()) {
                                return new Tracked(new ExceptionResponseCheckAction(info));
                            }
                        }
                    }
                }
            }
        }
        return TRACKED_RESPONSE_MARKER;
    }

    @Override
    public Response processRemoveProducer(ProducerId id) {
        if (id != null) {
            SessionId sessionId = id.getParentId();
            if (sessionId != null) {
                ConnectionId connectionId = sessionId.getParentId();
                if (connectionId != null) {
                    ConnectionState cs = connectionStates.get(connectionId);
                    if (cs != null) {
                        SessionState ss = cs.getSessionState(sessionId);
                        if (ss != null) {
                            ss.removeProducer(id);
                        }
                    }
                }
            }
        }
        return TRACKED_RESPONSE_MARKER;
    }

    @Override
    public Response processAddConsumer(ConsumerInfo info) {
        if (info != null) {
            SessionId sessionId = info.getConsumerId().getParentId();
            if (sessionId != null) {
                ConnectionId connectionId = sessionId.getParentId();
                if (connectionId != null) {
                    ConnectionState cs = connectionStates.get(connectionId);
                    if (cs != null) {
                        SessionState ss = cs.getSessionState(sessionId);
                        if (ss != null) {
                            ss.addConsumer(info);
                            if (info.isResponseRequired()) {
                                return new Tracked(new ExceptionResponseCheckAction(info));
                            }
                        }
                    }
                }
            }
        }
        return TRACKED_RESPONSE_MARKER;
    }

    @Override
    public Response processRemoveConsumer(ConsumerId id, long lastDeliveredSequenceId) {
        if (id != null) {
            SessionId sessionId = id.getParentId();
            if (sessionId != null) {
                ConnectionId connectionId = sessionId.getParentId();
                if (connectionId != null) {
                    ConnectionState cs = connectionStates.get(connectionId);
                    if (cs != null) {
                        SessionState ss = cs.getSessionState(sessionId);
                        if (ss != null) {
                            ss.removeConsumer(id);
                        }
                        cs.getRecoveringPullConsumers().remove(id);
                    }
                }
            }
        }
        return TRACKED_RESPONSE_MARKER;
    }

    @Override
    public Response processAddSession(SessionInfo info) {
        if (info != null) {
            ConnectionId connectionId = info.getSessionId().getParentId();
            if (connectionId != null) {
                ConnectionState cs = connectionStates.get(connectionId);
                if (cs != null) {
                    cs.addSession(info);
                }
            }
        }
        return TRACKED_RESPONSE_MARKER;
    }

    @Override
    public Response processRemoveSession(SessionId id, long lastDeliveredSequenceId) {
        if (id != null) {
            ConnectionId connectionId = id.getParentId();
            if (connectionId != null) {
                ConnectionState cs = connectionStates.get(connectionId);
                if (cs != null) {
                    cs.removeSession(id);
                }
            }
        }
        return TRACKED_RESPONSE_MARKER;
    }

    @Override
    public Response processAddConnection(ConnectionInfo info) {
        if (info != null) {
            connectionStates.put(info.getConnectionId(), new ConnectionState(info));
        }
        return TRACKED_RESPONSE_MARKER;
    }

    @Override
    public Response processRemoveConnection(ConnectionId id, long lastDeliveredSequenceId) throws Exception {
        if (id != null) {
            connectionStates.remove(id);
        }
        return TRACKED_RESPONSE_MARKER;
    }

    @Override
    public Response processMessage(Message send) throws Exception {
        if (send != null) {
            if (trackTransactions && send.getTransactionId() != null) {
                ProducerId producerId = send.getProducerId();
                ConnectionId connectionId = producerId.getParentId().getParentId();
                if (connectionId != null) {
                    ConnectionState cs = connectionStates.get(connectionId);
                    if (cs != null) {
                        TransactionState transactionState = cs.getTransactionState(send.getTransactionId());
                        if (transactionState != null) {
                            transactionState.addCommand(send);

                            if (trackTransactionProducers) {
                                // for jmstemplate, track the producer in case it is closed before commit
                                // and needs to be replayed
                                SessionState ss = cs.getSessionState(producerId.getParentId());
                                ProducerState producerState = ss.getProducerState(producerId);
                                producerState.setTransactionState(transactionState);
                            }
                        }
                    }
                }
                return TRACKED_RESPONSE_MARKER;
            }else if (trackMessages) {
                messageCache.put(send.getMessageId(), send);
            }
        }
        return null;
    }

    @Override
    public Response processBeginTransaction(TransactionInfo info) {
        if (trackTransactions && info != null && info.getTransactionId() != null) {
            ConnectionId connectionId = info.getConnectionId();
            if (connectionId != null) {
                ConnectionState cs = connectionStates.get(connectionId);
                if (cs != null) {
                    cs.addTransactionState(info.getTransactionId());
                    TransactionState state = cs.getTransactionState(info.getTransactionId());
                    state.addCommand(info);
                }
            }
            return TRACKED_RESPONSE_MARKER;
        }
        return null;
    }

    @Override
    public Response processPrepareTransaction(TransactionInfo info) throws Exception {
        if (trackTransactions && info != null && info.getTransactionId() != null) {
            ConnectionId connectionId = info.getConnectionId();
            if (connectionId != null) {
                ConnectionState cs = connectionStates.get(connectionId);
                if (cs != null) {
                    TransactionState transactionState = cs.getTransactionState(info.getTransactionId());
                    if (transactionState != null) {
                        transactionState.addCommand(info);
                        return new Tracked(new PrepareReadonlyTransactionAction(info));
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Response processCommitTransactionOnePhase(TransactionInfo info) throws Exception {
        if (trackTransactions && info != null && info.getTransactionId() != null) {
            ConnectionId connectionId = info.getConnectionId();
            if (connectionId != null) {
                ConnectionState cs = connectionStates.get(connectionId);
                if (cs != null) {
                    TransactionState transactionState = cs.getTransactionState(info.getTransactionId());
                    if (transactionState != null) {
                        transactionState.addCommand(info);
                        return new Tracked(new RemoveTransactionAction(info));
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Exception {
        if (trackTransactions && info != null && info.getTransactionId() != null) {
            ConnectionId connectionId = info.getConnectionId();
            if (connectionId != null) {
                ConnectionState cs = connectionStates.get(connectionId);
                if (cs != null) {
                    TransactionState transactionState = cs.getTransactionState(info.getTransactionId());
                    if (transactionState != null) {
                        transactionState.addCommand(info);
                        return new Tracked(new RemoveTransactionAction(info));
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Response processRollbackTransaction(TransactionInfo info) throws Exception {
        if (trackTransactions && info != null && info.getTransactionId() != null) {
            ConnectionId connectionId = info.getConnectionId();
            if (connectionId != null) {
                ConnectionState cs = connectionStates.get(connectionId);
                if (cs != null) {
                    TransactionState transactionState = cs.getTransactionState(info.getTransactionId());
                    if (transactionState != null) {
                        transactionState.addCommand(info);
                        return new Tracked(new RemoveTransactionAction(info));
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Response processEndTransaction(TransactionInfo info) throws Exception {
        if (trackTransactions && info != null && info.getTransactionId() != null) {
            ConnectionId connectionId = info.getConnectionId();
            if (connectionId != null) {
                ConnectionState cs = connectionStates.get(connectionId);
                if (cs != null) {
                    TransactionState transactionState = cs.getTransactionState(info.getTransactionId());
                    if (transactionState != null) {
                        transactionState.addCommand(info);
                    }
                }
            }
            return TRACKED_RESPONSE_MARKER;
        }
        return null;
    }

    @Override
    public Response processMessagePull(MessagePull pull) throws Exception {
        if (pull != null) {
            // leave a single instance in the cache
            final String id = pull.getDestination() + "::" + pull.getConsumerId();
            if (messageCache.put(id.intern(), pull) == null) {
                // Only marked as tracked if this is the first request we've seen.
                pull.setTracked(true);
            }
        }
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

    public boolean isTrackTransactions() {
        return trackTransactions;
    }

    public void setTrackTransactions(boolean trackTransactions) {
        this.trackTransactions = trackTransactions;
    }

    public boolean isTrackTransactionProducers() {
        return this.trackTransactionProducers;
    }

    public void setTrackTransactionProducers(boolean trackTransactionProducers) {
        this.trackTransactionProducers = trackTransactionProducers;
    }

    public boolean isRestoreTransaction() {
        return restoreTransaction;
    }

    public void setRestoreTransaction(boolean restoreTransaction) {
        this.restoreTransaction = restoreTransaction;
    }

    public boolean isTrackMessages() {
        return trackMessages;
    }

    public void setTrackMessages(boolean trackMessages) {
        this.trackMessages = trackMessages;
    }

    public int getMaxCacheSize() {
        return maxCacheSize;
    }

    public void setMaxCacheSize(int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    /**
     * @return the current cache size for the Message and MessagePull Command cache.
     */
    public long getCurrentCacheSize() {
        return this.currentCacheSize;
    }

    public void connectionInterruptProcessingComplete(Transport transport, ConnectionId connectionId) {
        ConnectionState connectionState = connectionStates.get(connectionId);
        if (connectionState != null) {
            connectionState.setConnectionInterruptProcessingComplete(true);
            Map<ConsumerId, ConsumerInfo> stalledConsumers = connectionState.getRecoveringPullConsumers();
            for (Entry<ConsumerId, ConsumerInfo> entry: stalledConsumers.entrySet()) {
                ConsumerControl control = new ConsumerControl();
                control.setConsumerId(entry.getKey());
                control.setPrefetch(entry.getValue().getPrefetchSize());
                control.setDestination(entry.getValue().getDestination());
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("restored recovering consumer: " + control.getConsumerId() + " with: " + control.getPrefetch());
                    }
                    transport.oneway(control);
                } catch (Exception ex) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Failed to submit control for consumer: " + control.getConsumerId()
                                + " with: " + control.getPrefetch(), ex);
                    }
                }
            }
            stalledConsumers.clear();
        }
    }

    public void transportInterrupted(ConnectionId connectionId) {
        ConnectionState connectionState = connectionStates.get(connectionId);
        if (connectionState != null) {
            connectionState.setConnectionInterruptProcessingComplete(false);
        }
    }
}
