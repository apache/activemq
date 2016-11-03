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
package org.apache.activemq.store.jdbc;

import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.IndexListener;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;

/**
 *
 */
public class JDBCMessageStore extends AbstractMessageStore {

    class Duration {
        static final int LIMIT = 100;
        final long start = System.currentTimeMillis();
        final String name;

        Duration(String name) {
            this.name = name;
        }
        void end() {
            end(null);
        }
        void end(Object o) {
            long duration = System.currentTimeMillis() - start;

            if (duration > LIMIT) {
                System.err.println(name + " took a long time: " + duration + "ms " + o);
            }
        }
    }
    private static final Logger LOG = LoggerFactory.getLogger(JDBCMessageStore.class);
    protected final WireFormat wireFormat;
    protected final JDBCAdapter adapter;
    protected final JDBCPersistenceAdapter persistenceAdapter;
    protected ActiveMQMessageAudit audit;
    protected final LinkedList<Long> pendingAdditions = new LinkedList<Long>();
    final long[] perPriorityLastRecovered = new long[10];

    public JDBCMessageStore(JDBCPersistenceAdapter persistenceAdapter, JDBCAdapter adapter, WireFormat wireFormat, ActiveMQDestination destination, ActiveMQMessageAudit audit) throws IOException {
        super(destination);
        this.persistenceAdapter = persistenceAdapter;
        this.adapter = adapter;
        this.wireFormat = wireFormat;
        this.audit = audit;

        if (destination.isQueue() && persistenceAdapter.getBrokerService().shouldRecordVirtualDestination(destination)) {
            recordDestinationCreation(destination);
        }
        resetBatching();
    }

    private void recordDestinationCreation(ActiveMQDestination destination) throws IOException {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            if (adapter.doGetLastAckedDurableSubscriberMessageId(c, destination, destination.getQualifiedName(), destination.getQualifiedName()) < 0) {
                adapter.doRecordDestination(c, destination);
            }
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to record destination: " + destination + ". Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    @Override
    public void addMessage(final ConnectionContext context, final Message message) throws IOException {
        MessageId messageId = message.getMessageId();
        if (audit != null && audit.isDuplicate(message)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(destination.getPhysicalName()
                    + " ignoring duplicated (add) message, already stored: "
                    + messageId);
            }
            return;
        }

        // if xaXid present - this is a prepare - so we don't yet have an outcome
        final XATransactionId xaXid =  context != null ? context.getXid() : null;

        // Serialize the Message..
        byte data[];
        try {
            ByteSequence packet = wireFormat.marshal(message);
            data = ByteSequenceData.toByteArray(packet);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to broker message: " + messageId + " in container: " + e, e);
        }

        // Get a connection and insert the message into the DB.
        TransactionContext c = persistenceAdapter.getTransactionContext(context);
        long sequenceId;
        synchronized (pendingAdditions) {
            sequenceId = persistenceAdapter.getNextSequenceId();
            final long sequence = sequenceId;
            message.getMessageId().setEntryLocator(sequence);

            if (xaXid == null) {
                pendingAdditions.add(sequence);

                c.onCompletion(new Runnable() {
                    @Override
                    public void run() {
                        // jdbc close or jms commit - while futureOrSequenceLong==null ordered
                        // work will remain pending on the Queue
                        message.getMessageId().setFutureOrSequenceLong(sequence);
                    }
                });

                if (indexListener != null) {
                    indexListener.onAdd(new IndexListener.MessageContext(context, message, new Runnable() {
                        @Override
                        public void run() {
                            // cursor add complete
                            synchronized (pendingAdditions) { pendingAdditions.remove(sequence); }
                        }
                    }));
                } else {
                    pendingAdditions.remove(sequence);
                }
            }
        }
        try {
            adapter.doAddMessage(c, sequenceId, messageId, destination, data, message.getExpiration(),
                    this.isPrioritizedMessages() ? message.getPriority() : 0, xaXid);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to broker message: " + messageId + " in container: " + e, e);
        } finally {
            c.close();
        }
        if (xaXid == null) {
            onAdd(message, sequenceId, message.getPriority());
        }
    }

    // jdbc commit order is random with concurrent connections - limit scan to lowest pending
    private long minPendingSequeunceId() {
        synchronized (pendingAdditions) {
            if (!pendingAdditions.isEmpty()) {
                return pendingAdditions.get(0);
            } else {
                // nothing pending, ensure scan is limited to current state
                return persistenceAdapter.sequenceGenerator.getLastSequenceId() + 1;
            }
        }
    }

    @Override
    public void updateMessage(Message message) throws IOException {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            adapter.doUpdateMessage(c, destination, message.getMessageId(), ByteSequenceData.toByteArray(wireFormat.marshal(message)));
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to update message: " + message.getMessageId() + " in container: " + e, e);
        } finally {
            c.close();
        }
    }

    protected void onAdd(Message message, long sequenceId, byte priority) {}

    public void addMessageReference(ConnectionContext context, MessageId messageId, long expirationTime, String messageRef) throws IOException {
        // Get a connection and insert the message into the DB.
        TransactionContext c = persistenceAdapter.getTransactionContext(context);
        try {
            adapter.doAddMessageReference(c, persistenceAdapter.getNextSequenceId(), messageId, destination, expirationTime, messageRef);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to broker message: " + messageId + " in container: " + e, e);
        } finally {
            c.close();
        }
    }

    @Override
    public Message getMessage(MessageId messageId) throws IOException {
        // Get a connection and pull the message out of the DB
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            byte data[] = adapter.doGetMessage(c, messageId);
            if (data == null) {
                return null;
            }

            Message answer = (Message)wireFormat.unmarshal(new ByteSequence(data));
            return answer;
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to broker message: " + messageId + " in container: " + e, e);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to broker message: " + messageId + " in container: " + e, e);
        } finally {
            c.close();
        }
    }

    public String getMessageReference(MessageId messageId) throws IOException {
        long id = messageId.getBrokerSequenceId();

        // Get a connection and pull the message out of the DB
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            return adapter.doGetMessageReference(c, id);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to broker message: " + messageId + " in container: " + e, e);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to broker message: " + messageId + " in container: " + e, e);
        } finally {
            c.close();
        }
    }

    @Override
    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {

    	long seq = ack.getLastMessageId().getFutureOrSequenceLong() != null ?
                (Long) ack.getLastMessageId().getFutureOrSequenceLong() :
                persistenceAdapter.getStoreSequenceIdForMessageId(context, ack.getLastMessageId(), destination)[0];

        // Get a connection and remove the message from the DB
        TransactionContext c = persistenceAdapter.getTransactionContext(context);
        try {
            adapter.doRemoveMessage(c, seq, context != null ? context.getXid() : null);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to broker message: " + ack.getLastMessageId() + " in container: " + e, e);
        } finally {
            c.close();
        }
    }

    @Override
    public void recover(final MessageRecoveryListener listener) throws Exception {

        // Get all the Message ids out of the database.
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            adapter.doRecover(c, destination, new JDBCMessageRecoveryListener() {
                @Override
                public boolean recoverMessage(long sequenceId, byte[] data) throws Exception {
                    if (listener.hasSpace()) {
                        Message msg = (Message) wireFormat.unmarshal(new ByteSequence(data));
                        msg.getMessageId().setBrokerSequenceId(sequenceId);
                        return listener.recoverMessage(msg);
                    } else {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Message recovery limit reached for MessageRecoveryListener");
                        }
                        return false;
                    }
                }

                @Override
                public boolean recoverMessageReference(String reference) throws Exception {
                    if (listener.hasSpace()) {
                        return listener.recoverMessageReference(new MessageId(reference));
                    } else {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Message recovery limit reached for MessageRecoveryListener");
                        }
                        return false;
                    }
                }
            });
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to recover container. Reason: " + e, e);
        } finally {
            c.close();
        }
    }

    /**
     * @see org.apache.activemq.store.MessageStore#removeAllMessages(ConnectionContext)
     */
    @Override
    public void removeAllMessages(ConnectionContext context) throws IOException {
        // Get a connection and remove the message from the DB
        TransactionContext c = persistenceAdapter.getTransactionContext(context);
        try {
            adapter.doRemoveAllMessages(c, destination);
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to broker remove all messages: " + e, e);
        } finally {
            c.close();
        }
    }

    @Override
    public int getMessageCount() throws IOException {
        int result = 0;
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {

            result = adapter.doGetMessageCount(c, destination);

        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
            throw IOExceptionSupport.create("Failed to get Message Count: " + destination + ". Reason: " + e, e);
        } finally {
            c.close();
        }
        return result;
    }

    /**
     * @param maxReturned
     * @param listener
     * @throws Exception
     * @see org.apache.activemq.store.MessageStore#recoverNextMessages(int,
     *      org.apache.activemq.store.MessageRecoveryListener)
     */
    @Override
    public void recoverNextMessages(int maxReturned, final MessageRecoveryListener listener) throws Exception {
        TransactionContext c = persistenceAdapter.getTransactionContext();
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace(this + " recoverNext lastRecovered:" + Arrays.toString(perPriorityLastRecovered) + ", minPending:" + minPendingSequeunceId());
            }
            adapter.doRecoverNextMessages(c, destination, perPriorityLastRecovered, minPendingSequeunceId(),
                    maxReturned, isPrioritizedMessages(), new JDBCMessageRecoveryListener() {

                @Override
                public boolean recoverMessage(long sequenceId, byte[] data) throws Exception {
                        Message msg = (Message)wireFormat.unmarshal(new ByteSequence(data));
                        msg.getMessageId().setBrokerSequenceId(sequenceId);
                        msg.getMessageId().setFutureOrSequenceLong(sequenceId);
                        listener.recoverMessage(msg);
                        trackLastRecovered(sequenceId, msg.getPriority());
                        return true;
                }

                @Override
                public boolean recoverMessageReference(String reference) throws Exception {
                    if (listener.hasSpace()) {
                        listener.recoverMessageReference(new MessageId(reference));
                        return true;
                    }
                    return false;
                }

            });
        } catch (SQLException e) {
            JDBCPersistenceAdapter.log("JDBC Failure: ", e);
        } finally {
            c.close();
        }

    }

    private void trackLastRecovered(long sequenceId, int priority) {
        perPriorityLastRecovered[isPrioritizedMessages() ? priority : 0] = sequenceId;
    }

    /**
     * @see org.apache.activemq.store.MessageStore#resetBatching()
     */
    @Override
    public void resetBatching() {
        if (LOG.isTraceEnabled()) {
            LOG.trace(this + " resetBatching. last recovered: " + Arrays.toString(perPriorityLastRecovered));
        }
        setLastRecovered(-1);
    }

    private void setLastRecovered(long val) {
        for (int i=0;i<perPriorityLastRecovered.length;i++) {
            perPriorityLastRecovered[i] = val;
        }
    }


    @Override
    public void setBatch(MessageId messageId) {
        if (LOG.isTraceEnabled()) {
            LOG.trace(this + " setBatch: last recovered: " + Arrays.toString(perPriorityLastRecovered));
        }
        try {
            long[] storedValues = persistenceAdapter.getStoreSequenceIdForMessageId(null, messageId, destination);
            setLastRecovered(storedValues[0]);
        } catch (IOException ignoredAsAlreadyLogged) {
            resetBatching();
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace(this + " setBatch: new last recovered: " + Arrays.toString(perPriorityLastRecovered));
        }
    }


    @Override
    public void setPrioritizedMessages(boolean prioritizedMessages) {
        super.setPrioritizedMessages(prioritizedMessages);
    }

    @Override
    public String toString() {
        return destination.getPhysicalName() + ",pendingSize:" + pendingAdditions.size();
    }

}
