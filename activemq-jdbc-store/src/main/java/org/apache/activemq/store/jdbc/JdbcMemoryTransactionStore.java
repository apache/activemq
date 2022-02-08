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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.IndexListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.memory.MemoryTransactionStore;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.DataByteArrayInputStream;

/**
 * respect 2pc prepare
 * uses local transactions to maintain prepared state
 * xid column provides transaction flag for additions and removals
 * a commit clears that context and completes the work
 * a rollback clears the flag and removes the additions
 * Essentially a prepare is an insert &| update transaction
 *  commit|rollback is an update &| remove
 */
public class JdbcMemoryTransactionStore extends MemoryTransactionStore {


    public JdbcMemoryTransactionStore(JDBCPersistenceAdapter jdbcPersistenceAdapter) {
        super(jdbcPersistenceAdapter);
    }

    @Override
    public void prepare(TransactionId txid) throws IOException {
        Tx tx = inflightTransactions.remove(txid);
        if (tx == null) {
            return;
        }

        ConnectionContext ctx = new ConnectionContext();
        // setting the xid modifies the add/remove to be pending transaction outcome
        ctx.setXid((XATransactionId) txid);
        persistenceAdapter.beginTransaction(ctx);
        try {

            // Do all the message adds.
            for (AddMessageCommand cmd : tx.messages) {
                cmd.run(ctx);
            }
            // And removes..
            for (RemoveMessageCommand cmd : tx.acks) {
                cmd.run(ctx);
            }

            persistenceAdapter.commitTransaction(ctx);

        } catch ( IOException e ) {
            persistenceAdapter.rollbackTransaction(ctx);
            throw e;
        }

        ctx.setXid(null);
        // setup for commit outcome
        ArrayList<AddMessageCommand> updateFromPreparedStateCommands = new ArrayList<>();
        for (final AddMessageCommand addMessageCommand : tx.messages) {
            updateFromPreparedStateCommands.add(new CommitAddOutcome(addMessageCommand));
        }
        tx.messages = updateFromPreparedStateCommands;
        preparedTransactions.put(txid, tx);

    }


    class CommitAddOutcome implements AddMessageCommand {
        final Message message;
        JDBCMessageStore jdbcMessageStore;

        public CommitAddOutcome(JDBCMessageStore jdbcMessageStore, Message message) {
            this.jdbcMessageStore = jdbcMessageStore;
            this.message = message;
        }

        public CommitAddOutcome(AddMessageCommand addMessageCommand) {
            this((JDBCMessageStore)addMessageCommand.getMessageStore(), addMessageCommand.getMessage());
        }

        @Override
        public Message getMessage() {
            return message;
        }

        @Override
        public MessageStore getMessageStore() {
            return jdbcMessageStore;
        }

        @Override
        public void run(final ConnectionContext context) throws IOException {
            JDBCPersistenceAdapter jdbcPersistenceAdapter = (JDBCPersistenceAdapter) persistenceAdapter;
            final Long preparedEntrySequence = (Long) message.getMessageId().getEntryLocator();
            TransactionContext c = jdbcPersistenceAdapter.getTransactionContext(context);

            long newSequence;
            synchronized (jdbcMessageStore.pendingAdditions) {
                newSequence = jdbcPersistenceAdapter.getNextSequenceId();
                final long sequenceToSet = newSequence;
                c.onCompletion(new Runnable() {
                    @Override
                    public void run() {
                        message.getMessageId().setEntryLocator(sequenceToSet);
                        message.getMessageId().setFutureOrSequenceLong(sequenceToSet);
                    }
                });

                if (jdbcMessageStore.getIndexListener() != null) {
                    jdbcMessageStore.getIndexListener().onAdd(new IndexListener.MessageContext(context, message, null));
                }
            }

            jdbcPersistenceAdapter.commitAdd(context, message.getMessageId(), preparedEntrySequence, newSequence);
            jdbcMessageStore.onAdd(message, (Long)message.getMessageId().getEntryLocator(), message.getPriority());
        }

        @Override
        public void setMessageStore(MessageStore messageStore) {
            jdbcMessageStore = (JDBCMessageStore) messageStore;
        }
    }

    @Override
    public void rollback(TransactionId txid) throws IOException {

        Tx tx = inflightTransactions.remove(txid);
        if (tx == null) {
            tx = preparedTransactions.remove(txid);
            if (tx != null) {
                // undo prepare work
                ConnectionContext ctx = new ConnectionContext();
                persistenceAdapter.beginTransaction(ctx);
                try {

                    for (AddMessageCommand addMessageCommand : tx.messages) {
                        final Message message = addMessageCommand.getMessage();
                        // need to delete the row
                        ((JDBCPersistenceAdapter) persistenceAdapter).commitRemove(ctx, new MessageAck(message, MessageAck.STANDARD_ACK_TYPE, 1));
                    }

                    for (RemoveMessageCommand removeMessageCommand : tx.acks) {
                        if (removeMessageCommand instanceof LastAckCommand) {
                            ((LastAckCommand) removeMessageCommand).rollback(ctx);
                        } else {
                            MessageId messageId = removeMessageCommand.getMessageAck().getLastMessageId();
                            long sequence = (Long) messageId.getEntryLocator();
                            // need to unset the txid flag on the existing row
                            ((JDBCPersistenceAdapter) persistenceAdapter).commitAdd(ctx, messageId, sequence, sequence);

                            if (removeMessageCommand instanceof RecoveredRemoveMessageCommand) {
                                ((JDBCMessageStore) removeMessageCommand.getMessageStore()).trackRollbackAck(((RecoveredRemoveMessageCommand) removeMessageCommand).getMessage());
                            }
                        }
                    }
                } catch (IOException e) {
                    persistenceAdapter.rollbackTransaction(ctx);
                    throw e;
                }
                persistenceAdapter.commitTransaction(ctx);
            }
        }
    }

    @Override
    public synchronized void recover(TransactionRecoveryListener listener) throws IOException {
        ((JDBCPersistenceAdapter)persistenceAdapter).recover(this);
        super.recover(listener);
    }

    public void recoverAdd(long id, byte[] messageBytes) throws IOException {
        final Message message = (Message) ((JDBCPersistenceAdapter)persistenceAdapter).getWireFormat().unmarshal(new ByteSequence(messageBytes));
        message.getMessageId().setFutureOrSequenceLong(id);
        message.getMessageId().setEntryLocator(id);
        Tx tx = getPreparedTx(message.getTransactionId());
        tx.add(new CommitAddOutcome(null, message));
    }

    public void recoverAck(long id, byte[] xid, byte[] message) throws IOException {
        final Message msg = (Message) ((JDBCPersistenceAdapter)persistenceAdapter).getWireFormat().unmarshal(new ByteSequence(message));
        msg.getMessageId().setFutureOrSequenceLong(id);
        msg.getMessageId().setEntryLocator(id);
        Tx tx = getPreparedTx(new XATransactionId(xid));
        final MessageAck ack = new MessageAck(msg, MessageAck.STANDARD_ACK_TYPE, 1);
        tx.add(new RecoveredRemoveMessageCommand() {
            MessageStore messageStore = null;
            @Override
            public MessageAck getMessageAck() {
                return ack;
            }

            @Override
            public void run(ConnectionContext context) throws IOException {
                ((JDBCPersistenceAdapter)persistenceAdapter).commitRemove(context, ack);
            }

            public Message getMessage() {
                return msg;
            }

            @Override
            public void setMessageStore(MessageStore messageStore) {
                this.messageStore = messageStore;
            }

            @Override
            public MessageStore getMessageStore() {
                return messageStore;
            }

        });
    }

    interface RecoveredRemoveMessageCommand extends RemoveMessageCommand {
        Message getMessage();

        void setMessageStore(MessageStore messageStore);
    }

    interface LastAckCommand extends RemoveMessageCommand {
        void rollback(ConnectionContext context) throws IOException;

        String getClientId();

        String getSubName();

        long getSequence();

        byte getPriority();

        void setMessageStore(JDBCTopicMessageStore jdbcTopicMessageStore);
    }

    public void recoverLastAck(byte[] encodedXid, final ActiveMQDestination destination, final String subName, final String clientId) throws IOException {
        Tx tx = getPreparedTx(new XATransactionId(encodedXid));
        DataByteArrayInputStream inputStream = new DataByteArrayInputStream(encodedXid);
        inputStream.skipBytes(1); // +|-
        final long lastAck = inputStream.readLong();
        final byte priority = inputStream.readByte();
        final MessageAck ack = new MessageAck();
        ack.setDestination(destination);
        tx.add(new LastAckCommand() {
            JDBCTopicMessageStore jdbcTopicMessageStore;

            @Override
            public MessageAck getMessageAck() {
                return ack;
            }

            @Override
            public MessageStore getMessageStore() {
                return jdbcTopicMessageStore;
            }

            @Override
            public void run(ConnectionContext context) throws IOException {
                ((JDBCPersistenceAdapter)persistenceAdapter).commitLastAck(context, lastAck, priority, destination, subName, clientId);
                jdbcTopicMessageStore.complete(clientId, subName);
            }

            @Override
            public void rollback(ConnectionContext context) throws IOException {
                ((JDBCPersistenceAdapter)persistenceAdapter).rollbackLastAck(context, jdbcTopicMessageStore.isPrioritizedMessages() ? priority : 0, jdbcTopicMessageStore.getDestination(), subName, clientId);
                jdbcTopicMessageStore.complete(clientId, subName);
            }

            @Override
            public String getClientId() {
                return clientId;
            }

            @Override
            public String getSubName() {
                return subName;
            }

            @Override
            public long getSequence() {
                return lastAck;
            }

            @Override
            public byte getPriority() {
                return priority;
            }

            @Override
            public void setMessageStore(JDBCTopicMessageStore jdbcTopicMessageStore) {
                this.jdbcTopicMessageStore = jdbcTopicMessageStore;
            }
        });

    }

    @Override
    protected void onRecovered(Tx tx) {
        for (RemoveMessageCommand removeMessageCommand: tx.acks) {
            if (removeMessageCommand instanceof LastAckCommand) {
                LastAckCommand lastAckCommand = (LastAckCommand) removeMessageCommand;
                JDBCTopicMessageStore jdbcTopicMessageStore = (JDBCTopicMessageStore) findMessageStore(lastAckCommand.getMessageAck().getDestination());
                jdbcTopicMessageStore.pendingCompletion(lastAckCommand.getClientId(), lastAckCommand.getSubName(), lastAckCommand.getSequence(), lastAckCommand.getPriority());
                lastAckCommand.setMessageStore(jdbcTopicMessageStore);
            } else {
                ((RecoveredRemoveMessageCommand)removeMessageCommand).setMessageStore(findMessageStore(removeMessageCommand.getMessageAck().getDestination()));
            }
        }
        for (AddMessageCommand addMessageCommand : tx.messages) {
            addMessageCommand.setMessageStore(findMessageStore(addMessageCommand.getMessage().getDestination()));
        }
    }

    private MessageStore findMessageStore(ActiveMQDestination destination) {
        ProxyMessageStore proxyMessageStore = null;
        try {
            if (destination.isQueue()) {
                proxyMessageStore = (ProxyMessageStore) persistenceAdapter.createQueueMessageStore((ActiveMQQueue) destination);
            } else {
                proxyMessageStore = (ProxyMessageStore) persistenceAdapter.createTopicMessageStore((ActiveMQTopic) destination);
            }
        } catch (IOException error) {
            throw new RuntimeException("Failed to find/create message store for destination: " + destination, error);
        }
        return proxyMessageStore.getDelegate();
    }

    @Override
    public void acknowledge(final TopicMessageStore topicMessageStore, final String clientId, final String subscriptionName,
                           final MessageId messageId, final MessageAck ack) throws IOException {

        if (ack.isInTransaction()) {
            Tx tx = getTx(ack.getTransactionId());
            tx.add(new LastAckCommand() {
                public MessageAck getMessageAck() {
                    return ack;
                }

                public void run(ConnectionContext ctx) throws IOException {
                    topicMessageStore.acknowledge(ctx, clientId, subscriptionName, messageId, ack);
                }

                @Override
                public MessageStore getMessageStore() {
                    return topicMessageStore;
                }

                @Override
                public void rollback(ConnectionContext context) throws IOException {
                    JDBCTopicMessageStore jdbcTopicMessageStore = (JDBCTopicMessageStore)topicMessageStore;
                    ((JDBCPersistenceAdapter)persistenceAdapter).rollbackLastAck(context,
                            jdbcTopicMessageStore,
                            ack,
                            subscriptionName, clientId);
                    jdbcTopicMessageStore.complete(clientId, subscriptionName);
                }


                @Override
                public String getClientId() {
                    return clientId;
                }

                @Override
                public String getSubName() {
                    return subscriptionName;
                }

                @Override
                public long getSequence() {
                    throw new IllegalStateException("Sequence id must be inferred from ack");
                }

                @Override
                public byte getPriority() {
                    throw new IllegalStateException("Priority must be inferred from ack or row");
                }

                @Override
                public void setMessageStore(JDBCTopicMessageStore jdbcTopicMessageStore) {
                    throw new IllegalStateException("message store already known!");
                }
            });
        } else {
            topicMessageStore.acknowledge(null, clientId, subscriptionName, messageId, ack);
        }
    }

}
