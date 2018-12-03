/*
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
package org.apache.activemq.store.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.InlineListenableFuture;
import org.apache.activemq.store.ListenableFuture;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.ProxyTopicMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;

/**
 * Provides a TransactionStore implementation that can create transaction aware
 * MessageStore objects from non transaction aware MessageStore objects.
 */
public class MemoryTransactionStore implements TransactionStore {

    protected ConcurrentMap<Object, Tx> inflightTransactions = new ConcurrentHashMap<Object, Tx>();
    protected Map<TransactionId, Tx> preparedTransactions = Collections.synchronizedMap(new LinkedHashMap<TransactionId, Tx>());
    protected final PersistenceAdapter persistenceAdapter;

    private boolean doingRecover;

    public class Tx {

        public List<AddMessageCommand> messages = Collections.synchronizedList(new ArrayList<AddMessageCommand>());

        public final List<RemoveMessageCommand> acks = Collections.synchronizedList(new ArrayList<RemoveMessageCommand>());

        public void add(AddMessageCommand msg) {
            messages.add(msg);
        }

        public void add(RemoveMessageCommand ack) {
            acks.add(ack);
        }

        public Message[] getMessages() {
            Message rc[] = new Message[messages.size()];
            int count = 0;
            for (Iterator<AddMessageCommand> iter = messages.iterator(); iter.hasNext();) {
                AddMessageCommand cmd = iter.next();
                rc[count++] = cmd.getMessage();
            }
            return rc;
        }

        public MessageAck[] getAcks() {
            MessageAck rc[] = new MessageAck[acks.size()];
            int count = 0;
            for (Iterator<RemoveMessageCommand> iter = acks.iterator(); iter.hasNext();) {
                RemoveMessageCommand cmd = iter.next();
                rc[count++] = cmd.getMessageAck();
            }
            return rc;
        }

        /**
         * @throws IOException
         */
        public void commit() throws IOException {
            ConnectionContext ctx = new ConnectionContext();
            persistenceAdapter.beginTransaction(ctx);
            try {

                // Do all the message adds.
                for (Iterator<AddMessageCommand> iter = messages.iterator(); iter.hasNext();) {
                    AddMessageCommand cmd = iter.next();
                    cmd.run(ctx);
                }
                // And removes..
                for (Iterator<RemoveMessageCommand> iter = acks.iterator(); iter.hasNext();) {
                    RemoveMessageCommand cmd = iter.next();
                    cmd.run(ctx);
                }

                persistenceAdapter.commitTransaction(ctx);

            } catch (IOException e) {
                persistenceAdapter.rollbackTransaction(ctx);
                throw e;
            }
        }
    }

    public interface AddMessageCommand {
        Message getMessage();

        MessageStore getMessageStore();

        void run(ConnectionContext context) throws IOException;

        void setMessageStore(MessageStore messageStore);
    }

    public interface RemoveMessageCommand {
        MessageAck getMessageAck();

        void run(ConnectionContext context) throws IOException;

        MessageStore getMessageStore();
    }

    public MemoryTransactionStore(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    public MessageStore proxy(MessageStore messageStore) {
        ProxyMessageStore proxyMessageStore = new ProxyMessageStore(messageStore) {
            @Override
            public void addMessage(ConnectionContext context, final Message send) throws IOException {
                MemoryTransactionStore.this.addMessage(context, getDelegate(), send);
            }

            @Override
            public void addMessage(ConnectionContext context, final Message send, boolean canOptimize) throws IOException {
                MemoryTransactionStore.this.addMessage(context, getDelegate(), send);
            }

            @Override
            public ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, Message message) throws IOException {
                MemoryTransactionStore.this.addMessage(context, getDelegate(), message);
                return new InlineListenableFuture();
            }

            @Override
            public ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, Message message, boolean canoptimize) throws IOException {
                MemoryTransactionStore.this.addMessage(context, getDelegate(), message);
                return new InlineListenableFuture();
            }

            @Override
            public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
                MemoryTransactionStore.this.removeMessage(getDelegate(), ack);
            }

            @Override
            public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
                MemoryTransactionStore.this.removeMessage(getDelegate(), ack);
            }
        };
        onProxyQueueStore(proxyMessageStore);
        return proxyMessageStore;
    }

    protected void onProxyQueueStore(ProxyMessageStore proxyMessageStore) {
    }

    public TopicMessageStore proxy(TopicMessageStore messageStore) {
        ProxyTopicMessageStore proxyTopicMessageStore = new ProxyTopicMessageStore(messageStore) {
            @Override
            public void addMessage(ConnectionContext context, final Message send) throws IOException {
                MemoryTransactionStore.this.addMessage(context, getDelegate(), send);
            }

            @Override
            public void addMessage(ConnectionContext context, final Message send, boolean canOptimize) throws IOException {
                MemoryTransactionStore.this.addMessage(context, getDelegate(), send);
            }

            @Override
            public ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, Message message) throws IOException {
                MemoryTransactionStore.this.addMessage(context, getDelegate(), message);
                return new InlineListenableFuture();
            }

            @Override
            public ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, Message message, boolean canOptimize) throws IOException {
                MemoryTransactionStore.this.addMessage(context, getDelegate(), message);
                return new InlineListenableFuture();
            }

            @Override
            public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
                MemoryTransactionStore.this.removeMessage(getDelegate(), ack);
            }

            @Override
            public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
                MemoryTransactionStore.this.removeMessage(getDelegate(), ack);
            }

            @Override
            public void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId, MessageAck ack)
                throws IOException {
                MemoryTransactionStore.this.acknowledge((TopicMessageStore) getDelegate(), clientId, subscriptionName, messageId, ack);
            }
        };
        onProxyTopicStore(proxyTopicMessageStore);
        return proxyTopicMessageStore;
    }

    protected void onProxyTopicStore(ProxyTopicMessageStore proxyTopicMessageStore) {
    }

    /**
     * @see org.apache.activemq.store.TransactionStore#prepare(TransactionId)
     */
    @Override
    public void prepare(TransactionId txid) throws IOException {
        Tx tx = inflightTransactions.remove(txid);
        if (tx == null) {
            return;
        }
        preparedTransactions.put(txid, tx);
    }

    public Tx getTx(Object txid) {
        Tx tx = inflightTransactions.get(txid);
        if (tx == null) {
            synchronized (inflightTransactions) {
                tx = inflightTransactions.get(txid);
                if ( tx == null) {
                    tx = new Tx();
                    inflightTransactions.put(txid, tx);
                }
            }
        }
        return tx;
    }

    public Tx getPreparedTx(TransactionId txid) {
        Tx tx = preparedTransactions.get(txid);
        if (tx == null) {
            tx = new Tx();
            preparedTransactions.put(txid, tx);
        }
        return tx;
    }

    @Override
    public void commit(TransactionId txid, boolean wasPrepared, Runnable preCommit, Runnable postCommit) throws IOException {
        if (preCommit != null) {
            preCommit.run();
        }
        Tx tx;
        if (wasPrepared) {
            tx = preparedTransactions.get(txid);
        } else {
            tx = inflightTransactions.remove(txid);
        }

        if (tx != null) {
            tx.commit();
        }
        if (wasPrepared) {
            preparedTransactions.remove(txid);
        }
        if (postCommit != null) {
            postCommit.run();
        }
    }

    /**
     * @see org.apache.activemq.store.TransactionStore#rollback(TransactionId)
     */
    @Override
    public void rollback(TransactionId txid) throws IOException {
        preparedTransactions.remove(txid);
        inflightTransactions.remove(txid);
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public synchronized void recover(TransactionRecoveryListener listener) throws IOException {
        // All the inflight transactions get rolled back..
        inflightTransactions.clear();
        this.doingRecover = true;
        try {
            for (Iterator<TransactionId> iter = preparedTransactions.keySet().iterator(); iter.hasNext();) {
                Object txid = iter.next();
                Tx tx = preparedTransactions.get(txid);
                listener.recover((XATransactionId) txid, tx.getMessages(), tx.getAcks());
                onRecovered(tx);
            }
        } finally {
            this.doingRecover = false;
        }
    }

    protected void onRecovered(Tx tx) {
    }

    /**
     * @param message
     * @throws IOException
     */
    void addMessage(final ConnectionContext context, final MessageStore destination, final Message message) throws IOException {

        if (doingRecover) {
            return;
        }

        if (message.getTransactionId() != null) {
            Tx tx = getTx(message.getTransactionId());
            tx.add(new AddMessageCommand() {
                @SuppressWarnings("unused")
                MessageStore messageStore = destination;

                @Override
                public Message getMessage() {
                    return message;
                }

                @Override
                public MessageStore getMessageStore() {
                    return destination;
                }

                @Override
                public void run(ConnectionContext ctx) throws IOException {
                    destination.addMessage(ctx, message);
                }

                @Override
                public void setMessageStore(MessageStore messageStore) {
                    this.messageStore = messageStore;
                }

            });
        } else {
            destination.addMessage(context, message);
        }
    }

    /**
     * @param ack
     * @throws IOException
     */
    final void removeMessage(final MessageStore destination, final MessageAck ack) throws IOException {
        if (doingRecover) {
            return;
        }

        if (ack.isInTransaction()) {
            Tx tx = getTx(ack.getTransactionId());
            tx.add(new RemoveMessageCommand() {
                @Override
                public MessageAck getMessageAck() {
                    return ack;
                }

                @Override
                public void run(ConnectionContext ctx) throws IOException {
                    destination.removeMessage(ctx, ack);
                }

                @Override
                public MessageStore getMessageStore() {
                    return destination;
                }
            });
        } else {
            destination.removeMessage(null, ack);
        }
    }

    public void acknowledge(final TopicMessageStore destination, final String clientId, final String subscriptionName, final MessageId messageId,
        final MessageAck ack) throws IOException {
        if (doingRecover) {
            return;
        }

        if (ack.isInTransaction()) {
            Tx tx = getTx(ack.getTransactionId());
            tx.add(new RemoveMessageCommand() {
                @Override
                public MessageAck getMessageAck() {
                    return ack;
                }

                @Override
                public void run(ConnectionContext ctx) throws IOException {
                    destination.acknowledge(ctx, clientId, subscriptionName, messageId, ack);
                }

                @Override
                public MessageStore getMessageStore() {
                    return destination;
                }
            });
        } else {
            destination.acknowledge(null, clientId, subscriptionName, messageId, ack);
        }
    }

    public void delete() {
        inflightTransactions.clear();
        preparedTransactions.clear();
        doingRecover = false;
    }
}
