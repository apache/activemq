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
package org.apache.activemq.store.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.xa.XAException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.ProxyTopicMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;

/**
 * Provides a TransactionStore implementation that can create transaction aware
 * MessageStore objects from non transaction aware MessageStore objects.
 * 
 * @version $Revision: 1.4 $
 */
public class MemoryTransactionStore implements TransactionStore {

    ConcurrentHashMap<Object, Tx> inflightTransactions = new ConcurrentHashMap<Object, Tx>();

    ConcurrentHashMap<TransactionId, Tx> preparedTransactions = new ConcurrentHashMap<TransactionId, Tx>();

    private boolean doingRecover;

    public static class Tx {
        private ArrayList<AddMessageCommand> messages = new ArrayList<AddMessageCommand>();

        private ArrayList<RemoveMessageCommand> acks = new ArrayList<RemoveMessageCommand>();

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
            // Do all the message adds.
            for (Iterator<AddMessageCommand> iter = messages.iterator(); iter.hasNext();) {
                AddMessageCommand cmd = iter.next();
                cmd.run();
            }
            // And removes..
            for (Iterator<RemoveMessageCommand> iter = acks.iterator(); iter.hasNext();) {
                RemoveMessageCommand cmd = iter.next();
                cmd.run();
            }
        }
    }

    public interface AddMessageCommand {
        Message getMessage();

        void run() throws IOException;
    }

    public interface RemoveMessageCommand {
        MessageAck getMessageAck();

        void run() throws IOException;
    }

    public MessageStore proxy(MessageStore messageStore) {
        return new ProxyMessageStore(messageStore) {
            public void addMessage(ConnectionContext context, final Message send) throws IOException {
                MemoryTransactionStore.this.addMessage(getDelegate(), send);
            }

            public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
                MemoryTransactionStore.this.removeMessage(getDelegate(), ack);
            }
        };
    }

    public TopicMessageStore proxy(TopicMessageStore messageStore) {
        return new ProxyTopicMessageStore(messageStore) {
            public void addMessage(ConnectionContext context, final Message send) throws IOException {
                MemoryTransactionStore.this.addMessage(getDelegate(), send);
            }

            public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
                MemoryTransactionStore.this.removeMessage(getDelegate(), ack);
            }
        };
    }

    /**
     * @see org.apache.activemq.store.TransactionStore#prepare(TransactionId)
     */
    public void prepare(TransactionId txid) {
        Tx tx = inflightTransactions.remove(txid);
        if (tx == null) {
            return;
        }
        preparedTransactions.put(txid, tx);
    }

    public Tx getTx(Object txid) {
        Tx tx = inflightTransactions.get(txid);
        if (tx == null) {
            tx = new Tx();
            inflightTransactions.put(txid, tx);
        }
        return tx;
    }

    /**
     * @throws XAException
     * @see org.apache.activemq.store.TransactionStore#commit(org.apache.activemq.service.Transaction)
     */
    public void commit(TransactionId txid, boolean wasPrepared) throws IOException {

        Tx tx;
        if (wasPrepared) {
            tx = preparedTransactions.remove(txid);
        } else {
            tx = inflightTransactions.remove(txid);
        }

        if (tx == null) {
            return;
        }
        tx.commit();

    }

    /**
     * @see org.apache.activemq.store.TransactionStore#rollback(TransactionId)
     */
    public void rollback(TransactionId txid) {
        preparedTransactions.remove(txid);
        inflightTransactions.remove(txid);
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public synchronized void recover(TransactionRecoveryListener listener) throws IOException {
        // All the inflight transactions get rolled back..
        inflightTransactions.clear();
        this.doingRecover = true;
        try {
            for (Iterator<TransactionId> iter = preparedTransactions.keySet().iterator(); iter.hasNext();) {
                Object txid = iter.next();
                Tx tx = preparedTransactions.get(txid);
                listener.recover((XATransactionId)txid, tx.getMessages(), tx.getAcks());
            }
        } finally {
            this.doingRecover = false;
        }
    }

    /**
     * @param message
     * @throws IOException
     */
    void addMessage(final MessageStore destination, final Message message) throws IOException {

        if (doingRecover) {
            return;
        }

        if (message.getTransactionId() != null) {
            Tx tx = getTx(message.getTransactionId());
            tx.add(new AddMessageCommand() {
                public Message getMessage() {
                    return message;
                }

                public void run() throws IOException {
                    destination.addMessage(null, message);
                }
            });
        } else {
            destination.addMessage(null, message);
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
                public MessageAck getMessageAck() {
                    return ack;
                }

                public void run() throws IOException {
                    destination.removeMessage(null, ack);
                }
            });
        } else {
            destination.removeMessage(null, ack);
        }
    }

    public void delete() {
        inflightTransactions.clear();
        preparedTransactions.clear();
        doingRecover = false;
    }

}
