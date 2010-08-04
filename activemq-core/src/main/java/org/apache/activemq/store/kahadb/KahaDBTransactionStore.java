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
package org.apache.activemq.store.kahadb;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.transaction.xa.XAException;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.ProxyTopicMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.MessageDatabase.AddOpperation;
import org.apache.activemq.store.kahadb.MessageDatabase.Operation;
import org.apache.activemq.store.kahadb.MessageDatabase.RemoveOpperation;
import org.apache.activemq.store.kahadb.data.KahaCommitCommand;
import org.apache.activemq.store.kahadb.data.KahaPrepareCommand;
import org.apache.activemq.store.kahadb.data.KahaRollbackCommand;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Provides a TransactionStore implementation that can create transaction aware
 * MessageStore objects from non transaction aware MessageStore objects.
 * 
 * @version $Revision: 1.4 $
 */
public class KahaDBTransactionStore implements TransactionStore {
    static final Log LOG = LogFactory.getLog(KahaDBTransactionStore.class);
    ConcurrentHashMap<Object, Tx> inflightTransactions = new ConcurrentHashMap<Object, Tx>();
    private final WireFormat wireFormat = new OpenWireFormat();
    private final KahaDBStore theStore;

    public KahaDBTransactionStore(KahaDBStore theStore) {
        this.theStore = theStore;
    }

    public class Tx {
        private final ArrayList<AddMessageCommand> messages = new ArrayList<AddMessageCommand>();

        private final ArrayList<RemoveMessageCommand> acks = new ArrayList<RemoveMessageCommand>();

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
         * @return true if something to commit
         * @throws IOException
         */
        public List<Future<Object>> commit() throws IOException {
            List<Future<Object>> results = new ArrayList<Future<Object>>();
            // Do all the message adds.
            for (Iterator<AddMessageCommand> iter = messages.iterator(); iter.hasNext();) {
                AddMessageCommand cmd = iter.next();
                results.add(cmd.run());

            }
            // And removes..
            for (Iterator<RemoveMessageCommand> iter = acks.iterator(); iter.hasNext();) {
                RemoveMessageCommand cmd = iter.next();
                cmd.run();
                results.add(cmd.run());
            }
            
            return results;
        }
    }

    public abstract class AddMessageCommand {
        private final ConnectionContext ctx;
        AddMessageCommand(ConnectionContext ctx) {
            this.ctx = ctx;
        }
        abstract Message getMessage();
        Future<Object> run() throws IOException {
            return run(this.ctx);
        }
        abstract Future<Object> run(ConnectionContext ctx) throws IOException;
    }

    public abstract class RemoveMessageCommand {

        private final ConnectionContext ctx;
        RemoveMessageCommand(ConnectionContext ctx) {
            this.ctx = ctx;
        }
        abstract MessageAck getMessageAck();
        Future<Object> run() throws IOException {
            return run(this.ctx);
        }
        abstract Future<Object> run(ConnectionContext context) throws IOException;
    }

    public MessageStore proxy(MessageStore messageStore) {
        return new ProxyMessageStore(messageStore) {
            @Override
            public void addMessage(ConnectionContext context, final Message send) throws IOException {
                KahaDBTransactionStore.this.addMessage(context, getDelegate(), send);
            }

            @Override
            public Future<Object> asyncAddQueueMessage(ConnectionContext context, Message message) throws IOException {
                return KahaDBTransactionStore.this.asyncAddQueueMessage(context, getDelegate(), message);
            }

            @Override
            public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
                KahaDBTransactionStore.this.removeMessage(context, getDelegate(), ack);
            }

            @Override
            public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
                KahaDBTransactionStore.this.removeAsyncMessage(context, getDelegate(), ack);
            }
        };
    }

    public TopicMessageStore proxy(TopicMessageStore messageStore) {
        return new ProxyTopicMessageStore(messageStore) {
            @Override
            public void addMessage(ConnectionContext context, final Message send) throws IOException {
                KahaDBTransactionStore.this.addMessage(context, getDelegate(), send);
            }

            @Override
            public Future<Object> asyncAddTopicMessage(ConnectionContext context, Message message) throws IOException {
                return KahaDBTransactionStore.this.asyncAddTopicMessage(context, getDelegate(), message);
            }

            @Override
            public void removeMessage(ConnectionContext context, final MessageAck ack) throws IOException {
                KahaDBTransactionStore.this.removeMessage(context, getDelegate(), ack);
            }

            @Override
            public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
                KahaDBTransactionStore.this.removeAsyncMessage(context, getDelegate(), ack);
            }
        };
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#prepare(TransactionId)
     */
    public void prepare(TransactionId txid) throws IOException {
        inflightTransactions.remove(txid);
        KahaTransactionInfo info = getTransactionInfo(txid);
        theStore.store(new KahaPrepareCommand().setTransactionInfo(info), true, null, null);
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
     * @see org.apache.activemq.store.TransactionStore#commit(org.apache.activemq.service.Transaction)
     */
    public void commit(TransactionId txid, boolean wasPrepared, Runnable preCommit, Runnable postCommit)
            throws IOException {
        if (txid != null) {
            if (!txid.isXATransaction() && theStore.isConcurrentStoreAndDispatchTransactions()) {
                if (preCommit != null) {
                    preCommit.run();
                }
                Tx tx = inflightTransactions.remove(txid);
                if (tx != null) {
                    List<Future<Object>> results = tx.commit();
                    boolean doneSomething = false;
                    for (Future<Object> result : results) {
                        try {
                            result.get();
                        } catch (InterruptedException e) {
                            theStore.brokerService.handleIOException(new IOException(e.getMessage()));
                        } catch (ExecutionException e) {
                            theStore.brokerService.handleIOException(new IOException(e.getMessage()));
                        }catch(CancellationException e) {
                        }
                        if (!result.isCancelled()) {
                            doneSomething = true;
                        }
                    }
                    if (postCommit != null) {
                        postCommit.run();
                    }
                    if (doneSomething) {
                        KahaTransactionInfo info = getTransactionInfo(txid);
                        theStore.store(new KahaCommitCommand().setTransactionInfo(info), true, null, null);
                    }
                }else {
                    //The Tx will be null for failed over clients - lets run their post commits
                    if (postCommit != null) {
                        postCommit.run();
                    }
                }

            } else {
                KahaTransactionInfo info = getTransactionInfo(txid);
                theStore.store(new KahaCommitCommand().setTransactionInfo(info), true, preCommit, postCommit);
            }
        }else {
           LOG.error("Null transaction passed on commit");
        }
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#rollback(TransactionId)
     */
    public void rollback(TransactionId txid) throws IOException {
        if (txid.isXATransaction() || theStore.isConcurrentStoreAndDispatchTransactions()) {
            KahaTransactionInfo info = getTransactionInfo(txid);
            theStore.store(new KahaRollbackCommand().setTransactionInfo(info), false, null, null);
        } else {
            inflightTransactions.remove(txid);
        }
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public synchronized void recover(TransactionRecoveryListener listener) throws IOException {
        // All the inflight transactions get rolled back..
        // inflightTransactions.clear();
        for (Map.Entry<TransactionId, List<Operation>> entry : theStore.preparedTransactions.entrySet()) {
            XATransactionId xid = (XATransactionId) entry.getKey();
            ArrayList<Message> messageList = new ArrayList<Message>();
            ArrayList<MessageAck> ackList = new ArrayList<MessageAck>();

            for (Operation op : entry.getValue()) {
                if (op.getClass() == AddOpperation.class) {
                    AddOpperation addOp = (AddOpperation) op;
                    Message msg = (Message) wireFormat.unmarshal(new DataInputStream(addOp.getCommand().getMessage()
                            .newInput()));
                    messageList.add(msg);
                } else {
                    RemoveOpperation rmOp = (RemoveOpperation) op;
                    Buffer ackb = rmOp.getCommand().getAck();
                    MessageAck ack = (MessageAck) wireFormat.unmarshal(new DataInputStream(ackb.newInput()));
                    ackList.add(ack);
                }
            }

            Message[] addedMessages = new Message[messageList.size()];
            MessageAck[] acks = new MessageAck[ackList.size()];
            messageList.toArray(addedMessages);
            ackList.toArray(acks);
            listener.recover(xid, addedMessages, acks);
        }
    }

    /**
     * @param message
     * @throws IOException
     */
    void addMessage(ConnectionContext context, final MessageStore destination, final Message message)
            throws IOException {

        if (message.getTransactionId() != null) {
            if (message.getTransactionId().isXATransaction() || theStore.isConcurrentStoreAndDispatchTransactions() == false) {
                destination.addMessage(context, message);
            } else {
                Tx tx = getTx(message.getTransactionId());
                tx.add(new AddMessageCommand(context) {
                    @Override
                    public Message getMessage() {
                        return message;
                    }
                    @Override
                    public Future<Object> run(ConnectionContext ctx) throws IOException {
                        destination.addMessage(ctx, message);
                        return AbstractMessageStore.FUTURE;
                    }

                });
            }
        } else {
            destination.addMessage(context, message);
        }
    }

    Future<Object> asyncAddQueueMessage(ConnectionContext context, final MessageStore destination, final Message message)
            throws IOException {

        if (message.getTransactionId() != null) {
            if (message.getTransactionId().isXATransaction() || theStore.isConcurrentStoreAndDispatchTransactions() == false) {
                destination.addMessage(context, message);
                return AbstractMessageStore.FUTURE;
            } else {
                Tx tx = getTx(message.getTransactionId());
                tx.add(new AddMessageCommand(context) {
                    @Override
                    public Message getMessage() {
                        return message;
                    }
                    @Override
                    public Future<Object> run(ConnectionContext ctx) throws IOException {
                        return destination.asyncAddQueueMessage(ctx, message);
                    }

                });
                return AbstractMessageStore.FUTURE;
            }
        } else {
            return destination.asyncAddQueueMessage(context, message);
        }
    }

    Future<Object> asyncAddTopicMessage(ConnectionContext context, final MessageStore destination, final Message message)
            throws IOException {

        if (message.getTransactionId() != null) {
            if (message.getTransactionId().isXATransaction() || theStore.isConcurrentStoreAndDispatchTransactions()==false) {
                destination.addMessage(context, message);
                return AbstractMessageStore.FUTURE;
            } else {
                Tx tx = getTx(message.getTransactionId());
                tx.add(new AddMessageCommand(context) {
                    @Override
                    public Message getMessage() {
                        return message;
                    }
                    @Override
                    public Future run(ConnectionContext ctx) throws IOException {
                        return destination.asyncAddTopicMessage(ctx, message);
                    }

                });
                return AbstractMessageStore.FUTURE;
            }
        } else {
            return destination.asyncAddTopicMessage(context, message);
        }
    }

    /**
     * @param ack
     * @throws IOException
     */
    final void removeMessage(ConnectionContext context, final MessageStore destination, final MessageAck ack)
            throws IOException {

        if (ack.isInTransaction()) {
            if (ack.getTransactionId().isXATransaction() || theStore.isConcurrentStoreAndDispatchTransactions()== false) {
                destination.removeMessage(context, ack);
            } else {
                Tx tx = getTx(ack.getTransactionId());
                tx.add(new RemoveMessageCommand(context) {
                    @Override
                    public MessageAck getMessageAck() {
                        return ack;
                    }

                    @Override
                    public Future<Object> run(ConnectionContext ctx) throws IOException {
                        destination.removeMessage(ctx, ack);
                        return AbstractMessageStore.FUTURE;
                    }
                });
            }
        } else {
            destination.removeMessage(context, ack);
        }
    }

    final void removeAsyncMessage(ConnectionContext context, final MessageStore destination, final MessageAck ack)
            throws IOException {

        if (ack.isInTransaction()) {
            if (ack.getTransactionId().isXATransaction() || theStore.isConcurrentStoreAndDispatchTransactions()==false) {
                destination.removeAsyncMessage(context, ack);
            } else {
                Tx tx = getTx(ack.getTransactionId());
                tx.add(new RemoveMessageCommand(context) {
                    @Override
                    public MessageAck getMessageAck() {
                        return ack;
                    }

                    @Override
                    public Future<Object> run(ConnectionContext ctx) throws IOException {
                        destination.removeMessage(ctx, ack);
                        return AbstractMessageStore.FUTURE;
                    }
                });
            }
        } else {
            destination.removeAsyncMessage(context, ack);
        }
    }

    private KahaTransactionInfo getTransactionInfo(TransactionId txid) {
        return theStore.createTransactionInfo(txid);
    }

}
