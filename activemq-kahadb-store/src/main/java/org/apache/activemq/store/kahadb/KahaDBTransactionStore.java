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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.ListenableFuture;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.ProxyTopicMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.MessageDatabase.Operation;
import org.apache.activemq.store.kahadb.data.KahaCommitCommand;
import org.apache.activemq.store.kahadb.data.KahaPrepareCommand;
import org.apache.activemq.store.kahadb.data.KahaRollbackCommand;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a TransactionStore implementation that can create transaction aware
 * MessageStore objects from non transaction aware MessageStore objects.
 *
 *
 */
public class KahaDBTransactionStore implements TransactionStore {
    static final Logger LOG = LoggerFactory.getLogger(KahaDBTransactionStore.class);
    private final KahaDBStore theStore;

    public KahaDBTransactionStore(KahaDBStore theStore) {
        this.theStore = theStore;
    }

    private WireFormat wireFormat(){
      return this.theStore.wireFormat;
    }

    public class Tx {
        private final List<AddMessageCommand> messages = Collections.synchronizedList(new ArrayList<>());

        private final List<RemoveMessageCommand> acks = Collections.synchronizedList(new ArrayList<>());

        public void add(AddMessageCommand msg) {
            messages.add(msg);
        }

        public void add(RemoveMessageCommand ack) {
            acks.add(ack);
        }

        public Message[] getMessages() {
            Message rc[] = new Message[messages.size()];
            int count = 0;
            for (AddMessageCommand cmd : messages) {
                rc[count++] = cmd.getMessage();
            }
            return rc;
        }

        public MessageAck[] getAcks() {
            MessageAck rc[] = new MessageAck[acks.size()];
            int count = 0;
            for (RemoveMessageCommand cmd : acks) {
                rc[count++] = cmd.getMessageAck();
            }
            return rc;
        }

        /**
         * @return true if something to commit
         * @throws IOException
         */
        public List<Future<Object>> commit() throws IOException {
            List<Future<Object>> results = new ArrayList<>();
            // Do all the message adds.
            for (AddMessageCommand cmd : messages) {
                results.add(cmd.run());

            }
            // And removes..
            for (RemoveMessageCommand cmd : acks) {
                cmd.run();
                results.add(cmd.run());
            }

            return results;
        }
    }

    public abstract static class AddMessageCommand {
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

    public abstract static class RemoveMessageCommand {

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
            public void addMessage(ConnectionContext context, final Message send, boolean canOptimize) throws IOException {
                KahaDBTransactionStore.this.addMessage(context, getDelegate(), send);
            }

            @Override
            public ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, Message message) throws IOException {
                return KahaDBTransactionStore.this.asyncAddQueueMessage(context, getDelegate(), message);
            }

            @Override
            public ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, Message message, boolean canOptimize) throws IOException {
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
            public void addMessage(ConnectionContext context, final Message send, boolean canOptimize) throws IOException {
                KahaDBTransactionStore.this.addMessage(context, getDelegate(), send);
            }

            @Override
            public ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, Message message) throws IOException {
                return KahaDBTransactionStore.this.asyncAddTopicMessage(context, getDelegate(), message);
            }

            @Override
            public ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, Message message, boolean canOptimize) throws IOException {
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

            @Override
            public void acknowledge(ConnectionContext context, String clientId, String subscriptionName,
                            MessageId messageId, MessageAck ack) throws IOException {
                KahaDBTransactionStore.this.acknowledge(context, (TopicMessageStore)getDelegate(), clientId,
                        subscriptionName, messageId, ack);
            }

        };
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#prepare(TransactionId)
     */
    @Override
    public void prepare(TransactionId txid) throws IOException {
        KahaTransactionInfo info = getTransactionInfo(txid);
        theStore.store(new KahaPrepareCommand().setTransactionInfo(info), true, null, null);
    }

    @Override
    public void commit(TransactionId txid, boolean wasPrepared, final Runnable preCommit, Runnable postCommit)
            throws IOException {
        if (txid != null) {
            KahaTransactionInfo info = getTransactionInfo(txid);
            if (preCommit != null) {
                preCommit.run();
            }
            theStore.store(new KahaCommitCommand().setTransactionInfo(info), theStore.isEnableJournalDiskSyncs(), null, postCommit);
            forgetRecoveredAcks(txid, false);
        } else {
           LOG.error("Null transaction passed on commit");
        }
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#rollback(TransactionId)
     */
    @Override
    public void rollback(TransactionId txid) throws IOException {
        KahaTransactionInfo info = getTransactionInfo(txid);
        theStore.store(new KahaRollbackCommand().setTransactionInfo(info), theStore.isEnableJournalDiskSyncs(), null, null);
        forgetRecoveredAcks(txid, true);
    }

    protected void forgetRecoveredAcks(TransactionId txid, boolean isRollback) throws IOException {
        if (txid.isXATransaction()) {
            XATransactionId xaTid = ((XATransactionId) txid);
            theStore.forgetRecoveredAcks(xaTid.getPreparedAcks(), isRollback);
        }
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public synchronized void recover(TransactionRecoveryListener listener) throws IOException {
        for (Map.Entry<TransactionId, List<Operation>> entry : theStore.preparedTransactions.entrySet()) {
            XATransactionId xid = (XATransactionId) entry.getKey();
            ArrayList<Message> messageList = new ArrayList<>();
            ArrayList<MessageAck> ackList = new ArrayList<>();

            for (Operation op : entry.getValue()) {
                if (op.getClass() == MessageDatabase.AddOperation.class) {
                    MessageDatabase.AddOperation addOp = (MessageDatabase.AddOperation) op;
                    Message msg = (Message) wireFormat().unmarshal(new DataInputStream(addOp.getCommand().getMessage()
                            .newInput()));
                    messageList.add(msg);
                } else {
                    MessageDatabase.RemoveOperation rmOp = (MessageDatabase.RemoveOperation) op;
                    Buffer ackb = rmOp.getCommand().getAck();
                    MessageAck ack = (MessageAck) wireFormat().unmarshal(new DataInputStream(ackb.newInput()));
                    // allow the ack to be tracked back to its durable sub
                    ConsumerId subKey = new ConsumerId();
                    subKey.setConnectionId(rmOp.getCommand().getSubscriptionKey());
                    ack.setConsumerId(subKey);
                    ackList.add(ack);
                }
            }

            Message[] addedMessages = new Message[messageList.size()];
            MessageAck[] acks = new MessageAck[ackList.size()];
            messageList.toArray(addedMessages);
            ackList.toArray(acks);
            xid.setPreparedAcks(ackList);
            theStore.trackRecoveredAcks(ackList);
            listener.recover(xid, addedMessages, acks);
        }
    }

    /**
     * @param message
     * @throws IOException
     */
    void addMessage(ConnectionContext context, final MessageStore destination, final Message message)
            throws IOException {
        destination.addMessage(context, message);
    }

    ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, final MessageStore destination, final Message message)
            throws IOException {
        if (message.getTransactionId() != null) {
            destination.addMessage(context, message);
            return AbstractMessageStore.FUTURE;
        } else {
            return destination.asyncAddQueueMessage(context, message);
        }
    }

    ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, final MessageStore destination, final Message message)
            throws IOException {

        if (message.getTransactionId() != null) {
            destination.addMessage(context, message);
            return AbstractMessageStore.FUTURE;
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
        destination.removeMessage(context, ack);
    }

    final void removeAsyncMessage(ConnectionContext context, final MessageStore destination, final MessageAck ack)
            throws IOException {
        destination.removeAsyncMessage(context, ack);
    }

    final void acknowledge(ConnectionContext context, final TopicMessageStore destination, final String clientId, final String subscriptionName,
                           final MessageId messageId, final MessageAck ack) throws IOException {
        destination.acknowledge(context, clientId, subscriptionName, messageId, ack);
    }


    private KahaTransactionInfo getTransactionInfo(TransactionId txid) {
        return TransactionIdConversion.convert(theStore.getTransactionIdTransformer().transform(txid));
    }
}
