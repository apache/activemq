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

package org.apache.activemq.store.journal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.transaction.xa.XAException;

import org.apache.activeio.journal.RecordLocation;
import org.apache.activemq.command.JournalTopicAck;
import org.apache.activemq.command.JournalTransaction;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;

/**
 */
public class JournalTransactionStore implements TransactionStore {

    private final JournalPersistenceAdapter peristenceAdapter;
    Map inflightTransactions = new LinkedHashMap();
    Map preparedTransactions = new LinkedHashMap();
    private boolean doingRecover;

    public static class TxOperation {

        static final byte ADD_OPERATION_TYPE = 0;
        static final byte REMOVE_OPERATION_TYPE = 1;
        static final byte ACK_OPERATION_TYPE = 3;

        public byte operationType;
        public JournalMessageStore store;
        public Object data;

        public TxOperation(byte operationType, JournalMessageStore store, Object data) {
            this.operationType = operationType;
            this.store = store;
            this.data = data;
        }

    }

    /**
     * Operations
     * 
     * @version $Revision: 1.6 $
     */
    public static class Tx {

        private final RecordLocation location;
        private ArrayList operations = new ArrayList();

        public Tx(RecordLocation location) {
            this.location = location;
        }

        public void add(JournalMessageStore store, Message msg) {
            operations.add(new TxOperation(TxOperation.ADD_OPERATION_TYPE, store, msg));
        }

        public void add(JournalMessageStore store, MessageAck ack) {
            operations.add(new TxOperation(TxOperation.REMOVE_OPERATION_TYPE, store, ack));
        }

        public void add(JournalTopicMessageStore store, JournalTopicAck ack) {
            operations.add(new TxOperation(TxOperation.ACK_OPERATION_TYPE, store, ack));
        }

        public Message[] getMessages() {
            ArrayList list = new ArrayList();
            for (Iterator iter = operations.iterator(); iter.hasNext();) {
                TxOperation op = (TxOperation)iter.next();
                if (op.operationType == TxOperation.ADD_OPERATION_TYPE) {
                    list.add(op.data);
                }
            }
            Message rc[] = new Message[list.size()];
            list.toArray(rc);
            return rc;
        }

        public MessageAck[] getAcks() {
            ArrayList list = new ArrayList();
            for (Iterator iter = operations.iterator(); iter.hasNext();) {
                TxOperation op = (TxOperation)iter.next();
                if (op.operationType == TxOperation.REMOVE_OPERATION_TYPE) {
                    list.add(op.data);
                }
            }
            MessageAck rc[] = new MessageAck[list.size()];
            list.toArray(rc);
            return rc;
        }

        public ArrayList getOperations() {
            return operations;
        }

    }

    public JournalTransactionStore(JournalPersistenceAdapter adapter) {
        this.peristenceAdapter = adapter;
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#prepare(TransactionId)
     */
    public void prepare(TransactionId txid) throws IOException {
        Tx tx = null;
        synchronized (inflightTransactions) {
            tx = (Tx)inflightTransactions.remove(txid);
        }
        if (tx == null) {
            return;
        }
        peristenceAdapter.writeCommand(new JournalTransaction(JournalTransaction.XA_PREPARE, txid, false),
                                       true);
        synchronized (preparedTransactions) {
            preparedTransactions.put(txid, tx);
        }
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#prepare(TransactionId)
     */
    public void replayPrepare(TransactionId txid) throws IOException {
        Tx tx = null;
        synchronized (inflightTransactions) {
            tx = (Tx)inflightTransactions.remove(txid);
        }
        if (tx == null) {
            return;
        }
        synchronized (preparedTransactions) {
            preparedTransactions.put(txid, tx);
        }
    }

    public Tx getTx(Object txid, RecordLocation location) {
        Tx tx = null;
        synchronized (inflightTransactions) {
            tx = (Tx)inflightTransactions.get(txid);
        }
        if (tx == null) {
            tx = new Tx(location);
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
            synchronized (preparedTransactions) {
                tx = (Tx)preparedTransactions.remove(txid);
            }
        } else {
            synchronized (inflightTransactions) {
                tx = (Tx)inflightTransactions.remove(txid);
            }
        }
        if (tx == null) {
            return;
        }
        if (txid.isXATransaction()) {
            peristenceAdapter.writeCommand(new JournalTransaction(JournalTransaction.XA_COMMIT, txid,
                                                                  wasPrepared), true);
        } else {
            peristenceAdapter.writeCommand(new JournalTransaction(JournalTransaction.LOCAL_COMMIT, txid,
                                                                  wasPrepared), true);
        }
    }

    /**
     * @throws XAException
     * @see org.apache.activemq.store.TransactionStore#commit(org.apache.activemq.service.Transaction)
     */
    public Tx replayCommit(TransactionId txid, boolean wasPrepared) throws IOException {
        if (wasPrepared) {
            synchronized (preparedTransactions) {
                return (Tx)preparedTransactions.remove(txid);
            }
        } else {
            synchronized (inflightTransactions) {
                return (Tx)inflightTransactions.remove(txid);
            }
        }
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#rollback(TransactionId)
     */
    public void rollback(TransactionId txid) throws IOException {
        Tx tx = null;
        synchronized (inflightTransactions) {
            tx = (Tx)inflightTransactions.remove(txid);
        }
        if (tx != null) {
            synchronized (preparedTransactions) {
                tx = (Tx)preparedTransactions.remove(txid);
            }
        }
        if (tx != null) {
            if (txid.isXATransaction()) {
                peristenceAdapter.writeCommand(new JournalTransaction(JournalTransaction.XA_ROLLBACK, txid,
                                                                      false), true);
            } else {
                peristenceAdapter.writeCommand(new JournalTransaction(JournalTransaction.LOCAL_ROLLBACK,
                                                                      txid, false), true);
            }
        }
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#rollback(TransactionId)
     */
    public void replayRollback(TransactionId txid) throws IOException {
        boolean inflight = false;
        synchronized (inflightTransactions) {
            inflight = inflightTransactions.remove(txid) != null;
        }
        if (inflight) {
            synchronized (preparedTransactions) {
                preparedTransactions.remove(txid);
            }
        }
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public synchronized void recover(TransactionRecoveryListener listener) throws IOException {
        // All the in-flight transactions get rolled back..
        synchronized (inflightTransactions) {
            inflightTransactions.clear();
        }
        this.doingRecover = true;
        try {
            Map txs = null;
            synchronized (preparedTransactions) {
                txs = new LinkedHashMap(preparedTransactions);
            }
            for (Iterator iter = txs.keySet().iterator(); iter.hasNext();) {
                Object txid = (Object)iter.next();
                Tx tx = (Tx)txs.get(txid);
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
    void addMessage(JournalMessageStore store, Message message, RecordLocation location) throws IOException {
        Tx tx = getTx(message.getTransactionId(), location);
        tx.add(store, message);
    }

    /**
     * @param ack
     * @throws IOException
     */
    public void removeMessage(JournalMessageStore store, MessageAck ack, RecordLocation location)
        throws IOException {
        Tx tx = getTx(ack.getTransactionId(), location);
        tx.add(store, ack);
    }

    public void acknowledge(JournalTopicMessageStore store, JournalTopicAck ack, RecordLocation location) {
        Tx tx = getTx(ack.getTransactionId(), location);
        tx.add(store, ack);
    }

    public RecordLocation checkpoint() throws IOException {
        // Nothing really to checkpoint.. since, we don't
        // checkpoint tx operations in to long term store until they are
        // committed.
        // But we keep track of the first location of an operation
        // that was associated with an active tx. The journal can not
        // roll over active tx records.
        RecordLocation rc = null;
        synchronized (inflightTransactions) {
            for (Iterator iter = inflightTransactions.values().iterator(); iter.hasNext();) {
                Tx tx = (Tx)iter.next();
                RecordLocation location = tx.location;
                if (rc == null || rc.compareTo(location) < 0) {
                    rc = location;
                }
            }
        }
        synchronized (preparedTransactions) {
            for (Iterator iter = preparedTransactions.values().iterator(); iter.hasNext();) {
                Tx tx = (Tx)iter.next();
                RecordLocation location = tx.location;
                if (rc == null || rc.compareTo(location) < 0) {
                    rc = location;
                }
            }
            return rc;
        }
    }

    public boolean isDoingRecover() {
        return doingRecover;
    }

}
