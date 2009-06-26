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

package org.apache.activemq.store.amq;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.transaction.xa.XAException;

import org.apache.activemq.command.JournalTopicAck;
import org.apache.activemq.command.JournalTransaction;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;

/**
 */
public class AMQTransactionStore implements TransactionStore {

    protected Map<TransactionId, AMQTx> inflightTransactions = new LinkedHashMap<TransactionId, AMQTx>();
    Map<TransactionId, AMQTx> preparedTransactions = new LinkedHashMap<TransactionId, AMQTx>();

    private final AMQPersistenceAdapter peristenceAdapter;
    private boolean doingRecover;

    public AMQTransactionStore(AMQPersistenceAdapter adapter) {
        this.peristenceAdapter = adapter;
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#prepare(TransactionId)
     */
    public void prepare(TransactionId txid) throws IOException {
        AMQTx tx = null;
        synchronized (inflightTransactions) {
            tx = inflightTransactions.remove(txid);
        }
        if (tx == null) {
            return;
        }
        peristenceAdapter.writeCommand(new JournalTransaction(JournalTransaction.XA_PREPARE, txid, false), true);
        synchronized (preparedTransactions) {
            preparedTransactions.put(txid, tx);
        }
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#prepare(TransactionId)
     */
    public void replayPrepare(TransactionId txid) throws IOException {
        AMQTx tx = null;
        synchronized (inflightTransactions) {
            tx = inflightTransactions.remove(txid);
        }
        if (tx == null) {
            return;
        }
        synchronized (preparedTransactions) {
            preparedTransactions.put(txid, tx);
        }
    }

    public AMQTx getTx(TransactionId txid, Location location) {
        AMQTx tx = null;
        synchronized (inflightTransactions) {
            tx = inflightTransactions.get(txid);
            if (tx == null) {
                tx = new AMQTx(location);
                inflightTransactions.put(txid, tx);
            }
        }
        return tx;
    }

    /**
     * @throws XAException
     * @see org.apache.activemq.store.TransactionStore#commit(org.apache.activemq.service.Transaction)
     */
    public void commit(TransactionId txid, boolean wasPrepared) throws IOException {
        AMQTx tx;
        if (wasPrepared) {
            synchronized (preparedTransactions) {
                tx = preparedTransactions.remove(txid);
            }
        } else {
            synchronized (inflightTransactions) {
                tx = inflightTransactions.remove(txid);
            }
        }
        if (tx == null) {
            return;
        }
        if (txid.isXATransaction()) {
            peristenceAdapter.writeCommand(new JournalTransaction(JournalTransaction.XA_COMMIT, txid, wasPrepared), true,true);
        } else {
            peristenceAdapter.writeCommand(new JournalTransaction(JournalTransaction.LOCAL_COMMIT, txid, wasPrepared), true,true);
        }
    }

    /**
     * @throws XAException
     * @see org.apache.activemq.store.TransactionStore#commit(org.apache.activemq.service.Transaction)
     */
    public AMQTx replayCommit(TransactionId txid, boolean wasPrepared) throws IOException {
        if (wasPrepared) {
            synchronized (preparedTransactions) {
                return preparedTransactions.remove(txid);
            }
        } else {
            synchronized (inflightTransactions) {
                return inflightTransactions.remove(txid);
            }
        }
    }

    /**
     * @throws IOException
     * @see org.apache.activemq.store.TransactionStore#rollback(TransactionId)
     */
    public void rollback(TransactionId txid) throws IOException {
        AMQTx tx = null;
        synchronized (inflightTransactions) {
            tx = inflightTransactions.remove(txid);
        }
        if (tx != null) {
            synchronized (preparedTransactions) {
                tx = preparedTransactions.remove(txid);
            }
        }
        if (tx != null) {
            if (txid.isXATransaction()) {
                peristenceAdapter.writeCommand(new JournalTransaction(JournalTransaction.XA_ROLLBACK, txid, false), true,true);
            } else {
                peristenceAdapter.writeCommand(new JournalTransaction(JournalTransaction.LOCAL_ROLLBACK, txid, false), true,true);
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
            Map<TransactionId, AMQTx> txs = null;
            synchronized (preparedTransactions) {
                txs = new LinkedHashMap<TransactionId, AMQTx>(preparedTransactions);
            }
            for (Iterator<TransactionId> iter = txs.keySet().iterator(); iter.hasNext();) {
                Object txid = iter.next();
                AMQTx tx = txs.get(txid);
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
    void addMessage(AMQMessageStore store, Message message, Location location) throws IOException {
        AMQTx tx = getTx(message.getTransactionId(), location);
        tx.add(store, message, location);
    }

    /**
     * @param ack
     * @throws IOException
     */
    public void removeMessage(AMQMessageStore store, MessageAck ack, Location location) throws IOException {
        AMQTx tx = getTx(ack.getTransactionId(), location);
        tx.add(store, ack);
    }

    public void acknowledge(AMQTopicMessageStore store, JournalTopicAck ack, Location location) {
        AMQTx tx = getTx(ack.getTransactionId(), location);
        tx.add(store, ack);
    }

    public Location checkpoint() throws IOException {
        // Nothing really to checkpoint.. since, we don't
        // checkpoint tx operations in to long term store until they are
        // committed.
        // But we keep track of the first location of an operation
        // that was associated with an active tx. The journal can not
        // roll over active tx records.
        Location minimumLocationInUse = null;
        synchronized (inflightTransactions) {
            for (Iterator<AMQTx> iter = inflightTransactions.values().iterator(); iter.hasNext();) {
                AMQTx tx = iter.next();
                Location location = tx.getLocation();
                if (minimumLocationInUse == null || location.compareTo(minimumLocationInUse) < 0) {
                    minimumLocationInUse = location;
                }
            }
        }
        synchronized (preparedTransactions) {
            for (Iterator<AMQTx> iter = preparedTransactions.values().iterator(); iter.hasNext();) {
                AMQTx tx = iter.next();
                Location location = tx.getLocation();
                if (minimumLocationInUse == null || location.compareTo(minimumLocationInUse) < 0) {
                    minimumLocationInUse = location;
                }
            }
            return minimumLocationInUse;
        }
    }

    public boolean isDoingRecover() {
        return doingRecover;
    }

    /**
     * @return the preparedTransactions
     */
    public Map<TransactionId, AMQTx> getPreparedTransactions() {
        return this.preparedTransactions;
    }

    /**
     * @param preparedTransactions the preparedTransactions to set
     */
    public void setPreparedTransactions(Map<TransactionId, AMQTx> preparedTransactions) {
        if (preparedTransactions != null) {
            this.preparedTransactions.clear();
            this.preparedTransactions.putAll(preparedTransactions);
        }
    }
}
