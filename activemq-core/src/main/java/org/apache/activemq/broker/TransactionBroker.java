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
package org.apache.activemq.broker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.transaction.xa.XAException;

import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.transaction.LocalTransaction;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.transaction.Transaction;
import org.apache.activemq.transaction.XATransaction;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.WrappedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This broker filter handles the transaction related operations in the Broker
 * interface.
 * 
 * @version $Revision: 1.10 $
 */
public class TransactionBroker extends BrokerFilter {

    private static final Log log = LogFactory.getLog(TransactionBroker.class);

    // The prepared XA transactions.
    private TransactionStore transactionStore;
    private Map xaTransactions = new LinkedHashMap();
    ActiveMQMessageAudit audit;

    public TransactionBroker(Broker next, TransactionStore transactionStore) {
        super(next);
        this.transactionStore = transactionStore;
    }

    // ////////////////////////////////////////////////////////////////////////////
    //
    // Life cycle Methods
    //
    // ////////////////////////////////////////////////////////////////////////////

    /**
     * Recovers any prepared transactions.
     */
    public void start() throws Exception {
        transactionStore.start();
        try {
            final ConnectionContext context = new ConnectionContext();
            context.setBroker(this);
            context.setInRecoveryMode(true);
            context.setTransactions(new ConcurrentHashMap());
            context.setProducerFlowControl(false);
            final ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
            producerExchange.setMutable(true);
            producerExchange.setConnectionContext(context);
            final ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
            consumerExchange.setConnectionContext(context);
            transactionStore.recover(new TransactionRecoveryListener() {
                public void recover(XATransactionId xid, Message[] addedMessages, MessageAck[] aks) {
                    try {
                        beginTransaction(context, xid);
                        for (int i = 0; i < addedMessages.length; i++) {
                            send(producerExchange, addedMessages[i]);
                        }
                        for (int i = 0; i < aks.length; i++) {
                            acknowledge(consumerExchange, aks[i]);
                        }
                        prepareTransaction(context, xid);
                    } catch (Throwable e) {
                        throw new WrappedException(e);
                    }
                }
            });
        } catch (WrappedException e) {
            Throwable cause = e.getCause();
            throw IOExceptionSupport.create("Recovery Failed: " + cause.getMessage(), cause);
        }
        next.start();
    }

    public void stop() throws Exception {
        transactionStore.stop();
        next.stop();
    }

    // ////////////////////////////////////////////////////////////////////////////
    //
    // BrokerFilter overrides
    //
    // ////////////////////////////////////////////////////////////////////////////
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        ArrayList txs = new ArrayList();
        synchronized (xaTransactions) {
            for (Iterator iter = xaTransactions.values().iterator(); iter.hasNext();) {
                Transaction tx = (Transaction)iter.next();
                if (tx.isPrepared())
                    txs.add(tx.getTransactionId());
            }
        }
        XATransactionId rc[] = new XATransactionId[txs.size()];
        txs.toArray(rc);
        return rc;
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        // the transaction may have already been started.
        if (xid.isXATransaction()) {
            Transaction transaction = null;
            synchronized (xaTransactions) {
                transaction = (Transaction)xaTransactions.get(xid);
                if (transaction != null)
                    return;
                transaction = new XATransaction(transactionStore, (XATransactionId)xid, this);
                xaTransactions.put(xid, transaction);
            }
        } else {
            Map transactionMap = context.getTransactions();
            Transaction transaction = (Transaction)transactionMap.get(xid);
            if (transaction != null)
                throw new JMSException("Transaction '" + xid + "' has already been started.");
            transaction = new LocalTransaction(transactionStore, (LocalTransactionId)xid, context);
            transactionMap.put(xid, transaction);
        }
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        Transaction transaction = getTransaction(context, xid, false);
        return transaction.prepare();
    }

    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        Transaction transaction = getTransaction(context, xid, true);
        transaction.commit(onePhase);
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        Transaction transaction = getTransaction(context, xid, true);
        transaction.rollback();
    }

    public void forgetTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        Transaction transaction = getTransaction(context, xid, true);
        transaction.rollback();
    }

    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        // This method may be invoked recursively.
        // Track original tx so that it can be restored.
        final ConnectionContext context = consumerExchange.getConnectionContext();
        Transaction originalTx = context.getTransaction();
        Transaction transaction = null;
        if (ack.isInTransaction()) {
            transaction = getTransaction(context, ack.getTransactionId(), false);
        }
        context.setTransaction(transaction);
        try {
            next.acknowledge(consumerExchange, ack);
        } finally {
            context.setTransaction(originalTx);
        }
    }

    public void send(ProducerBrokerExchange producerExchange, final Message message) throws Exception {
        // This method may be invoked recursively.
        // Track original tx so that it can be restored.
        final ConnectionContext context = producerExchange.getConnectionContext();
        Transaction originalTx = context.getTransaction();
        Transaction transaction = null;
        Synchronization sync = null;
        if (message.getTransactionId() != null) {
            transaction = getTransaction(context, message.getTransactionId(), false);
            if (transaction != null) {
                sync = new Synchronization() {

                    public void afterRollback() {
                        if (audit != null) {
                            audit.rollbackMessageReference(message);
                        }
                    }
                };
                transaction.addSynchronization(sync);
            }
        }
        if (audit == null || !audit.isDuplicateMessageReference(message)) {
            context.setTransaction(transaction);
            try {
                next.send(producerExchange, message);
            } finally {
                context.setTransaction(originalTx);
            }
        } else {
            if (sync != null && transaction != null) {
                transaction.removeSynchronization(sync);
            }
            if (log.isDebugEnabled()) {
                log.debug("IGNORING duplicate message " + message);
            }
        }
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        for (Iterator iter = context.getTransactions().values().iterator(); iter.hasNext();) {
            try {
                Transaction transaction = (Transaction)iter.next();
                transaction.rollback();
            } catch (Exception e) {
                log.warn("ERROR Rolling back disconnected client's transactions: ", e);
            }
            iter.remove();
        }
        next.removeConnection(context, info, error);
    }

    // ////////////////////////////////////////////////////////////////////////////
    //
    // Implementation help methods.
    //
    // ////////////////////////////////////////////////////////////////////////////
    public Transaction getTransaction(ConnectionContext context, TransactionId xid, boolean mightBePrepared) throws JMSException, XAException {
        Map transactionMap = null;
        synchronized (xaTransactions) {
            transactionMap = xid.isXATransaction() ? xaTransactions : context.getTransactions();
        }
        Transaction transaction = (Transaction)transactionMap.get(xid);
        if (transaction != null)
            return transaction;
        if (xid.isXATransaction()) {
            XAException e = new XAException("Transaction '" + xid + "' has not been started.");
            e.errorCode = XAException.XAER_NOTA;
            throw e;
        } else {
            throw new JMSException("Transaction '" + xid + "' has not been started.");
        }
    }

    public void removeTransaction(XATransactionId xid) {
        synchronized (xaTransactions) {
            xaTransactions.remove(xid);
        }
    }

    public synchronized void brokerServiceStarted() {
        super.brokerServiceStarted();
        if (getBrokerService().isSupportFailOver() && audit == null) {
            audit = new ActiveMQMessageAudit();
        }
    }

}
