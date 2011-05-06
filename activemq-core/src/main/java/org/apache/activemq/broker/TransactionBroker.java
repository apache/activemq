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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.transaction.xa.XAException;

import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BaseCommand;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.transaction.LocalTransaction;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.transaction.Transaction;
import org.apache.activemq.transaction.XATransaction;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.WrappedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This broker filter handles the transaction related operations in the Broker
 * interface.
 * 
 * 
 */
public class TransactionBroker extends BrokerFilter {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionBroker.class);

    // The prepared XA transactions.
    private TransactionStore transactionStore;
    private Map<TransactionId, XATransaction> xaTransactions = new LinkedHashMap<TransactionId, XATransaction>();
    private ActiveMQMessageAudit audit;

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
            context.setTransactions(new ConcurrentHashMap<TransactionId, Transaction>());
            context.setProducerFlowControl(false);
            final ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
            producerExchange.setMutable(true);
            producerExchange.setConnectionContext(context);
            producerExchange.setProducerState(new ProducerState(new ProducerInfo()));
            final ConsumerBrokerExchange consumerExchange = new ConsumerBrokerExchange();
            consumerExchange.setConnectionContext(context);
            transactionStore.recover(new TransactionRecoveryListener() {
                public void recover(XATransactionId xid, Message[] addedMessages, MessageAck[] aks) {
                    try {
                        beginTransaction(context, xid);
                        XATransaction transaction = (XATransaction) getTransaction(context, xid, false);
                        for (int i = 0; i < addedMessages.length; i++) {
                            forceDestinationWakeupOnCompletion(context, transaction, addedMessages[i].getDestination(), addedMessages[i]);
                        }
                        for (int i = 0; i < aks.length; i++) {
                            forceDestinationWakeupOnCompletion(context, transaction, aks[i].getDestination(), aks[i]);
                        }
                        transaction.setState(Transaction.PREPARED_STATE);
                        registerMBean(transaction);
                        LOG.debug("recovered prepared transaction: " + transaction.getTransactionId());
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

    private void registerMBean(XATransaction transaction) {
        if (getBrokerService().getRegionBroker() instanceof ManagedRegionBroker ) {
            ManagedRegionBroker managedRegionBroker = (ManagedRegionBroker) getBrokerService().getRegionBroker();
            managedRegionBroker.registerRecoveredTransactionMBean(transaction);
        }
    }

    private void forceDestinationWakeupOnCompletion(ConnectionContext context, Transaction transaction,
                                                    ActiveMQDestination amqDestination, BaseCommand ack) throws Exception {
        Destination destination =  addDestination(context, amqDestination, false);
        registerSync(destination, transaction, ack);
    }

    private void registerSync(Destination destination, Transaction transaction, BaseCommand command) {
        if (destination instanceof Queue) {
            Synchronization sync = new PreparedDestinationCompletion((Queue) destination, command.isMessage());
            // ensure one per destination in the list
            transaction.removeSynchronization(sync);
            transaction.addSynchronization(sync);
        }
    }

    static class PreparedDestinationCompletion extends Synchronization {
        final Queue queue;
        final boolean messageSend;
        public PreparedDestinationCompletion(final Queue queue, boolean messageSend) {
            this.queue = queue;
            // rollback relevant to acks, commit to sends
            this.messageSend = messageSend;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(queue) +
                    System.identityHashCode(Boolean.valueOf(messageSend));
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof PreparedDestinationCompletion &&
                    queue.equals(((PreparedDestinationCompletion) other).queue) &&
                    messageSend == ((PreparedDestinationCompletion) other).messageSend;
        }

        @Override
        public void afterRollback() throws Exception {
            if (!messageSend) {
                queue.clearPendingMessages();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("cleared pending from afterRollback : " + queue);
                }
            }
        }

        @Override
        public void afterCommit() throws Exception {
            if (messageSend) {
                queue.clearPendingMessages();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("cleared pending from afterCommit : " + queue);
                }
            }
        }
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
        List<TransactionId> txs = new ArrayList<TransactionId>();
        synchronized (xaTransactions) {
            for (Iterator<XATransaction> iter = xaTransactions.values().iterator(); iter.hasNext();) {
                Transaction tx = iter.next();
                if (tx.isPrepared()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("prepared transaction: " + tx.getTransactionId());
                    }
                    txs.add(tx.getTransactionId());
                }
            }
        }
        XATransactionId rc[] = new XATransactionId[txs.size()];
        txs.toArray(rc);
        if (LOG.isDebugEnabled()) {
            LOG.debug("prepared transaction list size: " + rc.length);
        }
        return rc;
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        // the transaction may have already been started.
        if (xid.isXATransaction()) {
            XATransaction transaction = null;
            synchronized (xaTransactions) {
                transaction = xaTransactions.get(xid);
                if (transaction != null) {
                    return;
                }
                transaction = new XATransaction(transactionStore, (XATransactionId)xid, this, context.getConnectionId());
                xaTransactions.put(xid, transaction);
            }
        } else {
            Map<TransactionId, Transaction> transactionMap = context.getTransactions();
            Transaction transaction = transactionMap.get(xid);
            if (transaction != null) {
                throw new JMSException("Transaction '" + xid + "' has already been started.");
            }
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
                            audit.rollback(message);
                        }
                    }
                };
                transaction.addSynchronization(sync);
            }
        }
        if (audit == null || !audit.isDuplicate(message)) {
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("IGNORING duplicate message " + message);
            }
        }
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        for (Iterator<Transaction> iter = context.getTransactions().values().iterator(); iter.hasNext();) {
            try {
                Transaction transaction = iter.next();
                transaction.rollback();
            } catch (Exception e) {
                LOG.warn("ERROR Rolling back disconnected client's transactions: ", e);
            }
            iter.remove();
        }

        synchronized (xaTransactions) {
            // first find all txs that belongs to the connection
            ArrayList<XATransaction> txs = new ArrayList<XATransaction>();
            for (XATransaction tx : xaTransactions.values()) {
                if (tx.getConnectionId() != null && tx.getConnectionId().equals(info.getConnectionId()) && !tx.isPrepared()) {
                    txs.add(tx);
                }
            }

            // then remove them
            // two steps needed to avoid ConcurrentModificationException, from removeTransaction()
            for (XATransaction tx : txs) {
                try {
                    tx.rollback();
                } catch (Exception e) {
                    LOG.warn("ERROR Rolling back disconnected client's xa transactions: ", e);
                }
            }

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
        if (transaction != null) {
            return transaction;
        }
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
