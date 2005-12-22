/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.broker;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

import org.activemq.command.ConnectionInfo;
import org.activemq.command.LocalTransactionId;
import org.activemq.command.Message;
import org.activemq.command.MessageAck;
import org.activemq.command.TransactionId;
import org.activemq.command.XATransactionId;
import org.activemq.store.TransactionRecoveryListener;
import org.activemq.store.TransactionStore;
import org.activemq.transaction.LocalTransaction;
import org.activemq.transaction.Transaction;
import org.activemq.transaction.XATransaction;
import org.activemq.util.IOExceptionSupport;
import org.activemq.util.WrappedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.JMSException;
import javax.transaction.xa.XAException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * This broker filter handles the transaction related operations in the Broker interface.
 * 
 * @version $Revision: 1.10 $
 */
public class TransactionBroker extends BrokerFilter {
    
    private static final Log log = LogFactory.getLog(TransactionBroker.class);

    // The prepared XA transactions.
    private TransactionStore transactionStore;
    private ConcurrentHashMap xaTransactions = new ConcurrentHashMap();

    public TransactionBroker(Broker next, TransactionStore transactionStore) {
        super(next);
        this.transactionStore=transactionStore;
    }
    
    //////////////////////////////////////////////////////////////////////////////
    //
    // Life cycle Methods
    //
    //////////////////////////////////////////////////////////////////////////////
    
    /**
     * Recovers any prepared transactions.
     */
    public void start() throws Exception {
        next.start();
        transactionStore.start();
        try {
            final ConnectionContext context = new ConnectionContext();
            context.setBroker(this);
            context.setInRecoveryMode(true);
            context.setTransactions(new ConcurrentHashMap());
            context.setProducerFlowControl(false);
            
            transactionStore.recover(new TransactionRecoveryListener() {
                public void recover(XATransactionId xid, Message[] addedMessages, MessageAck[] aks) {
                    try {
                        beginTransaction(context, xid);
                        for (int i = 0; i < addedMessages.length; i++) {
                            send(context, addedMessages[i]);
                        }
                        for (int i = 0; i < aks.length; i++) {
                            acknowledge(context, aks[i]);                    
                        }
                        prepareTransaction(context, xid);
                    } catch (Throwable e) {
                        throw new WrappedException(e);
                    }
                }
            });
        } catch (WrappedException e) {
            Throwable cause = e.getCause();
            throw IOExceptionSupport.create("Recovery Failed: "+cause.getMessage(), cause);
        }
    }
    
    public void stop() throws Exception {
        transactionStore.stop();
        next.stop();
    }
 

    //////////////////////////////////////////////////////////////////////////////
    //
    // BrokerFilter overrides
    //
    //////////////////////////////////////////////////////////////////////////////
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Throwable {
        ArrayList txs = new ArrayList();
        for (Iterator iter = xaTransactions.values().iterator(); iter.hasNext();) {
            Transaction tx = (Transaction) iter.next();
            if( tx.isPrepared() )
                txs.add(tx.getTransactionId());
        }
        XATransactionId rc[] = new XATransactionId[txs.size()];
        txs.toArray(rc);
        return rc;
    }

    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        
        // the transaction may have already been started.
        if( xid.isXATransaction() ) {
            Transaction transaction = (Transaction)xaTransactions.get(xid);
            if( transaction != null  )
                return;
            transaction = new XATransaction(transactionStore, (XATransactionId)xid, this);
            xaTransactions.put(xid, transaction);
        } else {
            Map transactionMap = context.getTransactions();        
            Transaction transaction = (Transaction)transactionMap.get(xid);
            if( transaction != null  )
                throw new JMSException("Transaction '"+xid+"' has already been started.");
            
            transaction = new LocalTransaction(transactionStore, (LocalTransactionId)xid, context);
            transactionMap.put(xid, transaction);
        }
    }

    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        Transaction transaction = getTransaction(context, xid, false);
        return transaction.prepare();
    }
    
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Throwable {
        Transaction transaction = getTransaction(context, xid, true);
        transaction.commit(onePhase);
    }

    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        Transaction transaction = getTransaction(context, xid, true);
        transaction.rollback();
    }
    
    public void forgetTransaction(ConnectionContext context, TransactionId xid) throws Throwable {
        Transaction transaction = getTransaction(context, xid, true);
        transaction.rollback();
    }
    
    public void acknowledge(ConnectionContext context, MessageAck ack) throws Throwable {
        // This method may be invoked recursively.  
        // Track original tx so that it can be restored.
        Transaction originalTx = context.getTransaction();
        Transaction transaction=null;
        if( ack.isInTransaction() ) {
            transaction = getTransaction(context, ack.getTransactionId(), false);
        }
        context.setTransaction(transaction);
        try {
            next.acknowledge(context, ack);
        } finally {
            context.setTransaction(originalTx);
        }
    }
    
    public void send(ConnectionContext context, Message message) throws Throwable {
        // This method may be invoked recursively.  
        // Track original tx so that it can be restored.
        Transaction originalTx = context.getTransaction();
        Transaction transaction=null;
        if( message.getTransactionId()!=null ) {
            transaction = getTransaction(context, message.getTransactionId(), false);
        }
        context.setTransaction(transaction);
        try {
            next.send(context, message);
        } finally {
            context.setTransaction(originalTx);
        }
    }
    
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Throwable {
        for (Iterator iter = context.getTransactions().values().iterator(); iter.hasNext();) {
            try {
                Transaction transaction = (Transaction) iter.next();
                transaction.rollback();            
            }
            catch (Exception e) {
                log.warn("ERROR Rolling back disconnected client's transactions: ", e);
            }
            iter.remove();
        }
        next.removeConnection(context, info, error);
    }
    
    //////////////////////////////////////////////////////////////////////////////
    //
    // Implementation help methods.
    //
    //////////////////////////////////////////////////////////////////////////////
    public Transaction getTransaction(ConnectionContext context, TransactionId xid, boolean mightBePrepared) throws JMSException, XAException {
        Map transactionMap = xid.isXATransaction() ? xaTransactions : context.getTransactions();        
        Transaction transaction = (Transaction)transactionMap.get(xid);

        if( transaction != null  )
            return transaction;
        
        if( xid.isXATransaction() ) {
            XAException e = new XAException("Transaction '" + xid + "' has not been started.");
            e.errorCode = XAException.XAER_NOTA;
            throw e;
        } else {
            throw new JMSException("Transaction '" + xid + "' has not been started.");
        }
    }

    public void removeTransaction(XATransactionId xid) {
        xaTransactions.remove(xid);
    }

}
