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
package org.apache.activemq;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.jms.TransactionInProgressException;
import javax.jms.TransactionRolledBackException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.DataArrayResponse;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.IntegerResponse;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A TransactionContext provides the means to control a JMS transaction. It
 * provides a local transaction interface and also an XAResource interface. <p/>
 * An application server controls the transactional assignment of an XASession
 * by obtaining its XAResource. It uses the XAResource to assign the session to
 * a transaction, prepare and commit work on the transaction, and so on. <p/> An
 * XAResource provides some fairly sophisticated facilities for interleaving
 * work on multiple transactions, recovering a list of transactions in progress,
 * and so on. A JTA aware JMS provider must fully implement this functionality.
 * This could be done by using the services of a database that supports XA, or a
 * JMS provider may choose to implement this functionality from scratch. <p/>
 * 
 * @version $Revision: 1.10 $
 * @see javax.jms.Session
 * @see javax.jms.QueueSession
 * @see javax.jms.TopicSession
 * @see javax.jms.XASession
 */
public class TransactionContext implements XAResource {

    private static final Log LOG = LogFactory.getLog(TransactionContext.class);

    // XATransactionId -> ArrayList of TransactionContext objects
    private final static ConcurrentHashMap<TransactionId, List<TransactionContext>> ENDED_XA_TRANSACTION_CONTEXTS = new ConcurrentHashMap<TransactionId, List<TransactionContext>>();

    private final ActiveMQConnection connection;
    private final LongSequenceGenerator localTransactionIdGenerator;
    private final ConnectionId connectionId;
    private List<Synchronization> synchronizations;

    // To track XA transactions.
    private Xid associatedXid;
    private TransactionId transactionId;
    private LocalTransactionEventListener localTransactionEventListener;
    private int beforeEndIndex;

    public TransactionContext(ActiveMQConnection connection) {
        this.connection = connection;
        this.localTransactionIdGenerator = connection.getLocalTransactionIdGenerator();
        this.connectionId = connection.getConnectionInfo().getConnectionId();
    }

    public boolean isInXATransaction() {
        return transactionId != null && transactionId.isXATransaction();
    }

    public boolean isInLocalTransaction() {
        return transactionId != null && transactionId.isLocalTransaction();
    }

    public boolean isInTransaction() {
        return transactionId != null;
    }
    
    /**
     * @return Returns the localTransactionEventListener.
     */
    public LocalTransactionEventListener getLocalTransactionEventListener() {
        return localTransactionEventListener;
    }

    /**
     * Used by the resource adapter to listen to transaction events.
     * 
     * @param localTransactionEventListener The localTransactionEventListener to
     *                set.
     */
    public void setLocalTransactionEventListener(LocalTransactionEventListener localTransactionEventListener) {
        this.localTransactionEventListener = localTransactionEventListener;
    }

    // ///////////////////////////////////////////////////////////
    //
    // Methods that work with the Synchronization objects registered with
    // the transaction.
    //
    // ///////////////////////////////////////////////////////////

    public void addSynchronization(Synchronization s) {
        if (synchronizations == null) {
            synchronizations = new ArrayList<Synchronization>(10);
        }
        synchronizations.add(s);
    }

    private void afterRollback() throws JMSException {
        if (synchronizations == null) {
            return;
        }

        int size = synchronizations.size();
        try {
            for (int i = 0; i < size; i++) {
                synchronizations.get(i).afterRollback();
            }
        } catch (JMSException e) {
            throw e;
        } catch (Throwable e) {
            throw JMSExceptionSupport.create(e);
        } finally {
            synchronizations = null;
        }
    }

    private void afterCommit() throws JMSException {
        if (synchronizations == null) {
            return;
        }

        int size = synchronizations.size();
        try {
            for (int i = 0; i < size; i++) {
                synchronizations.get(i).afterCommit();
            }
        } catch (JMSException e) {
            throw e;
        } catch (Throwable e) {
            throw JMSExceptionSupport.create(e);
        } finally {
        	synchronizations = null;
        }
    }

    private void beforeEnd() throws JMSException {
        if (synchronizations == null) {
            return;
        }

        int size = synchronizations.size();
        try {
            for (;beforeEndIndex < size;) {
                synchronizations.get(beforeEndIndex++).beforeEnd();
            }
        } catch (JMSException e) {
            throw e;
        } catch (Throwable e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    // ///////////////////////////////////////////////////////////
    //
    // Local transaction interface.
    //
    // ///////////////////////////////////////////////////////////

    /**
     * Start a local transaction.
     * @throws javax.jms.JMSException on internal error
     */
    public void begin() throws JMSException {

        if (isInXATransaction()) {
            throw new TransactionInProgressException("Cannot start local transaction.  XA transaction is already in progress.");
        }
        
        if (transactionId == null) {
            synchronizations = null;
            beforeEndIndex = 0;
            this.transactionId = new LocalTransactionId(connectionId, localTransactionIdGenerator.getNextSequenceId());
            TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.BEGIN);
            this.connection.ensureConnectionInfoSent();
            this.connection.asyncSendPacket(info);

            // Notify the listener that the tx was started.
            if (localTransactionEventListener != null) {
                localTransactionEventListener.beginEvent();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Begin:" + transactionId);
            }
        }
        
    }

    /**
     * Rolls back any work done in this transaction and releases any locks
     * currently held.
     * 
     * @throws JMSException if the JMS provider fails to roll back the
     *                 transaction due to some internal error.
     * @throws javax.jms.IllegalStateException if the method is not called by a
     *                 transacted session.
     */
    public void rollback() throws JMSException {
        if (isInXATransaction()) {
            throw new TransactionInProgressException("Cannot rollback() if an XA transaction is already in progress ");
        }
        
        try {
            beforeEnd();
        } catch (TransactionRolledBackException canOcurrOnFailover) {
            LOG.warn("rollback processing error", canOcurrOnFailover);
        }
        if (transactionId != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Rollback: "  + transactionId
                + " syncCount: " 
                + (synchronizations != null ? synchronizations.size() : 0));
            }

            TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.ROLLBACK);
            this.transactionId = null;
            //make this synchronous - see https://issues.apache.org/activemq/browse/AMQ-2364
            this.connection.syncSendPacket(info);
            // Notify the listener that the tx was rolled back
            if (localTransactionEventListener != null) {
                localTransactionEventListener.rollbackEvent();
            }
        }

        afterRollback();
    }

    /**
     * Commits all work done in this transaction and releases any locks
     * currently held.
     * 
     * @throws JMSException if the JMS provider fails to commit the transaction
     *                 due to some internal error.
     * @throws javax.jms.IllegalStateException if the method is not called by a
     *                 transacted session.
     */
    public void commit() throws JMSException {
        if (isInXATransaction()) {
            throw new TransactionInProgressException("Cannot commit() if an XA transaction is already in progress ");
        }
        
        try {
            beforeEnd();
        } catch (JMSException e) {
            rollback();
            throw e;
        }

        // Only send commit if the transaction was started.
        if (transactionId != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Commit: "  + transactionId
                        + " syncCount: " 
                        + (synchronizations != null ? synchronizations.size() : 0));
            }

            TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.COMMIT_ONE_PHASE);
            this.transactionId = null;
            // Notify the listener that the tx was committed back
            try {
                syncSendPacketWithInterruptionHandling(info);
                if (localTransactionEventListener != null) {
                    localTransactionEventListener.commitEvent();
                }
                afterCommit();
            } catch (JMSException cause) {
                LOG.info("commit failed for transaction " + info.getTransactionId(), cause);
                if (localTransactionEventListener != null) {
                    localTransactionEventListener.rollbackEvent();
                }
                afterRollback();
                throw cause;
            }
            
        }
    }

    // ///////////////////////////////////////////////////////////
    //
    // XAResource Implementation
    //
    // ///////////////////////////////////////////////////////////
    /**
     * Associates a transaction with the resource.
     */
    public void start(Xid xid, int flags) throws XAException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Start: " + xid);
        }
        if (isInLocalTransaction()) {
            throw new XAException(XAException.XAER_PROTO);
        }
        // Are we already associated?
        if (associatedXid != null) {
            throw new XAException(XAException.XAER_PROTO);
        }

        // if ((flags & TMJOIN) == TMJOIN) {
        // // TODO: verify that the server has seen the xid
        // }
        // if ((flags & TMJOIN) == TMRESUME) {
        // // TODO: verify that the xid was suspended.
        // }

        // associate
        synchronizations = null;
        beforeEndIndex = 0;
        setXid(xid);
    }

    /**
     * @return connectionId for connection
     */
    private ConnectionId getConnectionId() {
        return connection.getConnectionInfo().getConnectionId();
    }

    public void end(Xid xid, int flags) throws XAException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("End: " + xid);
        }
        
        if (isInLocalTransaction()) {
            throw new XAException(XAException.XAER_PROTO);
        }
        
        if ((flags & (TMSUSPEND | TMFAIL)) != 0) {
            // You can only suspend the associated xid.
            if (!equals(associatedXid, xid)) {
                throw new XAException(XAException.XAER_PROTO);
            }

            // TODO: we may want to put the xid in a suspended list.
            try {
                beforeEnd();
            } catch (JMSException e) {
                throw toXAException(e);
            }
            setXid(null);
        } else if ((flags & TMSUCCESS) == TMSUCCESS) {
            // set to null if this is the current xid.
            // otherwise this could be an asynchronous success call
            if (equals(associatedXid, xid)) {
                try {
                    beforeEnd();
                } catch (JMSException e) {
                    throw toXAException(e);
                }
                setXid(null);
            }
        } else {
            throw new XAException(XAException.XAER_INVAL);
        }
    }

    private boolean equals(Xid xid1, Xid xid2) {
        if (xid1 == xid2) {
            return true;
        }
        if (xid1 == null ^ xid2 == null) {
            return false;
        }
        return xid1.getFormatId() == xid2.getFormatId() && Arrays.equals(xid1.getBranchQualifier(), xid2.getBranchQualifier())
               && Arrays.equals(xid1.getGlobalTransactionId(), xid2.getGlobalTransactionId());
    }

    public int prepare(Xid xid) throws XAException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Prepare: " + xid);
        }
        
        // We allow interleaving multiple transactions, so
        // we don't limit prepare to the associated xid.
        XATransactionId x;
        // THIS SHOULD NEVER HAPPEN because end(xid, TMSUCCESS) should have been
        // called first
        if (xid == null || (equals(associatedXid, xid))) {
            throw new XAException(XAException.XAER_PROTO);
        } else {
            // TODO: cache the known xids so we don't keep recreating this one??
            x = new XATransactionId(xid);
        }

        try {
            TransactionInfo info = new TransactionInfo(getConnectionId(), x, TransactionInfo.PREPARE);

            // Find out if the server wants to commit or rollback.
            IntegerResponse response = (IntegerResponse)syncSendPacketWithInterruptionHandling(info);
            if (XAResource.XA_RDONLY == response.getResult()) {
                // transaction stops now, may be syncs that need a callback
                List<TransactionContext> l = ENDED_XA_TRANSACTION_CONTEXTS.remove(x);
                if (l != null && !l.isEmpty()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("firing afterCommit callbacks on XA_RDONLY from prepare: " + xid);
                    }
                    for (TransactionContext ctx : l) {
                        ctx.afterCommit();
                    }
                }
            }
            return response.getResult();

        } catch (JMSException e) {
            throw toXAException(e);
        }
    }

    public void rollback(Xid xid) throws XAException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Rollback: " + xid);
        }
        
        // We allow interleaving multiple transactions, so
        // we don't limit rollback to the associated xid.
        XATransactionId x;
        if (xid == null) {
            throw new XAException(XAException.XAER_PROTO);
        }
        if (equals(associatedXid, xid)) {
            // I think this can happen even without an end(xid) call. Need to
            // check spec.
            x = (XATransactionId)transactionId;
        } else {
            x = new XATransactionId(xid);
        }

        try {
            this.connection.checkClosedOrFailed();
            this.connection.ensureConnectionInfoSent();

            // Let the server know that the tx is rollback.
            TransactionInfo info = new TransactionInfo(getConnectionId(), x, TransactionInfo.ROLLBACK);
            syncSendPacketWithInterruptionHandling(info);

            List<TransactionContext> l = ENDED_XA_TRANSACTION_CONTEXTS.remove(x);
            if (l != null && !l.isEmpty()) {
                for (TransactionContext ctx : l) {
                    ctx.afterRollback();
                }
            }

        } catch (JMSException e) {
            throw toXAException(e);
        }
    }

    // XAResource interface
    public void commit(Xid xid, boolean onePhase) throws XAException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Commit: " + xid);
        }
        
        // We allow interleaving multiple transactions, so
        // we don't limit commit to the associated xid.
        XATransactionId x;
        if (xid == null || (equals(associatedXid, xid))) {
            // should never happen, end(xid,TMSUCCESS) must have been previously
            // called
            throw new XAException(XAException.XAER_PROTO);
        } else {
            x = new XATransactionId(xid);
        }

        try {
            this.connection.checkClosedOrFailed();
            this.connection.ensureConnectionInfoSent();

            // Notify the server that the tx was committed back
            TransactionInfo info = new TransactionInfo(getConnectionId(), x, onePhase ? TransactionInfo.COMMIT_ONE_PHASE : TransactionInfo.COMMIT_TWO_PHASE);

            syncSendPacketWithInterruptionHandling(info);

            List<TransactionContext> l = ENDED_XA_TRANSACTION_CONTEXTS.remove(x);
            if (l != null && !l.isEmpty()) {
                for (TransactionContext ctx : l) {
                    ctx.afterCommit();
                }
            }

        } catch (JMSException e) {
            LOG.warn("commit of: " + x + " failed with: " + e, e);
            List<TransactionContext> l = ENDED_XA_TRANSACTION_CONTEXTS.remove(x);
            if (l != null && !l.isEmpty()) {
                for (TransactionContext ctx : l) {
                    try {
                        ctx.afterRollback();
                    } catch (Throwable ignored) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("failed to firing afterRollback callbacks commit failure, txid: " + x + ", context: " + ctx, ignored);
                        }
                    }
                }
            }
            throw toXAException(e);
        }

    }

    public void forget(Xid xid) throws XAException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Forget: " + xid);
        }
        
        // We allow interleaving multiple transactions, so
        // we don't limit forget to the associated xid.
        XATransactionId x;
        if (xid == null) {
            throw new XAException(XAException.XAER_PROTO);
        }
        if (equals(associatedXid, xid)) {
            // TODO determine if this can happen... I think not.
            x = (XATransactionId)transactionId;
        } else {
            x = new XATransactionId(xid);
        }

        TransactionInfo info = new TransactionInfo(getConnectionId(), x, TransactionInfo.FORGET);

        try {
            // Tell the server to forget the transaction.
            syncSendPacketWithInterruptionHandling(info);
        } catch (JMSException e) {
            throw toXAException(e);
        }
    }

    public boolean isSameRM(XAResource xaResource) throws XAException {
        if (xaResource == null) {
            return false;
        }
        if (!(xaResource instanceof TransactionContext)) {
            return false;
        }
        TransactionContext xar = (TransactionContext)xaResource;
        try {
            return getResourceManagerId().equals(xar.getResourceManagerId());
        } catch (Throwable e) {
            throw (XAException)new XAException("Could not get resource manager id.").initCause(e);
        }
    }

    public Xid[] recover(int flag) throws XAException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Recover: " + flag);
        }
        
        TransactionInfo info = new TransactionInfo(getConnectionId(), null, TransactionInfo.RECOVER);
        try {
            this.connection.checkClosedOrFailed();
            this.connection.ensureConnectionInfoSent();

            DataArrayResponse receipt = (DataArrayResponse)this.connection.syncSendPacket(info);
            DataStructure[] data = receipt.getData();
            XATransactionId[] answer;
            if (data instanceof XATransactionId[]) {
                answer = (XATransactionId[])data;
            } else {
                answer = new XATransactionId[data.length];
                System.arraycopy(data, 0, answer, 0, data.length);
            }
            return answer;
        } catch (JMSException e) {
            throw toXAException(e);
        }
    }

    public int getTransactionTimeout() throws XAException {
        return 0;
    }

    public boolean setTransactionTimeout(int seconds) throws XAException {
        return false;
    }

    // ///////////////////////////////////////////////////////////
    //
    // Helper methods.
    //
    // ///////////////////////////////////////////////////////////
    private String getResourceManagerId() throws JMSException {
        return this.connection.getResourceManagerId();
    }

    private void setXid(Xid xid) throws XAException {

        try {
            this.connection.checkClosedOrFailed();
            this.connection.ensureConnectionInfoSent();
        } catch (JMSException e) {
            throw toXAException(e);
        }

        if (xid != null) {
            // associate
            associatedXid = xid;
            transactionId = new XATransactionId(xid);

            TransactionInfo info = new TransactionInfo(connectionId, transactionId, TransactionInfo.BEGIN);
            try {
                this.connection.asyncSendPacket(info);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Started XA transaction: " + transactionId);
                }
            } catch (JMSException e) {
                throw toXAException(e);
            }

        } else {

            if (transactionId != null) {
                TransactionInfo info = new TransactionInfo(connectionId, transactionId, TransactionInfo.END);
                try {
                    syncSendPacketWithInterruptionHandling(info);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Ended XA transaction: " + transactionId);
                    }
                } catch (JMSException e) {
                    throw toXAException(e);
                }

                // Add our self to the list of contexts that are interested in
                // post commit/rollback events.
                List<TransactionContext> l = ENDED_XA_TRANSACTION_CONTEXTS.get(transactionId);
                if (l == null) {
                    l = new ArrayList<TransactionContext>(3);
                    ENDED_XA_TRANSACTION_CONTEXTS.put(transactionId, l);
                    l.add(this);
                } else if (!l.contains(this)) {
                    l.add(this);
                }
            }

            // dis-associate
            associatedXid = null;
            transactionId = null;
        }
    }

    /**
     * Sends the given command. Also sends the command in case of interruption,
     * so that important commands like rollback and commit are never interrupted.
     * If interruption occurred, set the interruption state of the current 
     * after performing the action again. 
     * 
     * @return the response
     */
    private Response syncSendPacketWithInterruptionHandling(Command command) throws JMSException {
    	try {
			return this.connection.syncSendPacket(command);
		} catch (JMSException e) {
			if (e.getLinkedException() instanceof InterruptedIOException) {
				try {
					Thread.interrupted();
					return this.connection.syncSendPacket(command);
				} finally {
					Thread.currentThread().interrupt();
				}				
			}
			
			throw e;
		}
    }

    /**
     * Converts a JMSException from the server to an XAException. if the
     * JMSException contained a linked XAException that is returned instead.
     * 
     * @param e JMSException to convert
     * @return XAException wrapping original exception or its message
     */
    private XAException toXAException(JMSException e) {
        if (e.getCause() != null && e.getCause() instanceof XAException) {
            XAException original = (XAException)e.getCause();
            XAException xae = new XAException(original.getMessage());
            xae.errorCode = original.errorCode;
            xae.initCause(original);
            return xae;
        }

        XAException xae = new XAException(e.getMessage());
        xae.errorCode = XAException.XAER_RMFAIL;
        xae.initCause(e);
        return xae;
    }

    public ActiveMQConnection getConnection() {
        return connection;
    }

    public void cleanup() {
        associatedXid = null;
        transactionId = null;
    }
}
