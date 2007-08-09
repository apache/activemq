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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.jms.TransactionInProgressException;
import javax.jms.TransactionRolledBackException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.DataArrayResponse;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.IntegerResponse;
import org.apache.activemq.command.LocalTransactionId;
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

    static final private Log log = LogFactory.getLog(TransactionContext.class);

    // XATransactionId -> ArrayList of TransactionContext objects
    private static final ConcurrentHashMap endedXATransactionContexts = new ConcurrentHashMap();

    private final ActiveMQConnection connection;
    private final LongSequenceGenerator localTransactionIdGenerator;
    private final ConnectionId connectionId;
    private ArrayList synchornizations;

    // To track XA transactions.
    private Xid associatedXid;
    private TransactionId transactionId;
    private LocalTransactionEventListener localTransactionEventListener;

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
        if (synchornizations == null)
            synchornizations = new ArrayList(10);
        synchornizations.add(s);
    }

    private void afterRollback() throws JMSException {
        if (synchornizations == null)
            return;

        int size = synchornizations.size();
        try {
            for (int i = 0; i < size; i++) {
                ((Synchronization)synchornizations.get(i)).afterRollback();
            }
        } catch (JMSException e) {
            throw e;
        } catch (Throwable e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    private void afterCommit() throws JMSException {
        if (synchornizations == null)
            return;

        int size = synchornizations.size();
        try {
            for (int i = 0; i < size; i++) {
                ((Synchronization)synchornizations.get(i)).afterCommit();
            }
        } catch (JMSException e) {
            throw e;
        } catch (Throwable e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    private void beforeEnd() throws JMSException {
        if (synchornizations == null)
            return;

        int size = synchornizations.size();
        try {
            for (int i = 0; i < size; i++) {
                ((Synchronization)synchornizations.get(i)).beforeEnd();
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
     */
    public void begin() throws JMSException {

        if (isInXATransaction())
            throw new TransactionInProgressException("Cannot start local transaction.  XA transaction is already in progress.");

        if (transactionId == null) {
            synchornizations = null;
            this.transactionId = new LocalTransactionId(connectionId, localTransactionIdGenerator.getNextSequenceId());
            TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.BEGIN);
            this.connection.ensureConnectionInfoSent();
            this.connection.asyncSendPacket(info);

            // Notify the listener that the tx was started.
            if (localTransactionEventListener != null) {
                localTransactionEventListener.beginEvent();
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
        if (isInXATransaction())
            throw new TransactionInProgressException("Cannot rollback() if an XA transaction is already in progress ");

        if (transactionId != null) {
            TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.ROLLBACK);
            this.transactionId = null;
            this.connection.asyncSendPacket(info);
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
     * @throws TransactionRolledBackException if the transaction is rolled back
     *                 due to some internal error during commit.
     * @throws javax.jms.IllegalStateException if the method is not called by a
     *                 transacted session.
     */
    public void commit() throws JMSException {
        if (isInXATransaction())
            throw new TransactionInProgressException("Cannot commit() if an XA transaction is already in progress ");

        beforeEnd();

        // Only send commit if the transaction was started.
        if (transactionId != null) {
            TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.COMMIT_ONE_PHASE);
            this.transactionId = null;
            // Notify the listener that the tx was committed back
            this.connection.syncSendPacket(info);
            if (localTransactionEventListener != null) {
                localTransactionEventListener.commitEvent();
            }
            afterCommit();
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

        if (log.isDebugEnabled())
            log.debug("Start: " + xid);

        if (isInLocalTransaction())
            throw new XAException(XAException.XAER_PROTO);

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
        synchornizations = null;
        setXid(xid);
    }

    /**
     * @return
     */
    private ConnectionId getConnectionId() {
        return connection.getConnectionInfo().getConnectionId();
    }

    public void end(Xid xid, int flags) throws XAException {

        if (log.isDebugEnabled())
            log.debug("End: " + xid);

        if (isInLocalTransaction())
            throw new XAException(XAException.XAER_PROTO);

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
        if (xid1 == xid2)
            return true;
        if (xid1 == null ^ xid2 == null)
            return false;
        return xid1.getFormatId() == xid2.getFormatId() && Arrays.equals(xid1.getBranchQualifier(), xid2.getBranchQualifier())
               && Arrays.equals(xid1.getGlobalTransactionId(), xid2.getGlobalTransactionId());
    }

    public int prepare(Xid xid) throws XAException {
        if (log.isDebugEnabled())
            log.debug("Prepare: " + xid);

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
            IntegerResponse response = (IntegerResponse)this.connection.syncSendPacket(info);
            return response.getResult();

        } catch (JMSException e) {
            throw toXAException(e);
        }
    }

    public void rollback(Xid xid) throws XAException {

        if (log.isDebugEnabled())
            log.debug("Rollback: " + xid);

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
            this.connection.syncSendPacket(info);

            ArrayList l = (ArrayList)endedXATransactionContexts.remove(x);
            if (l != null && !l.isEmpty()) {
                for (Iterator iter = l.iterator(); iter.hasNext();) {
                    TransactionContext ctx = (TransactionContext)iter.next();
                    ctx.afterRollback();
                }
            }

        } catch (JMSException e) {
            throw toXAException(e);
        }
    }

    // XAResource interface
    public void commit(Xid xid, boolean onePhase) throws XAException {

        if (log.isDebugEnabled())
            log.debug("Commit: " + xid);

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

            this.connection.syncSendPacket(info);

            ArrayList l = (ArrayList)endedXATransactionContexts.remove(x);
            if (l != null && !l.isEmpty()) {
                for (Iterator iter = l.iterator(); iter.hasNext();) {
                    TransactionContext ctx = (TransactionContext)iter.next();
                    ctx.afterCommit();
                }
            }

        } catch (JMSException e) {
            throw toXAException(e);
        }

    }

    public void forget(Xid xid) throws XAException {
        if (log.isDebugEnabled())
            log.debug("Forget: " + xid);

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
            this.connection.syncSendPacket(info);
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
        if (log.isDebugEnabled())
            log.debug("Recover: " + flag);

        TransactionInfo info = new TransactionInfo(getConnectionId(), null, TransactionInfo.RECOVER);
        try {
            this.connection.checkClosedOrFailed();
            this.connection.ensureConnectionInfoSent();

            DataArrayResponse receipt = (DataArrayResponse)this.connection.syncSendPacket(info);
            DataStructure[] data = receipt.getData();
            XATransactionId[] answer = null;
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
                if (log.isDebugEnabled())
                    log.debug("Started XA transaction: " + transactionId);
            } catch (JMSException e) {
                throw toXAException(e);
            }

        } else {

            if (transactionId != null) {
                TransactionInfo info = new TransactionInfo(connectionId, transactionId, TransactionInfo.END);
                try {
                    this.connection.syncSendPacket(info);
                    if (log.isDebugEnabled())
                        log.debug("Ended XA transaction: " + transactionId);
                } catch (JMSException e) {
                    throw toXAException(e);
                }

                // Add our self to the list of contexts that are interested in
                // post commit/rollback events.
                ArrayList l = (ArrayList)endedXATransactionContexts.get(transactionId);
                if (l == null) {
                    l = new ArrayList(3);
                    endedXATransactionContexts.put(transactionId, l);
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
     * Converts a JMSException from the server to an XAException. if the
     * JMSException contained a linked XAException that is returned instead.
     * 
     * @param e
     * @return
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
