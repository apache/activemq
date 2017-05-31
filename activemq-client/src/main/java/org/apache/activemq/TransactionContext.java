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
import java.util.HashMap;
import java.util.List;

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
import org.apache.activemq.util.XASupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * @see javax.jms.Session
 * @see javax.jms.QueueSession
 * @see javax.jms.TopicSession
 * @see javax.jms.XASession
 */
public class TransactionContext implements XAResource {

    public static final String xaErrorCodeMarker = "xaErrorCode:";
    private static final Logger LOG = LoggerFactory.getLogger(TransactionContext.class);

    // XATransactionId -> ArrayList of TransactionContext objects
    private final static HashMap<TransactionId, List<TransactionContext>> ENDED_XA_TRANSACTION_CONTEXTS =
            new HashMap<TransactionId, List<TransactionContext>>();

    private ActiveMQConnection connection;
    private final LongSequenceGenerator localTransactionIdGenerator;
    private List<Synchronization> synchronizations;

    // To track XA transactions.
    private Xid associatedXid;
    private TransactionId transactionId;
    private LocalTransactionEventListener localTransactionEventListener;
    private int beforeEndIndex;
    private volatile boolean rollbackOnly;

    // for RAR recovery
    public TransactionContext() {
        localTransactionIdGenerator = null;
    }

    public TransactionContext(ActiveMQConnection connection) {
        this.connection = connection;
        this.localTransactionIdGenerator = connection.getLocalTransactionIdGenerator();
    }

    public boolean isInXATransaction() {
        if (transactionId != null && transactionId.isXATransaction()) {
            return true;
        } else {
            synchronized(ENDED_XA_TRANSACTION_CONTEXTS) {
                for(List<TransactionContext> transactions : ENDED_XA_TRANSACTION_CONTEXTS.values()) {
                      if (transactions.contains(this)) {
                          return true;
                      }
                }
            }
        }

        return false;
    }

    public void setRollbackOnly(boolean val) {
        rollbackOnly = val;
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

        Throwable firstException = null;
        int size = synchronizations.size();
        for (int i = 0; i < size; i++) {
            try {
                synchronizations.get(i).afterRollback();
            } catch (Throwable t) {
                LOG.debug("Exception from afterRollback on {}", synchronizations.get(i), t);
                if (firstException == null) {
                    firstException = t;
                }
            }
        }
        synchronizations = null;
        if (firstException != null) {
            throw JMSExceptionSupport.create(firstException);
        }
    }

    private void afterCommit() throws JMSException {
        if (synchronizations == null) {
            return;
        }

        Throwable firstException = null;
        int size = synchronizations.size();
        for (int i = 0; i < size; i++) {
            try {
                synchronizations.get(i).afterCommit();
            } catch (Throwable t) {
                LOG.debug("Exception from afterCommit on {}", synchronizations.get(i), t);
                if (firstException == null) {
                    firstException = t;
                }
            }
        }
        synchronizations = null;
        if (firstException != null) {
            throw JMSExceptionSupport.create(firstException);
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
            setRollbackOnly(false);
            this.transactionId = new LocalTransactionId(getConnectionId(), localTransactionIdGenerator.getNextSequenceId());
            TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.BEGIN);
            this.connection.ensureConnectionInfoSent();
            this.connection.asyncSendPacket(info);

            // Notify the listener that the tx was started.
            if (localTransactionEventListener != null) {
                localTransactionEventListener.beginEvent();
            }

            LOG.debug("Begin:{}", transactionId);
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
            LOG.debug("Rollback: {} syncCount: {}",
                transactionId, (synchronizations != null ? synchronizations.size() : 0));

            TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.ROLLBACK);
            this.transactionId = null;
            //make this synchronous - see https://issues.apache.org/activemq/browse/AMQ-2364
            this.connection.syncSendPacket(info, this.connection.isClosing() ? this.connection.getCloseTimeout() : 0);
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

        if (transactionId != null && rollbackOnly) {
            final String message = "Commit of " + transactionId + "  failed due to rollback only request; typically due to failover with pending acks";
            try {
                rollback();
            } finally {
                LOG.warn(message);
                throw new TransactionRolledBackException(message);
            }
        }

        // Only send commit if the transaction was started.
        if (transactionId != null) {
            LOG.debug("Commit: {} syncCount: {}",
                transactionId, (synchronizations != null ? synchronizations.size() : 0));

            TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.COMMIT_ONE_PHASE);
            this.transactionId = null;
            // Notify the listener that the tx was committed back
            try {
                this.connection.syncSendPacket(info);
                if (localTransactionEventListener != null) {
                    localTransactionEventListener.commitEvent();
                }
                afterCommit();
            } catch (JMSException cause) {
                LOG.info("commit failed for transaction {}", info.getTransactionId(), cause);
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
    @Override
    public void start(Xid xid, int flags) throws XAException {

        LOG.debug("Start: {}, flags: {}", xid, XASupport.toString(flags));

        if (isInLocalTransaction()) {
            throw new XAException(XAException.XAER_PROTO);
        }
        // Are we already associated?
        if (associatedXid != null) {
            throw new XAException(XAException.XAER_PROTO);
        }

        // if ((flags & TMJOIN) == TMJOIN) {
        // TODO: verify that the server has seen the xid
        // // }
        // if ((flags & TMRESUME) == TMRESUME) {
        // // TODO: verify that the xid was suspended.
        // }

        // associate
        synchronizations = null;
        beforeEndIndex = 0;
        setRollbackOnly(false);
        setXid(xid);
    }

    /**
     * @return connectionId for connection
     */
    private ConnectionId getConnectionId() {
        return connection.getConnectionInfo().getConnectionId();
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {

        LOG.debug("End: {}, flags: {}", xid, XASupport.toString(flags));

        if (isInLocalTransaction()) {
            throw new XAException(XAException.XAER_PROTO);
        }

        if ((flags & (TMSUSPEND | TMFAIL)) != 0) {
            // You can only suspend the associated xid.
            if (!equals(associatedXid, xid)) {
                throw new XAException(XAException.XAER_PROTO);
            }
            invokeBeforeEnd();
        } else if ((flags & TMSUCCESS) == TMSUCCESS) {
            // set to null if this is the current xid.
            // otherwise this could be an asynchronous success call
            if (equals(associatedXid, xid)) {
                invokeBeforeEnd();
            }
        } else {
            throw new XAException(XAException.XAER_INVAL);
        }
    }

    private void invokeBeforeEnd() throws XAException {
        boolean throwingException = false;
        try {
            beforeEnd();
        } catch (JMSException e) {
            throwingException = true;
            throw toXAException(e);
        } finally {
            try {
                setXid(null);
            } catch (XAException ignoreIfWillMask){
                if (!throwingException) {
                    throw ignoreIfWillMask;
                }
            }
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

    @Override
    public int prepare(Xid xid) throws XAException {
        LOG.debug("Prepare: {}", xid);

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

        if (rollbackOnly) {
            LOG.warn("prepare of: " + x + " failed because it was marked rollback only; typically due to failover with pending acks");
            throw new XAException(XAException.XA_RBINTEGRITY);
        }

        try {
            TransactionInfo info = new TransactionInfo(getConnectionId(), x, TransactionInfo.PREPARE);

            // Find out if the server wants to commit or rollback.
            IntegerResponse response = (IntegerResponse)this.connection.syncSendPacket(info);
            if (XAResource.XA_RDONLY == response.getResult()) {
                // transaction stops now, may be syncs that need a callback
                List<TransactionContext> l;
                synchronized(ENDED_XA_TRANSACTION_CONTEXTS) {
                    l = ENDED_XA_TRANSACTION_CONTEXTS.remove(x);
                }
                // After commit may be expensive and can deadlock, do it outside global synch block
                // No risk for concurrent updates as we own the list now
                if (l != null) {
                    if(! l.isEmpty()) {
                        LOG.debug("firing afterCommit callbacks on XA_RDONLY from prepare: {}", xid);
                        for (TransactionContext ctx : l) {
                            ctx.afterCommit();
                        }
                    }
                }
            }
            return response.getResult();

        } catch (JMSException e) {
            LOG.warn("prepare of: " + x + " failed with: " + e, e);
            List<TransactionContext> l;
            synchronized(ENDED_XA_TRANSACTION_CONTEXTS) {
                l = ENDED_XA_TRANSACTION_CONTEXTS.remove(x);
            }
            // After rollback may be expensive and can deadlock, do it outside global synch block
            // No risk for concurrent updates as we own the list now
            if (l != null) {
                for (TransactionContext ctx : l) {
                    try {
                        ctx.afterRollback();
                    } catch (Throwable ignored) {
                        LOG.debug("failed to firing afterRollback callbacks on prepare " +
                                  "failure, txid: {}, context: {}", x, ctx, ignored);
                    }
                }
            }
            throw toXAException(e);
        }
    }

    @Override
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
            this.connection.syncSendPacket(info);

            List<TransactionContext> l;
            synchronized(ENDED_XA_TRANSACTION_CONTEXTS) {
                l = ENDED_XA_TRANSACTION_CONTEXTS.remove(x);
            }
            // After rollback may be expensive and can deadlock, do it outside global synch block
            // No risk for concurrent updates as we own the list now
            if (l != null) {
                for (TransactionContext ctx : l) {
                    ctx.afterRollback();
                }                  
            }
        } catch (JMSException e) {
            throw toXAException(e);
        }
    }

    // XAResource interface
    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {

        LOG.debug("Commit: {}, onePhase={}", xid, onePhase);

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

        if (rollbackOnly) {
             LOG.warn("commit of: " + x + " failed because it was marked rollback only; typically due to failover with pending acks");
             throw new XAException(XAException.XA_RBINTEGRITY);
         }

        try {
            this.connection.checkClosedOrFailed();
            this.connection.ensureConnectionInfoSent();

            // Notify the server that the tx was committed back
            TransactionInfo info = new TransactionInfo(getConnectionId(), x, onePhase ? TransactionInfo.COMMIT_ONE_PHASE : TransactionInfo.COMMIT_TWO_PHASE);

            this.connection.syncSendPacket(info);

            List<TransactionContext> l;
            synchronized(ENDED_XA_TRANSACTION_CONTEXTS) {
                l = ENDED_XA_TRANSACTION_CONTEXTS.remove(x);
            }
            // After commit may be expensive and can deadlock, do it outside global synch block
            // No risk for concurrent updates as we own the list now
            if (l != null) {
                for (TransactionContext ctx : l) {
                    try {
                        ctx.afterCommit();
                    } catch (Exception ignored) {
                        LOG.debug("ignoring exception from after completion on ended transaction: {}", ignored, ignored);
                    }
                }
            }

        } catch (JMSException e) {
            LOG.warn("commit of: " + x + " failed with: " + e, e);
            if (onePhase) {
                List<TransactionContext> l;
                synchronized(ENDED_XA_TRANSACTION_CONTEXTS) {
                    l = ENDED_XA_TRANSACTION_CONTEXTS.remove(x);
                }
                // After rollback may be expensive and can deadlock, do it outside global synch block
                // No risk for concurrent updates as we own the list now
                if (l != null) {
                    for (TransactionContext ctx : l) {
                        try {
                            ctx.afterRollback();
                        } catch (Throwable ignored) {
                            LOG.debug("failed to firing afterRollback callbacks commit failure, txid: {}, context: {}", x, ctx, ignored);
                        }
                    }
                }
            }
            throw toXAException(e);
        }
    }

    @Override
    public void forget(Xid xid) throws XAException {
        LOG.debug("Forget: {}", xid);

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
        synchronized(ENDED_XA_TRANSACTION_CONTEXTS) {
            ENDED_XA_TRANSACTION_CONTEXTS.remove(x);
        }
    }

    @Override
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

    @Override
    public Xid[] recover(int flag) throws XAException {
        LOG.debug("recover({})", flag);
        XATransactionId[] answer;

        if (XAResource.TMNOFLAGS == flag) {
            // signal next in cursor scan, which for us is always the end b/c we don't maintain any cursor state
            // allows looping scan to complete
            answer = new XATransactionId[0];
        } else {
            TransactionInfo info = new TransactionInfo(getConnectionId(), null, TransactionInfo.RECOVER);
            try {
                this.connection.checkClosedOrFailed();
                this.connection.ensureConnectionInfoSent();

                DataArrayResponse receipt = (DataArrayResponse) this.connection.syncSendPacket(info);
                DataStructure[] data = receipt.getData();
                if (data instanceof XATransactionId[]) {
                    answer = (XATransactionId[]) data;
                } else {
                    answer = new XATransactionId[data.length];
                    System.arraycopy(data, 0, answer, 0, data.length);
                }
            } catch (JMSException e) {
                throw toXAException(e);
            }
        }
        LOG.debug("recover({})={}", flag, answer);
        return answer;
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return 0;
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        return false;
    }

    // ///////////////////////////////////////////////////////////
    //
    // Helper methods.
    //
    // ///////////////////////////////////////////////////////////
    protected String getResourceManagerId() throws JMSException {
        return this.connection.getResourceManagerId();
    }

    private void setXid(Xid xid) throws XAException {

        try {
            this.connection.checkClosedOrFailed();
            this.connection.ensureConnectionInfoSent();
        } catch (JMSException e) {
            disassociate();
            throw toXAException(e);
        }

        if (xid != null) {
            // associate
            associatedXid = xid;
            transactionId = new XATransactionId(xid);

            TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.BEGIN);
            try {
                this.connection.asyncSendPacket(info);
                LOG.debug("{} started XA transaction {}", this, transactionId);
            } catch (JMSException e) {
                disassociate();
                throw toXAException(e);
            }

        } else {

            if (transactionId != null) {
                TransactionInfo info = new TransactionInfo(getConnectionId(), transactionId, TransactionInfo.END);
                try {
                    this.connection.syncSendPacket(info);
                    LOG.debug("{} ended XA transaction {}", this, transactionId);
                } catch (JMSException e) {
                    disassociate();
                    throw toXAException(e);
                }

                // Add our self to the list of contexts that are interested in
                // post commit/rollback events.
                List<TransactionContext> l;
                synchronized(ENDED_XA_TRANSACTION_CONTEXTS) {
                    l = ENDED_XA_TRANSACTION_CONTEXTS.get(transactionId);
                    if (l == null) {
                        l = new ArrayList<TransactionContext>(3);
                        ENDED_XA_TRANSACTION_CONTEXTS.put(transactionId, l);
                    }
                    if (!l.contains(this)) {
                        l.add(this);
                    }
                }
            }

            disassociate();
        }
    }

    private void disassociate() {
         // dis-associate
         associatedXid = null;
         transactionId = null;
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
            if (xae.errorCode == XA_OK) {
                // detail not unmarshalled see: org.apache.activemq.openwire.v1.BaseDataStreamMarshaller.createThrowable
                xae.errorCode = parseFromMessageOr(original.getMessage(), XAException.XAER_RMERR);
            }
            xae.initCause(original);
            return xae;
        }

        XAException xae = new XAException(e.getMessage());
        xae.errorCode = XAException.XAER_RMFAIL;
        xae.initCause(e);
        return xae;
    }

    private int parseFromMessageOr(String message, int fallbackCode) {
        final String marker = "xaErrorCode:";
        final int index = message.lastIndexOf(marker);
        if (index > -1) {
            try {
                return Integer.parseInt(message.substring(index + marker.length()));
            } catch (Exception ignored) {}
        }
        return fallbackCode;
    }

    public ActiveMQConnection getConnection() {
        return connection;
    }

    // for RAR xa recovery where xaresource connection is per request
    public ActiveMQConnection setConnection(ActiveMQConnection connection) {
        ActiveMQConnection existing = this.connection;
        this.connection = connection;
        return existing;
    }

    public void cleanup() {
        associatedXid = null;
        transactionId = null;
    }

    @Override
    public String toString() {
        return "TransactionContext{" +
                "transactionId=" + transactionId +
                ",connection=" + connection +
                '}';
    }
}
