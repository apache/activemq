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
package org.apache.activemq.transaction;

import java.io.IOException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import org.apache.activemq.TransactionContext;
import org.apache.activemq.broker.TransactionBroker;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class XATransaction extends Transaction {

    private static final Logger LOG = LoggerFactory.getLogger(XATransaction.class);

    private final TransactionStore transactionStore;
    private final XATransactionId xid;
    private final TransactionBroker broker;
    private final ConnectionId connectionId;

    public XATransaction(TransactionStore transactionStore, XATransactionId xid, TransactionBroker broker, ConnectionId connectionId) {
        this.transactionStore = transactionStore;
        this.xid = xid;
        this.broker = broker;
        this.connectionId = connectionId;
        if (LOG.isDebugEnabled()) {
            LOG.debug("XA Transaction new/begin : " + xid);
        }
    }

    @Override
    public void commit(boolean onePhase) throws XAException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("XA Transaction commit onePhase:" + onePhase + ", xid: " + xid);
        }

        switch (getState()) {
        case START_STATE:
            // 1 phase commit, no work done.
            checkForPreparedState(onePhase);
            setStateFinished();
            break;
        case IN_USE_STATE:
            // 1 phase commit, work done.
            checkForPreparedState(onePhase);
            doPrePrepare();
            setStateFinished();
            storeCommit(getTransactionId(), false, preCommitTask, postCommitTask);
            break;
        case PREPARED_STATE:
            // 2 phase commit, work done.
            // We would record commit here.
            storeCommit(getTransactionId(), true, null /* done post prepare call */, postCommitTask);
            setStateFinished();
            break;
        default:
            illegalStateTransition("commit");
        }
    }

    private void storeCommit(TransactionId txid, boolean wasPrepared, Runnable preCommit,Runnable postCommit)
            throws XAException, IOException {
        try {
            transactionStore.commit(getTransactionId(), wasPrepared, preCommitTask, postCommitTask);
            waitPostCommitDone(postCommitTask);
        } catch (XAException xae) {
            throw xae;
        } catch (Throwable t) {
            LOG.warn("Store COMMIT FAILED: " + txid, t);
            XAException xae = null;
            if (wasPrepared) {
                // report and await outcome
                xae = newXAException("STORE COMMIT FAILED: " + t.getMessage(), XAException.XA_RETRY);
                // fire rollback syncs to revert
                doPostRollback();
            } else {
                try {
                    rollback();
                    xae = newXAException("STORE COMMIT FAILED: Transaction rolled back", XAException.XA_RBCOMMFAIL);
                } catch (Throwable e) {
                    xae = newXAException("STORE COMMIT FAILED: " + t.getMessage() +". Rolled failed:"  + e.getMessage(), XAException.XA_RBINTEGRITY);
                }
            }
            xae.initCause(t);
            throw xae;
        }
    }

    private void illegalStateTransition(String callName) throws XAException {
        XAException xae = newXAException("Cannot call " + callName + " now.", XAException.XAER_PROTO);
        throw xae;
    }

    private void checkForPreparedState(boolean onePhase) throws XAException {
        if (!onePhase) {
            XAException xae = newXAException("Cannot do 2 phase commit if the transaction has not been prepared", XAException.XAER_PROTO);
            throw xae;
        }
    }

    private void doPrePrepare() throws XAException, IOException {
        try {
            prePrepare();
        } catch (XAException e) {
            throw e;
        } catch (Throwable e) {
            LOG.warn("PRE-PREPARE FAILED: ", e);
            rollback();
            XAException xae = newXAException("PRE-PREPARE FAILED: Transaction rolled back", XAException.XA_RBOTHER);
            xae.initCause(e);
            throw xae;
        }
    }

    @Override
    public void rollback() throws XAException, IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("XA Transaction rollback: " + xid);
        }

        switch (getState()) {
        case START_STATE:
            // 1 phase rollback no work done.
            setStateFinished();
            break;
        case IN_USE_STATE:
            // 1 phase rollback work done.
            setStateFinished();
            transactionStore.rollback(getTransactionId());
            doPostRollback();
            break;
        case PREPARED_STATE:
            // 2 phase rollback work done.
            setStateFinished();
            transactionStore.rollback(getTransactionId());
            doPostRollback();
            break;
        case FINISHED_STATE:
            // failure to commit
            transactionStore.rollback(getTransactionId());
            doPostRollback();
            break;
        default:
            throw newXAException("Invalid state: " + getState(), XAException.XA_RBPROTO);
        }

    }

    private void doPostRollback() throws XAException {
        try {
            fireAfterRollback();
        } catch (Throwable e) {
            // I guess this could happen. Post commit task failed
            // to execute properly.
            LOG.warn("POST ROLLBACK FAILED: ", e);
            XAException xae = newXAException("POST ROLLBACK FAILED", XAException.XAER_RMERR);
            xae.initCause(e);
            throw xae;
        }
    }

    @Override
    public int prepare() throws XAException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("XA Transaction prepare: " + xid);
        }

        switch (getState()) {
        case START_STATE:
            // No work done.. no commit/rollback needed.
            setStateFinished();
            return XAResource.XA_RDONLY;
        case IN_USE_STATE:
            // We would record prepare here.
            doPrePrepare();
            setState(Transaction.PREPARED_STATE);
            transactionStore.prepare(getTransactionId());
            preCommitTask.run();
            return XAResource.XA_OK;
        default:
            illegalStateTransition("prepare");
            return XAResource.XA_RDONLY;
        }
    }

    private void setStateFinished() {
        setState(Transaction.FINISHED_STATE);
        broker.removeTransaction(xid);
    }

    public ConnectionId getConnectionId() {
        return connectionId;
    }

    @Override
    public TransactionId getTransactionId() {
        return xid;
    }
    
    @Override
    public Logger getLog() {
        return LOG;
    }

    public XATransactionId getXid() {
        return xid;
    }
}
