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

import org.apache.activemq.broker.TransactionBroker;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.TransactionStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.4 $
 */
public class XATransaction extends Transaction {

    private static final Log log = LogFactory.getLog(XATransaction.class);

    private final TransactionStore transactionStore;
    private final XATransactionId xid;
    private final TransactionBroker broker;

    public XATransaction(TransactionStore transactionStore, XATransactionId xid, TransactionBroker broker) {
        this.transactionStore = transactionStore;
        this.xid = xid;
        this.broker = broker;
    }

    public void commit(boolean onePhase) throws XAException, IOException {
        if (log.isDebugEnabled())
            log.debug("XA Transaction commit: " + xid);

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
            transactionStore.commit(getTransactionId(), false);
            doPostCommit();
            break;
        case PREPARED_STATE:
            // 2 phase commit, work done.
            // We would record commit here.
            setStateFinished();
            transactionStore.commit(getTransactionId(), true);
            doPostCommit();
            break;
        default:
            illegalStateTransition("commit");
        }
    }

    private void illegalStateTransition(String callName) throws XAException {
        XAException xae = new XAException("Cannot call " + callName + " now.");
        xae.errorCode = XAException.XAER_PROTO;
        throw xae;
    }

    private void checkForPreparedState(boolean onePhase) throws XAException {
        if (!onePhase) {
            XAException xae = new XAException("Cannot do 2 phase commit if the transaction has not been prepared.");
            xae.errorCode = XAException.XAER_PROTO;
            throw xae;
        }
    }

    private void doPrePrepare() throws XAException, IOException {
        try {
            prePrepare();
        } catch (XAException e) {
            throw e;
        } catch (Throwable e) {
            log.warn("PRE-PREPARE FAILED: ", e);
            rollback();
            XAException xae = new XAException("PRE-PREPARE FAILED: Transaction rolled back.");
            xae.errorCode = XAException.XA_RBOTHER;
            xae.initCause(e);
            throw xae;
        }
    }

    private void doPostCommit() throws XAException {
        try {
            fireAfterCommit();
        } catch (Throwable e) {
            // I guess this could happen. Post commit task failed
            // to execute properly.
            log.warn("POST COMMIT FAILED: ", e);
            XAException xae = new XAException("POST COMMIT FAILED");
            xae.errorCode = XAException.XAER_RMERR;
            xae.initCause(e);
            throw xae;
        }
    }

    public void rollback() throws XAException, IOException {

        if (log.isDebugEnabled())
            log.debug("XA Transaction rollback: " + xid);

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
        }

    }

    private void doPostRollback() throws XAException {
        try {
            fireAfterRollback();
        } catch (Throwable e) {
            // I guess this could happen. Post commit task failed
            // to execute properly.
            log.warn("POST ROLLBACK FAILED: ", e);
            XAException xae = new XAException("POST ROLLBACK FAILED");
            xae.errorCode = XAException.XAER_RMERR;
            xae.initCause(e);
            throw xae;
        }
    }

    public int prepare() throws XAException, IOException {
        if (log.isDebugEnabled())
            log.debug("XA Transaction prepare: " + xid);

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

    public TransactionId getTransactionId() {
        return xid;
    }
}
