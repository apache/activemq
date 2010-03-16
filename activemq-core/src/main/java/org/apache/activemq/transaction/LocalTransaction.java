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

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.TransactionStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.3 $
 */
public class LocalTransaction extends Transaction {

    private static final Log LOG = LogFactory.getLog(LocalTransaction.class);

    private final TransactionStore transactionStore;
    private final LocalTransactionId xid;
    private final ConnectionContext context;

    public LocalTransaction(TransactionStore transactionStore, LocalTransactionId xid, ConnectionContext context) {
        this.transactionStore = transactionStore;
        this.xid = xid;
        this.context = context;
    }

    public void commit(boolean onePhase) throws XAException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("commit: "  + xid
                    + " syncCount: " + size());
        }
        
        // Get ready for commit.
        try {
            prePrepare();
        } catch (XAException e) {
            throw e;
        } catch (Throwable e) {
            LOG.warn("COMMIT FAILED: ", e);
            rollback();
            // Let them know we rolled back.
            XAException xae = new XAException("COMMIT FAILED: Transaction rolled back.");
            xae.errorCode = XAException.XA_RBOTHER;
            xae.initCause(e);
            throw xae;
        }

        setState(Transaction.FINISHED_STATE);
        context.getTransactions().remove(xid);
        synchronized (transactionStore) {
            transactionStore.commit(getTransactionId(), false);

            try {
                fireAfterCommit();
            } catch (Throwable e) {
                // I guess this could happen. Post commit task failed
                // to execute properly.
                LOG.warn("POST COMMIT FAILED: ", e);
                XAException xae = new XAException("POST COMMIT FAILED");
                xae.errorCode = XAException.XAER_RMERR;
                xae.initCause(e);
                throw xae;
            }
        }
    }

    public void rollback() throws XAException, IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("rollback: "  + xid
                    + " syncCount: " + size());
        }
        setState(Transaction.FINISHED_STATE);
        context.getTransactions().remove(xid);
        synchronized (transactionStore) {
           transactionStore.rollback(getTransactionId());

            try {
                fireAfterRollback();
            } catch (Throwable e) {
                LOG.warn("POST ROLLBACK FAILED: ", e);
                XAException xae = new XAException("POST ROLLBACK FAILED");
                xae.errorCode = XAException.XAER_RMERR;
                xae.initCause(e);
                throw xae;
            }
        }
    }

    public int prepare() throws XAException {
        XAException xae = new XAException("Prepare not implemented on Local Transactions.");
        xae.errorCode = XAException.XAER_RMERR;
        throw xae;
    }

    public TransactionId getTransactionId() {
        return xid;
    }

}
