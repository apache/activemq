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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.3 $
 */
public class LocalTransaction extends Transaction {

    private static final Logger LOG = LoggerFactory.getLogger(LocalTransaction.class);

    private final TransactionStore transactionStore;
    private final LocalTransactionId xid;
    private final ConnectionContext context;

    public LocalTransaction(TransactionStore transactionStore, LocalTransactionId xid, ConnectionContext context) {
        this.transactionStore = transactionStore;
        this.xid = xid;
        this.context = context;
    }

    @Override
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
        // Sync on transaction store to avoid out of order messages in the cursor
        // https://issues.apache.org/activemq/browse/AMQ-2594
        try {
            transactionStore.commit(getTransactionId(), false,preCommitTask, postCommitTask);
            this.waitPostCommitDone(postCommitTask);
        } catch (Throwable t) {
            LOG.warn("Store COMMIT FAILED: ", t);
            rollback();
            XAException xae = new XAException("STORE COMMIT FAILED: Transaction rolled back.");
            xae.errorCode = XAException.XA_RBOTHER;
            xae.initCause(t);
            throw xae;
        }
    }

    @Override
    public void rollback() throws XAException, IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("rollback: "  + xid
                    + " syncCount: " + size());
        }
        setState(Transaction.FINISHED_STATE);
        context.getTransactions().remove(xid);
        // Sync on transaction store to avoid out of order messages in the cursor
        // https://issues.apache.org/activemq/browse/AMQ-2594
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

    @Override
    public int prepare() throws XAException {
        XAException xae = new XAException("Prepare not implemented on Local Transactions.");
        xae.errorCode = XAException.XAER_RMERR;
        throw xae;
    }

    @Override
    public TransactionId getTransactionId() {
        return xid;
    }
    
    @Override
    public Logger getLog() {
        return LOG;
    }
}
