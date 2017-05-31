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
package org.apache.activemq.ra;

import javax.jms.JMSException;
import javax.resource.ResourceException;
import javax.resource.spi.LocalTransaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to provide a LocalTransaction and XAResource to a JMS session.
 */
public class LocalAndXATransaction implements XAResource, LocalTransaction {
    private static final Logger LOG = LoggerFactory.getLogger(LocalAndXATransaction.class);

    private TransactionContext transactionContext;
    private boolean inManagedTx;

    public LocalAndXATransaction(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    public void setTransactionContext(TransactionContext transactionContext) {
        this.transactionContext = transactionContext;
    }

    public void setInManagedTx(boolean inManagedTx) throws JMSException {
        this.inManagedTx = inManagedTx;
        if (!inManagedTx) {
            transactionContext.cleanup();
        }
    }

    public void begin() throws ResourceException {
        try {
            transactionContext.begin();
            setInManagedTx(true);
        } catch (JMSException e) {
            throw new ResourceException("begin failed.", e);
        }
    }

    public void commit() throws ResourceException {
        try {
            transactionContext.commit();
        } catch (JMSException e) {
            throw new ResourceException("commit failed.", e);
        } finally {
            try {
                setInManagedTx(false);
            } catch (JMSException e) {
                throw new ResourceException("commit failed.", e);
            }
        }
    }

    public void rollback() throws ResourceException {
        try {
            transactionContext.rollback();
        } catch (JMSException e) {
            throw new ResourceException("rollback failed.", e);
        } finally {
            try {
                setInManagedTx(false);
            } catch (JMSException e) {
                throw new ResourceException("rollback failed.", e);
            }
        }
    }

    public void commit(Xid arg0, boolean arg1) throws XAException {
        transactionContext.commit(arg0, arg1);
    }

    public void end(Xid arg0, int arg1) throws XAException {
        LOG.debug("{} end {} with {}", new Object[]{this, arg0, arg1});
        try {
            transactionContext.end(arg0, arg1);
        } finally {
            try {
                setInManagedTx(false);
            } catch (JMSException e) {
                throw (XAException)new XAException(XAException.XAER_PROTO).initCause(e);
            }
        }
    }

    public void forget(Xid arg0) throws XAException {
        transactionContext.forget(arg0);
    }

    public int getTransactionTimeout() throws XAException {
        return transactionContext.getTransactionTimeout();
    }

    public boolean isSameRM(XAResource xaresource) throws XAException {
        boolean isSame = false;
        if (xaresource != null) {
            // Do we have to unwrap?
            if (xaresource instanceof LocalAndXATransaction) {
                xaresource = ((LocalAndXATransaction)xaresource).transactionContext;
            }
            isSame = transactionContext.isSameRM(xaresource);
        }
        LOG.trace("{} isSameRM({}) = {}", new Object[]{this, xaresource, isSame});
        return isSame;
    }

    public int prepare(Xid arg0) throws XAException {
        return transactionContext.prepare(arg0);
    }

    public Xid[] recover(int arg0) throws XAException {
        Xid[] answer = null;
        LOG.trace("{} recover({})", new Object[]{this, arg0});
        answer = transactionContext.recover(arg0);
        LOG.trace("{} recover({}) = {}", new Object[]{this, arg0, answer});
        return answer;
    }

    public void rollback(Xid arg0) throws XAException {
        transactionContext.rollback(arg0);
    }

    public boolean setTransactionTimeout(int arg0) throws XAException {
        return transactionContext.setTransactionTimeout(arg0);
    }

    public void start(Xid arg0, int arg1) throws XAException {
        LOG.trace("{} start {} with {}", new Object[]{this, arg0, arg1});
        transactionContext.start(arg0, arg1);
        try {
            setInManagedTx(true);
        } catch (JMSException e) {
            throw (XAException)new XAException(XAException.XAER_PROTO).initCause(e);
        }
    }

    public boolean isInManagedTx() {
        return inManagedTx;
    }

    public void cleanup() {
        transactionContext.cleanup();
        inManagedTx = false;
    }

    @Override
    public String toString() {
        return "[" + super.toString() + "," + transactionContext + "]";
    }
}
