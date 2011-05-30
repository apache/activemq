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
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.TransactionContext;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.transaction.Synchronization;

/**
 * Allows us to switch between using a shared transaction context, or using a
 * local transaction context.
 * 
 * 
 */
public class ManagedTransactionContext extends TransactionContext {

    private final TransactionContext sharedContext;
    private boolean useSharedTxContext;

    public ManagedTransactionContext(TransactionContext sharedContext) {
        super(sharedContext.getConnection());
        this.sharedContext = sharedContext;
        setLocalTransactionEventListener(sharedContext.getLocalTransactionEventListener());
    }

    public void setUseSharedTxContext(boolean enable) throws JMSException {
        if (isInLocalTransaction() || isInXATransaction()) {
            throw new JMSException("The resource is already being used in transaction context.");
        }
        useSharedTxContext = enable;
    }

    public void begin() throws JMSException {
        if (useSharedTxContext) {
            sharedContext.begin();
        } else {
            super.begin();
        }
    }

    public void commit() throws JMSException {
        if (useSharedTxContext) {
            sharedContext.commit();
        } else {
            super.commit();
        }
    }

    public void commit(Xid xid, boolean onePhase) throws XAException {
        if (useSharedTxContext) {
            sharedContext.commit(xid, onePhase);
        } else {
            super.commit(xid, onePhase);
        }
    }

    public void end(Xid xid, int flags) throws XAException {
        if (useSharedTxContext) {
            sharedContext.end(xid, flags);
        } else {
            super.end(xid, flags);
        }
    }

    public void forget(Xid xid) throws XAException {
        if (useSharedTxContext) {
            sharedContext.forget(xid);
        } else {
            super.forget(xid);
        }
    }

    public TransactionId getTransactionId() {
        if (useSharedTxContext) {
            return sharedContext.getTransactionId();
        } else {
            return super.getTransactionId();
        }
    }

    public int getTransactionTimeout() throws XAException {
        if (useSharedTxContext) {
            return sharedContext.getTransactionTimeout();
        } else {
            return super.getTransactionTimeout();
        }
    }

    public boolean isInLocalTransaction() {
        if (useSharedTxContext) {
            return sharedContext.isInLocalTransaction();
        } else {
            return super.isInLocalTransaction();
        }
    }

    public boolean isInXATransaction() {
        if (useSharedTxContext) {
            // context considers endesd XA transactions as active, so just check for presence
            // of tx when it is shared
            return sharedContext.isInTransaction();
        } else {
            return super.isInXATransaction();
        }
    }

    @Override
    public boolean isInTransaction() {
        return isInXATransaction() || isInLocalTransaction();
    }
 
    public boolean isSameRM(XAResource xaResource) throws XAException {
        if (useSharedTxContext) {
            return sharedContext.isSameRM(xaResource);
        } else {
            return super.isSameRM(xaResource);
        }
    }

    public int prepare(Xid xid) throws XAException {
        if (useSharedTxContext) {
            return sharedContext.prepare(xid);
        } else {
            return super.prepare(xid);
        }
    }

    public Xid[] recover(int flag) throws XAException {
        if (useSharedTxContext) {
            return sharedContext.recover(flag);
        } else {
            return super.recover(flag);
        }
    }

    public void rollback() throws JMSException {
        if (useSharedTxContext) {
            sharedContext.rollback();
        } else {
            super.rollback();
        }
    }

    public void rollback(Xid xid) throws XAException {
        if (useSharedTxContext) {
            sharedContext.rollback(xid);
        } else {
            super.rollback(xid);
        }
    }

    public boolean setTransactionTimeout(int seconds) throws XAException {
        if (useSharedTxContext) {
            return sharedContext.setTransactionTimeout(seconds);
        } else {
            return super.setTransactionTimeout(seconds);
        }
    }

    public void start(Xid xid, int flags) throws XAException {
        if (useSharedTxContext) {
            sharedContext.start(xid, flags);
        } else {
            super.start(xid, flags);
        }
    }

    public void addSynchronization(Synchronization s) {
        if (useSharedTxContext) {
            sharedContext.addSynchronization(s);
        } else {
            super.addSynchronization(s);
        }
    }
}
