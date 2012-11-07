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
package org.apache.activemq.broker.jmx;

import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.transaction.XATransaction;

public class RecoveredXATransactionView implements RecoveredXATransactionViewMBean {

    private final XATransaction transaction;

    public RecoveredXATransactionView(final ManagedRegionBroker managedRegionBroker, final XATransaction transaction) {
        this.transaction = transaction;
        transaction.addSynchronization(new Synchronization() {
            @Override
            public void afterCommit() throws Exception {
                managedRegionBroker.unregister(transaction);
            }

            @Override
            public void afterRollback() throws Exception {
                managedRegionBroker.unregister(transaction);
            }
        });
    }

    @Override
    public int getFormatId() {
        return transaction.getXid().getFormatId();
    }

    @Override
    public byte[] getBranchQualifier() {
        return transaction.getXid().getBranchQualifier();
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return transaction.getXid().getGlobalTransactionId();
    }

    @Override
    public void heuristicCommit() throws Exception {
        transaction.commit(false);
    }

    @Override
    public void heuristicRollback() throws Exception {
        transaction.rollback();
    }
}
