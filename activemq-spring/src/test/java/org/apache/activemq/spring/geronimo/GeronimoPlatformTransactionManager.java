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
package org.apache.activemq.spring.geronimo;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.transaction.Status;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import javax.transaction.xa.XAException;

import org.apache.geronimo.transaction.manager.GeronimoTransactionManager;
import org.apache.geronimo.transaction.manager.TransactionLog;
import org.apache.geronimo.transaction.manager.TransactionManagerMonitor;
import org.apache.geronimo.transaction.manager.XidFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * @version $Revision$ $Date$
 */
public class GeronimoPlatformTransactionManager extends GeronimoTransactionManager implements PlatformTransactionManager {
    
    private final PlatformTransactionManager platformTransactionManager;
    private final Map<Transaction, SuspendedResourcesHolder> suspendedResources = new ConcurrentHashMap<Transaction, SuspendedResourcesHolder>();

    public GeronimoPlatformTransactionManager() throws XAException {
        platformTransactionManager = new JtaTransactionManager(this, this);
        registerTransactionAssociationListener();
    }

    public GeronimoPlatformTransactionManager(int defaultTransactionTimeoutSeconds) throws XAException {
        super(defaultTransactionTimeoutSeconds);
        platformTransactionManager = new JtaTransactionManager(this, this);
        registerTransactionAssociationListener();
    }

    public GeronimoPlatformTransactionManager(int defaultTransactionTimeoutSeconds, TransactionLog transactionLog) throws XAException {
        super(defaultTransactionTimeoutSeconds, transactionLog);
        platformTransactionManager = new JtaTransactionManager(this, this);
        registerTransactionAssociationListener();
    }

    public GeronimoPlatformTransactionManager(int defaultTransactionTimeoutSeconds, XidFactory xidFactory, TransactionLog transactionLog) throws XAException {
        super(defaultTransactionTimeoutSeconds, xidFactory, transactionLog);
        platformTransactionManager = new JtaTransactionManager(this, this);
        registerTransactionAssociationListener();
    }

    public TransactionStatus getTransaction(TransactionDefinition definition) throws TransactionException {
        return platformTransactionManager.getTransaction(definition);
    }

    public void commit(TransactionStatus status) throws TransactionException {
        platformTransactionManager.commit(status);
    }

    public void rollback(TransactionStatus status) throws TransactionException {
        platformTransactionManager.rollback(status);
    }

    protected void registerTransactionAssociationListener() {
        addTransactionAssociationListener(new TransactionManagerMonitor() {
            public void threadAssociated(Transaction transaction) {
                try {
                    if (transaction.getStatus() == Status.STATUS_ACTIVE) {
                        SuspendedResourcesHolder holder = suspendedResources.remove(transaction);
                        if (holder != null && holder.getSuspendedSynchronizations() != null) {
                            TransactionSynchronizationManager.setActualTransactionActive(true);
                            TransactionSynchronizationManager.setCurrentTransactionReadOnly(holder.isReadOnly());
                            TransactionSynchronizationManager.setCurrentTransactionName(holder.getName());
                            TransactionSynchronizationManager.initSynchronization();
                            for (Iterator<?> it = holder.getSuspendedSynchronizations().iterator(); it.hasNext();) {
                                TransactionSynchronization synchronization = (TransactionSynchronization) it.next();
                                synchronization.resume();
                                TransactionSynchronizationManager.registerSynchronization(synchronization);
                            }
                        }
                    }
                } catch (SystemException e) {
                    return;
                }
            }
            public void threadUnassociated(Transaction transaction) {
                try {
                    if (transaction.getStatus() == Status.STATUS_ACTIVE) {
                        if (TransactionSynchronizationManager.isSynchronizationActive()) {
                            List<?> suspendedSynchronizations = TransactionSynchronizationManager.getSynchronizations();
                            for (Iterator<?> it = suspendedSynchronizations.iterator(); it.hasNext();) {
                                ((TransactionSynchronization) it.next()).suspend();
                            }
                            TransactionSynchronizationManager.clearSynchronization();
                            String name = TransactionSynchronizationManager.getCurrentTransactionName();
                            TransactionSynchronizationManager.setCurrentTransactionName(null);
                            boolean readOnly = TransactionSynchronizationManager.isCurrentTransactionReadOnly();
                            TransactionSynchronizationManager.setCurrentTransactionReadOnly(false);
                            TransactionSynchronizationManager.setActualTransactionActive(false);
                            SuspendedResourcesHolder holder = new SuspendedResourcesHolder(null, suspendedSynchronizations, name, readOnly);
                            suspendedResources.put(transaction, holder);
                        }
                    }
                } catch (SystemException e) {
                    return;
                }
            }
        });
    }

    /**
     * Holder for suspended resources.
     * Used internally by <code>suspend</code> and <code>resume</code>.
     */
    private static class SuspendedResourcesHolder {

        private final Object suspendedResources;

        private final List<?> suspendedSynchronizations;

        private final String name;

        private final boolean readOnly;

        public SuspendedResourcesHolder(
                Object suspendedResources, List<?> suspendedSynchronizations, String name, boolean readOnly) {

            this.suspendedResources = suspendedResources;
            this.suspendedSynchronizations = suspendedSynchronizations;
            this.name = name;
            this.readOnly = readOnly;
        }

        public Object getSuspendedResources() {
            return suspendedResources;
        }

        public List<?> getSuspendedSynchronizations() {
            return suspendedSynchronizations;
        }

        public String getName() {
            return name;
        }

        public boolean isReadOnly() {
            return readOnly;
        }
    }

}
