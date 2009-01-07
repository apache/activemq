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
package org.apache.activemq.store.jpa;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.memory.MemoryTransactionStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An implementation of {@link PersistenceAdapter} that uses JPA to store it's
 * messages.
 * 
 * @org.apache.xbean.XBean element="jpaPersistenceAdapter"
 * @version $Revision: 1.17 $
 */
public class JPAPersistenceAdapter implements PersistenceAdapter {

    String entityManagerName = "activemq";
    Properties entityManagerProperties = System.getProperties();
    EntityManagerFactory entityManagerFactory;
    private WireFormat wireFormat;
    private MemoryTransactionStore transactionStore;

    public void beginTransaction(ConnectionContext context) throws IOException {
        if (context.getLongTermStoreContext() != null) {
            throw new IOException("Transation already started.");
        }
        EntityManager manager = getEntityManagerFactory().createEntityManager();
        manager.getTransaction().begin();
        context.setLongTermStoreContext(manager);
    }

    public void commitTransaction(ConnectionContext context) throws IOException {
        EntityManager manager = (EntityManager)context.getLongTermStoreContext();
        if (manager == null) {
            throw new IOException("Transation not started.");
        }
        context.setLongTermStoreContext(null);
        manager.getTransaction().commit();
        manager.close();
    }

    public void rollbackTransaction(ConnectionContext context) throws IOException {
        EntityManager manager = (EntityManager)context.getLongTermStoreContext();
        if (manager == null) {
            throw new IOException("Transation not started.");
        }
        context.setLongTermStoreContext(null);
        manager.getTransaction().rollback();
        manager.close();
    }

    public EntityManager beginEntityManager(ConnectionContext context) {
        if (context == null || context.getLongTermStoreContext() == null) {
            EntityManager manager = getEntityManagerFactory().createEntityManager();
            manager.getTransaction().begin();
            return manager;
        } else {
            return (EntityManager)context.getLongTermStoreContext();
        }
    }

    public void commitEntityManager(ConnectionContext context, EntityManager manager) {
        if (context == null || context.getLongTermStoreContext() == null) {
            manager.getTransaction().commit();
            manager.close();
        }
    }

    public void rollbackEntityManager(ConnectionContext context, EntityManager manager) {
        if (context == null || context.getLongTermStoreContext() == null) {
            manager.getTransaction().rollback();
            manager.close();
        }
    }

    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        MessageStore rc = new JPAMessageStore(this, destination);
        if (transactionStore != null) {
            rc = transactionStore.proxy(rc);
        }
        return rc;
    }

    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        TopicMessageStore rc = new JPATopicMessageStore(this, destination);
        if (transactionStore != null) {
            rc = transactionStore.proxy(rc);
        }
        return rc;
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     *
     * @param destination Destination to forget
     */
    public void removeQueueMessageStore(ActiveMQQueue destination) {
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     *
     * @param destination Destination to forget
     */
    public void removeTopicMessageStore(ActiveMQTopic destination) {
    }

    public TransactionStore createTransactionStore() throws IOException {
        if (transactionStore == null) {
            transactionStore = new MemoryTransactionStore(this);
        }
        return this.transactionStore;
    }

    public void deleteAllMessages() throws IOException {
        EntityManager manager = beginEntityManager(null);
        try {
            Query query = manager.createQuery("delete from StoredMessage m");
            query.executeUpdate();
            query = manager.createQuery("delete from StoredSubscription ss");
            query.executeUpdate();
        } catch (Throwable e) {
            rollbackEntityManager(null, manager);
            throw IOExceptionSupport.create(e);
        }
        commitEntityManager(null, manager);
    }

    public Set<ActiveMQDestination> getDestinations() {
        HashSet<ActiveMQDestination> rc = new HashSet<ActiveMQDestination>();

        EntityManager manager = beginEntityManager(null);
        try {
            Query query = manager.createQuery("select distinct m.destination from StoredMessage m");
            for (String dest : (List<String>)query.getResultList()) {
                rc.add(ActiveMQDestination.createDestination(dest, ActiveMQDestination.QUEUE_TYPE));
            }
        } catch (RuntimeException e) {
            rollbackEntityManager(null, manager);
            throw e;
        }
        commitEntityManager(null, manager);
        return rc;
    }

    public long getLastMessageBrokerSequenceId() throws IOException {
        long rc = 0;
        EntityManager manager = beginEntityManager(null);
        try {
            Query query = manager.createQuery("select max(m.id) from StoredMessage m");
            Long t = (Long)query.getSingleResult();
            if (t != null) {
                rc = t;
            }
        } catch (Throwable e) {
            rollbackEntityManager(null, manager);
            throw IOExceptionSupport.create(e);
        }
        commitEntityManager(null, manager);
        return rc;
    }

    public boolean isUseExternalMessageReferences() {
        return false;
    }

    public void setUsageManager(SystemUsage usageManager) {
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
        if (entityManagerFactory != null) {
            entityManagerFactory.close();
        }
    }

    public EntityManagerFactory getEntityManagerFactory() {
        if (entityManagerFactory == null) {
            entityManagerFactory = createEntityManagerFactory();
        }
        return entityManagerFactory;
    }

    protected EntityManagerFactory createEntityManagerFactory() {
        return Persistence.createEntityManagerFactory(getEntityManagerName(), getEntityManagerProperties());
    }

    public void setEntityManagerFactory(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    public Properties getEntityManagerProperties() {
        return entityManagerProperties;
    }

    public void setEntityManagerProperties(Properties entityManagerProperties) {
        this.entityManagerProperties = entityManagerProperties;
    }

    public String getEntityManagerName() {
        return entityManagerName;
    }

    public void setEntityManagerName(String entityManager) {
        this.entityManagerName = entityManager;
    }

    public WireFormat getWireFormat() {
        if (wireFormat == null) {
            wireFormat = createWireFormat();
        }
        return wireFormat;
    }

    private WireFormat createWireFormat() {
        OpenWireFormatFactory wff = new OpenWireFormatFactory();
        return wff.createWireFormat();
    }

    public void setWireFormat(WireFormat wireFormat) {
        this.wireFormat = wireFormat;
    }

    public void checkpoint(boolean sync) throws IOException {
    }

    public void setBrokerName(String brokerName) {
    }

    public void setDirectory(File dir) {
    }
    
    public long size(){
        return 0;
    }

}
