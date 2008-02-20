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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ReferenceStore;
import org.apache.activemq.store.ReferenceStoreAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TopicReferenceStore;
import org.apache.activemq.store.amq.AMQTx;
import org.apache.activemq.util.IOExceptionSupport;

/**
 * An implementation of {@link ReferenceStoreAdapter} that uses JPA to store
 * it's message references.
 * 
 * @org.apache.xbean.XBean element="jpaReferenceStoreAdapter"
 * @version $Revision: 1.17 $
 */
public class JPAReferenceStoreAdapter extends JPAPersistenceAdapter implements ReferenceStoreAdapter {

    @Override
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        throw new RuntimeException("Use createQueueReferenceStore instead.");
    }

    @Override
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        throw new RuntimeException("Use createTopicReferenceStore instead.");
    }

    public ReferenceStore createQueueReferenceStore(ActiveMQQueue destination) throws IOException {
        JPAReferenceStore rc = new JPAReferenceStore(this, destination);
        return rc;
    }

    public TopicReferenceStore createTopicReferenceStore(ActiveMQTopic destination) throws IOException {
        JPATopicReferenceStore rc = new JPATopicReferenceStore(this, destination);
        return rc;
    }

    public void deleteAllMessages() throws IOException {
        EntityManager manager = beginEntityManager(null);
        try {
            Query query = manager.createQuery("delete from StoredMessageReference m");
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
            Query query = manager.createQuery("select distinct m.destination from StoredMessageReference m");
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
            Query query = manager.createQuery("select max(m.id) from StoredMessageReference m");
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

    public Set<Integer> getReferenceFileIdsInUse() throws IOException {
        HashSet<Integer> rc = null;
        EntityManager manager = beginEntityManager(null);
        try {
            Query query = manager.createQuery("select distinct m.fileId from StoredMessageReference m");
            rc = new HashSet<Integer>((List<Integer>)query.getResultList());
        } catch (Throwable e) {
            rollbackEntityManager(null, manager);
            throw IOExceptionSupport.create(e);
        }
        commitEntityManager(null, manager);
        return rc;
    }

    /**
     * @return
     * @see org.apache.activemq.store.ReferenceStoreAdapter#isStoreValid()
     */
    public boolean isStoreValid() {
        return false;
    }

    /**
     * @see org.apache.activemq.store.ReferenceStoreAdapter#clearMessages()
     */
    public void clearMessages() {
    }

    /**
     * @see org.apache.activemq.store.ReferenceStoreAdapter#recoverState()
     */
    public void recoverState() {
    }

    public Map<TransactionId, AMQTx> retrievePreparedState() throws IOException {
        return null;
    }

    public void savePreparedState(Map<TransactionId, AMQTx> map) throws IOException {
    }

    public long getMaxDataFileLength() {
        return 0;
    }
   
    public void setMaxDataFileLength(long maxDataFileLength) {        
    }
}
