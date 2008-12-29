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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.ReferenceStore;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.jpa.model.StoredMessageReference;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;

public class JPAReferenceStore extends AbstractMessageStore implements ReferenceStore {

    protected final JPAPersistenceAdapter adapter;
    protected final WireFormat wireFormat;
    protected final String destinationName;
    protected AtomicLong lastMessageId = new AtomicLong(-1);
    protected final Lock lock = new ReentrantLock();
    
    public JPAReferenceStore(JPAPersistenceAdapter adapter, ActiveMQDestination destination) {
        super(destination);
        this.adapter = adapter;
        this.destinationName = destination.getQualifiedName();
        this.wireFormat = this.adapter.getWireFormat();
    }
    
    public Lock getStoreLock() {
        return lock;
    }

    public void addMessage(ConnectionContext context, Message message) throws IOException {
        throw new RuntimeException("Use addMessageReference instead");
    }

    public Message getMessage(MessageId identity) throws IOException {
        throw new RuntimeException("Use addMessageReference instead");
    }

    public void addMessageReference(ConnectionContext context, MessageId messageId, ReferenceData data) throws IOException {
        EntityManager manager = adapter.beginEntityManager(context);
        try {

            StoredMessageReference sm = new StoredMessageReference();
            sm.setDestination(destinationName);
            sm.setId(messageId.getBrokerSequenceId());
            sm.setMessageId(messageId.toString());
            sm.setExiration(data.getExpiration());
            sm.setFileId(data.getFileId());
            sm.setOffset(data.getOffset());

            manager.persist(sm);

        } catch (Throwable e) {
            adapter.rollbackEntityManager(context, manager);
            throw IOExceptionSupport.create(e);
        }
        adapter.commitEntityManager(context, manager);
    }

    public ReferenceData getMessageReference(MessageId identity) throws IOException {
        ReferenceData rc = null;
        EntityManager manager = adapter.beginEntityManager(null);
        try {
            StoredMessageReference message = null;
            if (identity.getBrokerSequenceId() != 0) {
                message = manager.find(StoredMessageReference.class, identity.getBrokerSequenceId());
            } else {
                Query query = manager.createQuery("select m from StoredMessageReference m where m.messageId=?1");
                query.setParameter(1, identity.toString());
                message = (StoredMessageReference)query.getSingleResult();
            }
            if (message != null) {
                rc = new ReferenceData();
                rc.setExpiration(message.getExiration());
                rc.setFileId(message.getFileId());
                rc.setOffset(message.getOffset());
            }
        } catch (Throwable e) {
            adapter.rollbackEntityManager(null, manager);
            throw IOExceptionSupport.create(e);
        }
        adapter.commitEntityManager(null, manager);
        return rc;
    }

    public int getMessageCount() throws IOException {
        Long rc;
        EntityManager manager = adapter.beginEntityManager(null);
        try {
            Query query = manager.createQuery("select count(m) from StoredMessageReference m");
            rc = (Long)query.getSingleResult();
        } catch (Throwable e) {
            adapter.rollbackEntityManager(null, manager);
            throw IOExceptionSupport.create(e);
        }
        adapter.commitEntityManager(null, manager);
        return rc.intValue();
    }

    public void recover(MessageRecoveryListener container) throws Exception {
        EntityManager manager = adapter.beginEntityManager(null);
        try {
            Query query = manager.createQuery("select m from StoredMessageReference m where m.destination=?1 order by m.id asc");
            query.setParameter(1, destinationName);
            for (StoredMessageReference m : (List<StoredMessageReference>)query.getResultList()) {
                MessageId id = new MessageId(m.getMessageId());
                id.setBrokerSequenceId(m.getId());
                container.recoverMessageReference(id);
            }
        } catch (Throwable e) {
            adapter.rollbackEntityManager(null, manager);
            throw IOExceptionSupport.create(e);
        }
        adapter.commitEntityManager(null, manager);
    }

    public void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception {

        EntityManager manager = adapter.beginEntityManager(null);
        try {

            Query query = manager.createQuery("select m from StoredMessageReference m where m.destination=?1 and m.id>?2 order by m.id asc");
            query.setParameter(1, destinationName);
            query.setParameter(2, lastMessageId.get());
            query.setMaxResults(maxReturned);
            int count = 0;
            for (StoredMessageReference m : (List<StoredMessageReference>)query.getResultList()) {
                MessageId id = new MessageId(m.getMessageId());
                id.setBrokerSequenceId(m.getId());
                listener.recoverMessageReference(id);
                lastMessageId.set(m.getId());
                count++;
                if (count >= maxReturned) {
                    return;
                }
            }

        } catch (Throwable e) {
            adapter.rollbackEntityManager(null, manager);
            throw IOExceptionSupport.create(e);
        }
        adapter.commitEntityManager(null, manager);
    }

    public void removeAllMessages(ConnectionContext context) throws IOException {
        EntityManager manager = adapter.beginEntityManager(context);
        try {
            Query query = manager.createQuery("delete from StoredMessageReference m where m.destination=?1");
            query.setParameter(1, destinationName);
            query.executeUpdate();
        } catch (Throwable e) {
            adapter.rollbackEntityManager(context, manager);
            throw IOExceptionSupport.create(e);
        }
        adapter.commitEntityManager(context, manager);
    }

    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
        EntityManager manager = adapter.beginEntityManager(context);
        try {
            Query query = manager.createQuery("delete from StoredMessageReference m where m.id=?1");
            query.setParameter(1, ack.getLastMessageId().getBrokerSequenceId());
            query.executeUpdate();
        } catch (Throwable e) {
            adapter.rollbackEntityManager(context, manager);
            throw IOExceptionSupport.create(e);
        }
        adapter.commitEntityManager(context, manager);
    }

    public void resetBatching() {
        lastMessageId.set(-1);
    }

    public void setBatch(MessageId startAfter) {
    }

    public boolean supportsExternalBatchControl() {
        return false;
    }
}
