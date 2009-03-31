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
package org.apache.activemq.store.kahadaptor;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.ReferenceStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author rajdavies
 *
 */
public class KahaReferenceStore extends AbstractMessageStore implements ReferenceStore {

    private static final Log LOG = LogFactory.getLog(KahaReferenceStore.class);
    protected final MapContainer<MessageId, ReferenceRecord> messageContainer;
    protected KahaReferenceStoreAdapter adapter;
    // keep track of dispatched messages so that duplicate sends that follow a successful
    // dispatch can be suppressed.
    protected ActiveMQMessageAudit dispatchAudit = new ActiveMQMessageAudit();
    private StoreEntry batchEntry;
    private String lastBatchId;
    protected final Lock lock = new ReentrantLock();

    public KahaReferenceStore(KahaReferenceStoreAdapter adapter, MapContainer<MessageId, ReferenceRecord> container,
                              ActiveMQDestination destination) throws IOException {
        super(destination);
        this.adapter = adapter;
        this.messageContainer = container;
    }
    
    public Lock getStoreLock() {
        return lock;
    }

    public void dispose(ConnectionContext context) {
        super.dispose(context);
        this.messageContainer.delete();
        this.adapter.removeReferenceStore(this);
    }

    protected MessageId getMessageId(Object object) {
        return new MessageId(((ReferenceRecord)object).getMessageId());
    }

    public void addMessage(ConnectionContext context, Message message) throws IOException {
        throw new RuntimeException("Use addMessageReference instead");
    }

    public Message getMessage(MessageId identity) throws IOException {
        throw new RuntimeException("Use addMessageReference instead");
    }

    protected final boolean recoverReference(MessageRecoveryListener listener,
            ReferenceRecord record) throws Exception {
        MessageId id = new MessageId(record.getMessageId());
        if (listener.hasSpace()) {
            return listener.recoverMessageReference(id);
        }
        return false;
    }

    public void recover(MessageRecoveryListener listener) throws Exception {
        lock.lock();
        try {
            for (StoreEntry entry = messageContainer.getFirst(); entry != null; entry = messageContainer
                .getNext(entry)) {
                ReferenceRecord record = messageContainer.getValue(entry);
                if (!recoverReference(listener, record)) {
                    break;
                }
            }
        }finally {
            lock.unlock();
        }
    }

    public void recoverNextMessages(int maxReturned, MessageRecoveryListener listener)
        throws Exception {
        lock.lock();
        try {
            StoreEntry entry = batchEntry;
            if (entry == null) {
                entry = messageContainer.getFirst();
            } else {
                entry = messageContainer.refresh(entry);
                if (entry != null) {
                    entry = messageContainer.getNext(entry);
                }
            }
            if (entry != null) {      
                int count = 0;
                do {
                    ReferenceRecord msg = messageContainer.getValue(entry);
                    if (msg != null ) {
                        if (recoverReference(listener, msg)) {
                            count++;
                            lastBatchId = msg.getMessageId();
                        } else if (!listener.isDuplicate(new MessageId(msg.getMessageId()))) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(destination.getQualifiedName() + " did not recover (will retry) message: " + msg.getMessageId());
                            }
                            // give usage limits a chance to reclaim
                            break;
                        } else {
                            // skip duplicate and continue
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(destination.getQualifiedName() + " skipping duplicate, " + msg.getMessageId());
                            }
                        }                        
                    } else {
                        lastBatchId = null;
                    }
                    batchEntry = entry;
                    entry = messageContainer.getNext(entry);
                } while (entry != null && count < maxReturned && listener.hasSpace());
            }
        }finally {
            lock.unlock();
        }
    }

    public boolean addMessageReference(ConnectionContext context, MessageId messageId,
                                                 ReferenceData data) throws IOException {
        
        boolean uniqueueReferenceAdded = false;
        lock.lock();
        try {
            if (!isDuplicate(messageId)) {
                ReferenceRecord record = new ReferenceRecord(messageId.toString(), data);
                messageContainer.put(messageId, record);
                uniqueueReferenceAdded = true;
                addInterest(record);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(destination.getPhysicalName() + " add: " + messageId);
                }
            }
        } finally {
            lock.unlock();
        }
        return uniqueueReferenceAdded;
    }

    protected boolean isDuplicate(final MessageId messageId) {
        boolean duplicate = messageContainer.containsKey(messageId);
        if (!duplicate) {
            duplicate = dispatchAudit.isDuplicate(messageId);
            if (duplicate) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(destination.getPhysicalName()
                        + " ignoring duplicated (add) message reference, already dispatched: "
                        + messageId);
                }
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug(destination.getPhysicalName()
                    + " ignoring duplicated (add) message reference, already in store: " + messageId);
        }
        return duplicate;
    }
    
    public ReferenceData getMessageReference(MessageId identity) throws IOException {
        lock.lock();
        try {
            ReferenceRecord result = messageContainer.get(identity);
            if (result == null) {
                return null;
            }
            return result.getData();
        }finally {
            lock.unlock();
        }
    }

    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
        removeMessage(ack.getLastMessageId());
    }

    public void removeMessage(MessageId msgId) throws IOException {  
        lock.lock();
        try {
            StoreEntry entry = messageContainer.getEntry(msgId);
            if (entry != null) {
                ReferenceRecord rr = messageContainer.remove(msgId);
                if (rr != null) {
                    removeInterest(rr);
                    dispatchAudit.isDuplicate(msgId);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(destination.getPhysicalName() + " remove reference: " + msgId);
                    }
                    if (messageContainer.isEmpty()
                        || (lastBatchId != null && lastBatchId.equals(msgId.toString()))
                        || (batchEntry != null && batchEntry.equals(entry))) {
                        resetBatching();
                    }
                }
            }
        }finally {
            lock.unlock();
        }
    }

    public void removeAllMessages(ConnectionContext context) throws IOException {
        lock.lock();
        try {
            Set<MessageId> tmpSet = new HashSet<MessageId>(messageContainer.keySet());
            for (MessageId id:tmpSet) {
                removeMessage(id);
            }
            resetBatching();
            messageContainer.clear();
        }finally {
            lock.unlock();
        }
    }

    public void delete() {
        lock.lock();
        try {
            messageContainer.clear();
        }finally {
            lock.unlock();
        }
    }

    public void resetBatching() {
        lock.lock();
        try {
            batchEntry = null;
            lastBatchId = null;
        }finally {
            lock.unlock();
        }
    }

    public int getMessageCount() {
        return messageContainer.size();
    }

    public boolean isSupportForCursors() {
        return true;
    }

    public boolean supportsExternalBatchControl() {
        return true;
    }

    void removeInterest(ReferenceRecord rr) {
        adapter.removeInterestInRecordFile(rr.getData().getFileId());
    }

    void addInterest(ReferenceRecord rr) {
        adapter.addInterestInRecordFile(rr.getData().getFileId());
    }

    /**
     * @param startAfter
     * @see org.apache.activemq.store.ReferenceStore#setBatch(org.apache.activemq.command.MessageId)
     */
    public void setBatch(MessageId startAfter) {
        lock.lock();
        try {
            batchEntry = messageContainer.getEntry(startAfter);
            if (LOG.isDebugEnabled()) {
                LOG.debug("setBatch: " + startAfter);
            }
        } finally {
            lock.unlock();
        }
    }
}
