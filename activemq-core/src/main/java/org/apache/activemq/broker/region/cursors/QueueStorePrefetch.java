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
package org.apache.activemq.broker.region.cursors;

import java.io.IOException;
import java.util.LinkedList;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * persist pending messages pending message (messages awaiting dispatch to a
 * consumer) cursor
 * 
 * @version $Revision: 474985 $
 */
class QueueStorePrefetch extends AbstractPendingMessageCursor implements MessageRecoveryListener {

    private static final Log LOG = LogFactory.getLog(QueueStorePrefetch.class);

    private MessageStore store;
    private final LinkedList<Message> batchList = new LinkedList<Message>();
    private Destination regionDestination;
    private int size;
    private boolean fillBatchDuplicates;
    private boolean cacheEnabled;

    /**
     * @param topic
     * @param clientId
     * @param subscriberName
     * @throws IOException
     */
    public QueueStorePrefetch(Queue queue) {
        this.regionDestination = queue;
        this.store = (MessageStore)queue.getMessageStore();

    }

    public synchronized void start() throws Exception{
        if (!isStarted()) {
            this.size = getStoreSize();
            if (this.size==0) {
                cacheEnabled=true;
            }
        }
        super.start();
        store.resetBatching();
    }

    public void stop() throws Exception {
        store.resetBatching();
        super.stop();
    }

    /**
     * @return true if there are no pending messages
     */
    public boolean isEmpty() {
        return size <= 0;
    }

    public boolean hasMessagesBufferedToDeliver() {
        return !batchList.isEmpty();
    }

    public synchronized int size() {
        if (isStarted()) {
            return size;
        }
        this.size = getStoreSize();
        return size;
        
    }

    public synchronized void addMessageLast(MessageReference node) throws Exception {
        if (cacheEnabled && !isFull()) {
            //optimization - A persistent queue will add the message to
            //to store then retrieve it again from the store.
            recoverMessage(node.getMessage());
        }else {
            cacheEnabled=false;
        }
        size++;
    }

    public void addMessageFirst(MessageReference node) throws Exception {
        size++;
    }

    public synchronized void remove() {
        size--;
        if (size==0 && isStarted()) {
            cacheEnabled=true;
        }
    }

    public void remove(MessageReference node) {
        size--;
        cacheEnabled=false;
    }

    public synchronized boolean hasNext() {
        if (batchList.isEmpty()) {
            try {
                fillBatch();
            } catch (Exception e) {
                LOG.error("Failed to fill batch", e);
                throw new RuntimeException(e);
            }
        }
        return !batchList.isEmpty();
    }

    public synchronized MessageReference next() {
        Message result = batchList.removeFirst();
        result.decrementReferenceCount();
        result.setRegionDestination(regionDestination);
        result.setMemoryUsage(this.getSystemUsage().getMemoryUsage());
        return result;
    }

    public void reset() {
    }

    // MessageRecoveryListener implementation
    public void finished() {
    }

    public synchronized boolean recoverMessage(Message message)
            throws Exception {
        if (!isDuplicate(message.getMessageId())) {
            message.setRegionDestination(regionDestination);
            message.setMemoryUsage(this.getSystemUsage().getMemoryUsage());
            message.incrementReferenceCount();
            batchList.addLast(message);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ignoring batched duplicated from store: " + message);
            }
            fillBatchDuplicates=true;
        }
        return true;
    }

    public boolean recoverMessageReference(MessageId messageReference) throws Exception {
        Message msg = store.getMessage(messageReference);
        if (msg != null) {
            return recoverMessage(msg);
        } else {
            String err = "Failed to retrieve message for id: " + messageReference;
            LOG.error(err);
            throw new IOException(err);
        }
    }

    public synchronized void gc() {
        for (Message msg : batchList) {
            rollback(msg.getMessageId());
            msg.decrementReferenceCount();
        }
        cacheEnabled=false;
        batchList.clear();
    }

    // implementation
    protected synchronized void fillBatch() throws Exception {
        store.recoverNextMessages(maxBatchSize, this);
        while (fillBatchDuplicates && batchList.isEmpty()) {
            fillBatchDuplicates=false;
            store.recoverNextMessages(maxBatchSize, this);
        }
        fillBatchDuplicates=false;
    }
    
    protected synchronized int getStoreSize() {
        try {
            return store.getMessageCount();
        } catch (IOException e) {
            LOG.error("Failed to get message count", e);
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        return "QueueStorePrefetch" + System.identityHashCode(this);
    }

}
