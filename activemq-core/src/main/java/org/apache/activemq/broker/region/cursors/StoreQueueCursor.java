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

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.Message;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.memory.UsageManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Store based Cursor for Queues
 * 
 * @version $Revision: 474985 $
 */
public class StoreQueueCursor extends AbstractPendingMessageCursor {

    private static final Log LOG = LogFactory.getLog(StoreQueueCursor.class);
    private int pendingCount;
    private Queue queue;
    private Store tmpStore;
    private PendingMessageCursor nonPersistent;
    private QueueStorePrefetch persistent;
    private boolean started;
    private PendingMessageCursor currentCursor;

    /**
     * Construct
     * 
     * @param queue
     * @param tmpStore
     */
    public StoreQueueCursor(Queue queue, Store tmpStore) {
        this.queue = queue;
        this.tmpStore = tmpStore;
        this.persistent = new QueueStorePrefetch(queue);
        currentCursor = persistent;
    }

    public synchronized void start() throws Exception {
        started = true;
        if (nonPersistent == null) {
            nonPersistent = new FilePendingMessageCursor(queue.getDestination(), tmpStore);
            nonPersistent.setMaxBatchSize(getMaxBatchSize());
            nonPersistent.setUsageManager(usageManager);
        }
        nonPersistent.start();
        persistent.start();
        pendingCount = persistent.size() + nonPersistent.size();
    }

    public synchronized void stop() throws Exception {
        started = false;
        if (nonPersistent != null) {
            nonPersistent.stop();
            nonPersistent.gc();
        }
        persistent.stop();
        persistent.gc();
        pendingCount = 0;
    }

    public synchronized void addMessageLast(MessageReference node) throws Exception {
        if (node != null) {
            Message msg = node.getMessage();
            if (started) {
                pendingCount++;
                if (!msg.isPersistent()) {
                    nonPersistent.addMessageLast(node);
                }
            }
            if (msg.isPersistent()) {
                persistent.addMessageLast(node);
            }
        }
    }

    public synchronized void addMessageFirst(MessageReference node) throws Exception {
        if (node != null) {
            Message msg = node.getMessage();
            if (started) {
                pendingCount++;
                if (!msg.isPersistent()) {
                    nonPersistent.addMessageFirst(node);
                }
            }
            if (msg.isPersistent()) {
                persistent.addMessageFirst(node);
            }
        }
    }

    public synchronized void clear() {
        pendingCount = 0;
    }

    public synchronized boolean hasNext() {

        boolean result = pendingCount > 0;
        if (result) {
            try {
                currentCursor = getNextCursor();
            } catch (Exception e) {
                LOG.error("Failed to get current cursor ", e);
                throw new RuntimeException(e);
            }
            result = currentCursor != null ? currentCursor.hasNext() : false;
        }
        return result;
    }

    public synchronized MessageReference next() {
        MessageReference result = currentCursor != null ? currentCursor.next() : null;
        return result;
    }

    public synchronized void remove() {
        if (currentCursor != null) {
            currentCursor.remove();
        }
        pendingCount--;
    }

    public synchronized void remove(MessageReference node) {
        if (!node.isPersistent()) {
            nonPersistent.remove(node);
        } else {
            persistent.remove(node);
        }
        pendingCount--;
    }

    public synchronized void reset() {
        nonPersistent.reset();
        persistent.reset();
    }

    public synchronized int size() {
        return pendingCount;
    }

    public synchronized boolean isEmpty() {
        return pendingCount <= 0;
    }

    /**
     * Informs the Broker if the subscription needs to intervention to recover
     * it's state e.g. DurableTopicSubscriber may do
     * 
     * @see org.apache.activemq.region.cursors.PendingMessageCursor
     * @return true if recovery required
     */
    public synchronized boolean isRecoveryRequired() {
        return false;
    }

    /**
     * @return the nonPersistent Cursor
     */
    public synchronized PendingMessageCursor getNonPersistent() {
        return this.nonPersistent;
    }

    /**
     * @param nonPersistent cursor to set
     */
    public synchronized void setNonPersistent(PendingMessageCursor nonPersistent) {
        this.nonPersistent = nonPersistent;
    }

    public synchronized void setMaxBatchSize(int maxBatchSize) {
        persistent.setMaxBatchSize(maxBatchSize);
        if (nonPersistent != null) {
            nonPersistent.setMaxBatchSize(maxBatchSize);
        }
        super.setMaxBatchSize(maxBatchSize);
    }

    public synchronized void gc() {
        if (persistent != null) {
            persistent.gc();
        }
        if (nonPersistent != null) {
            nonPersistent.gc();
        }
    }

    public synchronized void setUsageManager(UsageManager usageManager) {
        super.setUsageManager(usageManager);
        if (persistent != null) {
            persistent.setUsageManager(usageManager);
        }
        if (nonPersistent != null) {
            nonPersistent.setUsageManager(usageManager);
        }
    }

    protected synchronized PendingMessageCursor getNextCursor() throws Exception {
        if (currentCursor == null || !currentCursor.hasMessagesBufferedToDeliver()) {
            currentCursor = currentCursor == persistent ? nonPersistent : persistent;
            // sanity check
            if (currentCursor.isEmpty()) {
                currentCursor = currentCursor == persistent ? nonPersistent : persistent;
            }
        }
        return currentCursor;
    }
}
