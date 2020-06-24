/*
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.IndirectMessageReference;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.PList;
import org.apache.activemq.store.PListEntry;
import org.apache.activemq.store.PListStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.usage.UsageListener;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * persist pending messages pending message (messages awaiting dispatch to a
 * consumer) cursor
 */
public class FilePendingMessageCursor extends AbstractPendingMessageCursor implements UsageListener {

    static final Logger LOG = LoggerFactory.getLogger(FilePendingMessageCursor.class);

    private static final AtomicLong NAME_COUNT = new AtomicLong();

    protected Broker broker;
    private final PListStore store;
    private final String name;
    private PendingList memoryList;
    private PList diskList;
    private Iterator<MessageReference> iter;
    private Destination regionDestination;
    private boolean iterating;
    private boolean flushRequired;
    private final AtomicBoolean started = new AtomicBoolean();
    private final WireFormat wireFormat = new OpenWireFormat();

    /**
     * @param broker
     * @param name
     * @param prioritizedMessages
     */
    public FilePendingMessageCursor(Broker broker, String name, boolean prioritizedMessages) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2791
        super(prioritizedMessages);
//IC see: https://issues.apache.org/jira/browse/AMQ-3596
        if (this.prioritizedMessages) {
            this.memoryList = new PrioritizedPendingList();
        } else {
            this.memoryList = new OrderedPendingList();
        }
        this.broker = broker;
        // the store can be null if the BrokerService has persistence
        // turned off
        this.store = broker.getTempDataStore();
        this.name = NAME_COUNT.incrementAndGet() + "_" + name;
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
//IC see: https://issues.apache.org/jira/browse/AMQ-4563
            if( this.broker != null) {
                wireFormat.setVersion(this.broker.getBrokerService().getStoreOpenWireVersion());
            }
//IC see: https://issues.apache.org/jira/browse/AMQ-1452
//IC see: https://issues.apache.org/jira/browse/AMQ-729
            super.start();
            if (systemUsage != null) {
                systemUsage.getMemoryUsage().addUsageListener(this);
            }
        }
    }

    @Override
    public void stop() throws Exception {
        if (started.compareAndSet(true, false)) {
            super.stop();
            if (systemUsage != null) {
                systemUsage.getMemoryUsage().removeUsageListener(this);
            }
        }
    }

    /**
     * @return true if there are no pending messages
     */
    @Override
    public synchronized boolean isEmpty() {
//IC see: https://issues.apache.org/jira/browse/AMQ-2575
        if (memoryList.isEmpty() && isDiskListEmpty()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-1251
//IC see: https://issues.apache.org/jira/browse/AMQ-1748
            return true;
        }
        for (Iterator<MessageReference> iterator = memoryList.iterator(); iterator.hasNext();) {
            MessageReference node = iterator.next();
            if (node == QueueMessageReference.NULL_MESSAGE) {
                continue;
            }
            if (!node.isDropped()) {
                return false;
            }
            // We can remove dropped references.
            iterator.remove();
        }
        return isDiskListEmpty();
    }

    /**
     * reset the cursor
     */
    @Override
    public synchronized void reset() {
        iterating = true;
        last = null;
//IC see: https://issues.apache.org/jira/browse/AMQ-2575
        if (isDiskListEmpty()) {
            this.iter = this.memoryList.iterator();
        } else {
            this.iter = new DiskIterator();
        }
    }

    @Override
    public synchronized void release() {
        iterating = false;
//IC see: https://issues.apache.org/jira/browse/AMQ-3434
        if (iter instanceof DiskIterator) {
           ((DiskIterator)iter).release();
        };
        if (flushRequired) {
            flushRequired = false;
            if (!hasSpace()) {
                flushToDisk();
            }
        }
        // ensure any memory ref is released
//IC see: https://issues.apache.org/jira/browse/AMQ-4248
        iter = null;
    }

    @Override
    public synchronized void destroy() throws Exception {
        stop();
        for (Iterator<MessageReference> i = memoryList.iterator(); i.hasNext();) {
//IC see: https://issues.apache.org/jira/browse/AMQ-4283
            MessageReference node = i.next();
//IC see: https://issues.apache.org/jira/browse/AMQ-3507
            node.decrementReferenceCount();
        }
        memoryList.clear();
        destroyDiskList();
    }

    private void destroyDiskList() throws Exception {
//IC see: https://issues.apache.org/jira/browse/AMQ-3351
        if (diskList != null) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2575
            store.removePList(name);
            diskList = null;
        }
    }

    @Override
    public synchronized LinkedList<MessageReference> pageInList(int maxItems) {
        LinkedList<MessageReference> result = new LinkedList<MessageReference>();
        int count = 0;
        for (Iterator<MessageReference> i = memoryList.iterator(); i.hasNext() && count < maxItems;) {
            MessageReference ref = i.next();
//IC see: https://issues.apache.org/jira/browse/AMQ-2610
            ref.incrementReferenceCount();
            result.add(ref);
            count++;
        }
        if (count < maxItems && !isDiskListEmpty()) {
            for (Iterator<MessageReference> i = new DiskIterator(); i.hasNext() && count < maxItems;) {
                Message message = (Message) i.next();
                message.setRegionDestination(regionDestination);
//IC see: https://issues.apache.org/jira/browse/AMQ-1490
//IC see: https://issues.apache.org/jira/browse/AMQ-1490
                message.setMemoryUsage(this.getSystemUsage().getMemoryUsage());
                message.incrementReferenceCount();
                result.add(message);
                count++;
            }
        }
        return result;
    }

    /**
     * add message to await dispatch
     *
     * @param node
     * @throws Exception
     */
    @Override
    public synchronized boolean tryAddMessageLast(MessageReference node, long maxWaitTime) throws Exception {
        if (!node.isExpired()) {
            try {
                regionDestination = (Destination) node.getMessage().getRegionDestination();
                if (isDiskListEmpty()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2575
                    if (hasSpace() || this.store == null) {
                        memoryList.addMessageLast(node);
                        node.incrementReferenceCount();
                        setCacheEnabled(true);
                        return true;
                    }
                }
                if (!hasSpace()) {
                    if (isDiskListEmpty()) {
                        expireOldMessages();
                        if (hasSpace()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3596
//IC see: https://issues.apache.org/jira/browse/AMQ-3596
                            memoryList.addMessageLast(node);
                            node.incrementReferenceCount();
                            return true;
                        } else {
                            flushToDisk();
                        }
                    }
                }
                if (systemUsage.getTempUsage().waitForSpace(maxWaitTime)) {
                    ByteSequence bs = getByteSequence(node.getMessage());
                    getDiskList().addLast(node.getMessageId().toString(), bs);
                    return true;
                }
                return false;

            } catch (Exception e) {
                LOG.error("Caught an Exception adding a message: {} first to FilePendingMessageCursor ", node, e);
                throw new RuntimeException(e);
            }
        } else {
//IC see: https://issues.apache.org/jira/browse/AMQ-3507
            discardExpiredMessage(node);
        }
        //message expired
        return true;
    }

    /**
     * add message to await dispatch
     *
     * @param node
     */
    @Override
    public synchronized void addMessageFirst(MessageReference node) {
        if (!node.isExpired()) {
            try {
                regionDestination = (Destination) node.getMessage().getRegionDestination();
                if (isDiskListEmpty()) {
                    if (hasSpace()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3596
                        memoryList.addMessageFirst(node);
                        node.incrementReferenceCount();
//IC see: https://issues.apache.org/jira/browse/AMQ-3188
//IC see: https://issues.apache.org/jira/browse/AMQ-3188
                        setCacheEnabled(true);
                        return;
                    }
                }
                if (!hasSpace()) {
                    if (isDiskListEmpty()) {
                        expireOldMessages();
                        if (hasSpace()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3596
                            memoryList.addMessageFirst(node);
                            node.incrementReferenceCount();
                            return;
                        } else {
                            flushToDisk();
                        }
                    }
                }
                systemUsage.getTempUsage().waitForSpace();
                node.decrementReferenceCount();
                ByteSequence bs = getByteSequence(node.getMessage());
//IC see: https://issues.apache.org/jira/browse/AMQ-4215
                Object locator = getDiskList().addFirst(node.getMessageId().toString(), bs);
                node.getMessageId().setPlistLocator(locator);

            } catch (Exception e) {
                LOG.error("Caught an Exception adding a message: {} first to FilePendingMessageCursor ", node, e);
                throw new RuntimeException(e);
            }
//IC see: https://issues.apache.org/jira/browse/AMQ-2575
        } else {
            discardExpiredMessage(node);
        }
    }

    /**
     * @return true if there pending messages to dispatch
     */
    @Override
    public synchronized boolean hasNext() {
        return iter.hasNext();
    }

    /**
     * @return the next pending message
     */
    @Override
    public synchronized MessageReference next() {
//IC see: https://issues.apache.org/jira/browse/AMQ-3288
        MessageReference reference = iter.next();
        last = reference;
        if (!isDiskListEmpty()) {
            // got from disk
            reference.getMessage().setRegionDestination(regionDestination);
            reference.getMessage().setMemoryUsage(this.getSystemUsage().getMemoryUsage());
        }
        reference.incrementReferenceCount();
        return reference;
    }

    /**
     * remove the message at the cursor position
     */
    @Override
    public synchronized void remove() {
//IC see: https://issues.apache.org/jira/browse/AMQ-1748
        iter.remove();
        if (last != null) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2575
            last.decrementReferenceCount();
        }
    }

    /**
     * @param node
     * @see org.apache.activemq.broker.region.cursors.AbstractPendingMessageCursor#remove(org.apache.activemq.broker.region.MessageReference)
     */
    @Override
    public synchronized void remove(MessageReference node) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3596
        if (memoryList.remove(node) != null) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2575
            node.decrementReferenceCount();
        }
        if (!isDiskListEmpty()) {
            try {
//IC see: https://issues.apache.org/jira/browse/AMQ-4215
                getDiskList().remove(node.getMessageId().getPlistLocator());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @return the number of pending messages
     */
    @Override
    public synchronized int size() {
//IC see: https://issues.apache.org/jira/browse/AMQ-3351
        return memoryList.size() + (isDiskListEmpty() ? 0 : (int)getDiskList().size());
    }

    @Override
    public synchronized long messageSize() {
//IC see: https://issues.apache.org/jira/browse/AMQ-6352
        return memoryList.messageSize() + (isDiskListEmpty() ? 0 : getDiskList().messageSize());
    }

    /**
     * clear all pending messages
     */
    @Override
    public synchronized void clear() {
        memoryList.clear();
        if (!isDiskListEmpty()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-2575
            try {
                getDiskList().destroy();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        last = null;
    }

    @Override
    public synchronized boolean isFull() {
        return super.isFull() || (!isDiskListEmpty() && systemUsage != null && systemUsage.getTempUsage().isFull());
    }

    @Override
    public boolean hasMessagesBufferedToDeliver() {
        return !isEmpty();
    }

    @Override
    public void setSystemUsage(SystemUsage usageManager) {
        super.setSystemUsage(usageManager);
    }

    @Override
    public void onUsageChanged(Usage usage, int oldPercentUsage, int newPercentUsage) {
        if (newPercentUsage >= getMemoryUsageHighWaterMark()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-5785
            List<MessageReference> expiredMessages = null;
//IC see: https://issues.apache.org/jira/browse/AMQ-1748
            synchronized (this) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3474
                if (!flushRequired && size() != 0) {
                    flushRequired =true;
                    if (!iterating) {
                        expiredMessages = expireOldMessages();
//IC see: https://issues.apache.org/jira/browse/AMQ-3351
                        if (!hasSpace()) {
                            flushToDisk();
                            flushRequired = false;
                        }
                    }
                }
            }

//IC see: https://issues.apache.org/jira/browse/AMQ-5785
            if (expiredMessages != null) {
                for (MessageReference node : expiredMessages) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3507
                    discardExpiredMessage(node);
                }
            }
        }
    }

    @Override
    public boolean isTransient() {
//IC see: https://issues.apache.org/jira/browse/AMQ-1449
        return true;
    }

    private synchronized List<MessageReference> expireOldMessages() {
//IC see: https://issues.apache.org/jira/browse/AMQ-5785
        List<MessageReference> expired = new ArrayList<MessageReference>();
        if (!memoryList.isEmpty()) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3596
            for (Iterator<MessageReference> iterator = memoryList.iterator(); iterator.hasNext();) {
                MessageReference node = iterator.next();
                if (node.isExpired()) {
                    node.decrementReferenceCount();
                    expired.add(node);
                    iterator.remove();
                }
            }
        }

        return expired;
    }

    protected synchronized void flushToDisk() {
//IC see: https://issues.apache.org/jira/browse/AMQ-3434
        if (!memoryList.isEmpty() && store != null) {
//IC see: https://issues.apache.org/jira/browse/AMQ-3351
            long start = 0;
            if (LOG.isTraceEnabled()) {
                start = System.currentTimeMillis();
                LOG.trace("{}, flushToDisk() mem list size: {} {}", new Object[] { name, memoryList.size(),
                    (systemUsage != null ? systemUsage.getMemoryUsage() : "") });
            }
//IC see: https://issues.apache.org/jira/browse/AMQ-3596
            for (Iterator<MessageReference> iterator = memoryList.iterator(); iterator.hasNext();) {
                MessageReference node = iterator.next();
                node.decrementReferenceCount();
                ByteSequence bs;
                try {
                    bs = getByteSequence(node.getMessage());
                    getDiskList().addLast(node.getMessageId().toString(), bs);
                } catch (IOException e) {
                    LOG.error("Failed to write to disk list", e);
                    throw new RuntimeException(e);
                }

            }
            memoryList.clear();
//IC see: https://issues.apache.org/jira/browse/AMQ-3188
            setCacheEnabled(false);
            LOG.trace("{}, flushToDisk() done - {} ms {}", new Object[]{ name, (System.currentTimeMillis() - start), (systemUsage != null ? systemUsage.getMemoryUsage() : "") });
        }
    }

    protected boolean isDiskListEmpty() {
        return diskList == null || diskList.isEmpty();
    }

    public PList getDiskList() {
        if (diskList == null) {
            try {
                diskList = store.getPList(name);
            } catch (Exception e) {
                LOG.error("Caught an IO Exception getting the DiskList {}", name, e);
                throw new RuntimeException(e);
            }
        }
        return diskList;
    }

    private void discardExpiredMessage(MessageReference reference) {
        LOG.debug("Discarding expired message {}", reference);
//IC see: https://issues.apache.org/jira/browse/AMQ-6361
        if (reference.isExpired() && broker.isExpired(reference)) {
//IC see: https://issues.apache.org/jira/browse/AMQ-7035
//IC see: https://issues.apache.org/jira/browse/AMQ-6465
            ConnectionContext context = new ConnectionContext();
            context.setBroker(broker);
            ((Destination)reference.getRegionDestination()).messageExpired(context, null, new IndirectMessageReference(reference.getMessage()));
        }
    }

    protected ByteSequence getByteSequence(Message message) throws IOException {
        org.apache.activemq.util.ByteSequence packet = wireFormat.marshal(message);
        return new ByteSequence(packet.data, packet.offset, packet.length);
    }

    protected Message getMessage(ByteSequence bs) throws IOException {
        org.apache.activemq.util.ByteSequence packet = new org.apache.activemq.util.ByteSequence(bs.getData(), bs
                .getOffset(), bs.getLength());
        return (Message) this.wireFormat.unmarshal(packet);

    }

    final class DiskIterator implements Iterator<MessageReference> {
        private final PList.PListIterator iterator;
        DiskIterator() {
            try {
                iterator = getDiskList().iterator();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public MessageReference next() {
            try {
                PListEntry entry = iterator.next();
//IC see: https://issues.apache.org/jira/browse/AMQ-4215
                Message message = getMessage(entry.getByteSequence());
                message.getMessageId().setPlistLocator(entry.getLocator());
                return message;
            } catch (IOException e) {
                LOG.error("I/O error", e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
//IC see: https://issues.apache.org/jira/browse/AMQ-3351
            iterator.remove();
        }

        public void release() {
//IC see: https://issues.apache.org/jira/browse/AMQ-3434
            iterator.release();
        }
    }
}
