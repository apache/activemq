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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.kaha.CommandMarshaller;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.Usage;
import org.apache.activemq.usage.UsageListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * persist pending messages pending message (messages awaiting dispatch to a
 * consumer) cursor
 * 
 * @version $Revision$
 */
public class FilePendingMessageCursor extends AbstractPendingMessageCursor implements UsageListener {
    private static final Log LOG = LogFactory.getLog(FilePendingMessageCursor.class);
    private static final AtomicLong NAME_COUNT = new AtomicLong();
    protected Broker broker;
    private Store store;
    private String name;
    private LinkedList<MessageReference> memoryList = new LinkedList<MessageReference>();
    private ListContainer<MessageReference> diskList;
    private Iterator<MessageReference> iter;
    private Destination regionDestination;
    private boolean iterating;
    private boolean flushRequired;
    private AtomicBoolean started = new AtomicBoolean();
    private MessageReference last = null;

    /**
     * @param name
     * @param store
     */
    public FilePendingMessageCursor(Broker broker,String name) {
        this.useCache=false;
        this.broker = broker;
        //the store can be null if the BrokerService has persistence 
        //turned off
        this.store= broker.getTempDataStore();
        this.name = NAME_COUNT.incrementAndGet() + "_" + name;
    }

    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            super.start();
            if (systemUsage != null) {
                systemUsage.getMemoryUsage().addUsageListener(this);
            }
        }
    }

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
    public synchronized boolean isEmpty() {
        if(memoryList.isEmpty() && isDiskListEmpty()){
            return true;
        }
        for (Iterator<MessageReference> iterator = memoryList.iterator(); iterator.hasNext();) {
            MessageReference node = iterator.next();
            if (node== QueueMessageReference.NULL_MESSAGE){
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
    public synchronized void reset() {
        iterating = true;
        last = null;
        iter = isDiskListEmpty() ? memoryList.iterator() : getDiskList().listIterator();
    }

    public synchronized void release() {
        iterating = false;
        if (flushRequired) {
            flushRequired = false;
            flushToDisk();
        }
    }

    public synchronized void destroy() throws Exception {
        stop();
        for (Iterator<MessageReference> i = memoryList.iterator(); i.hasNext();) {
            Message node = (Message)i.next();
            node.decrementReferenceCount();
        }
        memoryList.clear();
        destroyDiskList();
    }

    private void destroyDiskList() throws Exception {
        if (!isDiskListEmpty()) {
            Iterator<MessageReference> iterator = diskList.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                iterator.remove();
            }
            diskList.clear();
        }   
	    store.deleteListContainer(name, "TopicSubscription");
    }

    public synchronized LinkedList<MessageReference> pageInList(int maxItems) {
        LinkedList<MessageReference> result = new LinkedList<MessageReference>();
        int count = 0;
        for (Iterator<MessageReference> i = memoryList.iterator(); i.hasNext() && count < maxItems;) {
            result.add(i.next());
            count++;
        }
        if (count < maxItems && !isDiskListEmpty()) {
            for (Iterator<MessageReference> i = getDiskList().iterator(); i.hasNext() && count < maxItems;) {
                Message message = (Message)i.next();
                message.setRegionDestination(regionDestination);
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
     */
    public synchronized void addMessageLast(MessageReference node) {
        if (!node.isExpired()) {
            try {
                regionDestination = node.getMessage().getRegionDestination();
                if (isDiskListEmpty()) {
                    if (hasSpace() || this.store==null) {
                        memoryList.add(node);
                        node.incrementReferenceCount();
                        return;
                    }
                }
                if (!hasSpace()) {
                    if (isDiskListEmpty()) {
                        expireOldMessages();
                        if (hasSpace()) {
                            memoryList.add(node);
                            node.incrementReferenceCount();
                            return;
                        } else {
                            flushToDisk();
                        }
                    }
                }
                systemUsage.getTempUsage().waitForSpace();
                getDiskList().add(node);

            } catch (Exception e) {
                LOG.error("Caught an Exception adding a message: " + node
                        + " first to FilePendingMessageCursor ", e);
                throw new RuntimeException(e);
            }
        } else {
            discard(node);
        }
    }

    /**
     * add message to await dispatch
     * 
     * @param node
     */
    public synchronized void addMessageFirst(MessageReference node) {
        if (!node.isExpired()) {
            try {
                regionDestination = node.getMessage().getRegionDestination();
                if (isDiskListEmpty()) {
                    if (hasSpace()) {
                        memoryList.addFirst(node);
                        node.incrementReferenceCount();
                        return;
                    }
                }
                if (!hasSpace()) {
                    if (isDiskListEmpty()) {
                        expireOldMessages();
                        if (hasSpace()) {
                            memoryList.addFirst(node);
                            node.incrementReferenceCount();
                            return;
                        } else {
                            flushToDisk();
                        }
                    }
                }
                systemUsage.getTempUsage().waitForSpace();
                node.decrementReferenceCount();
                getDiskList().addFirst(node);

            } catch (Exception e) {
                LOG.error("Caught an Exception adding a message: " + node
                        + " first to FilePendingMessageCursor ", e);
                throw new RuntimeException(e);
            }
        } else {
            discard(node);
        }
    }

    /**
     * @return true if there pending messages to dispatch
     */
    public synchronized boolean hasNext() {
        return iter.hasNext();
    }

    /**
     * @return the next pending message
     */
    public synchronized MessageReference next() {
        Message message = (Message)iter.next();
        last = message;
        if (!isDiskListEmpty()) {
            // got from disk
            message.setRegionDestination(regionDestination);
            message.setMemoryUsage(this.getSystemUsage().getMemoryUsage());
            message.incrementReferenceCount();
        }
        return message;
    }

    /**
     * remove the message at the cursor position
     */
    public synchronized void remove() {
        iter.remove();
        if (last != null) {
        	last.decrementReferenceCount();
        }
    }

    /**
     * @param node
     * @see org.apache.activemq.broker.region.cursors.AbstractPendingMessageCursor#remove(org.apache.activemq.broker.region.MessageReference)
     */
    public synchronized void remove(MessageReference node) {
        if (memoryList.remove(node)) {
        	node.decrementReferenceCount();
        }
        if (!isDiskListEmpty()) {
            getDiskList().remove(node);
        }
    }

    /**
     * @return the number of pending messages
     */
    public synchronized int size() {
        return memoryList.size() + (isDiskListEmpty() ? 0 : getDiskList().size());
    }

    /**
     * clear all pending messages
     */
    public synchronized void clear() {
        memoryList.clear();
        if (!isDiskListEmpty()) {
            getDiskList().clear();
        }
        last=null;
    }

	public synchronized boolean isFull() {

		return super.isFull()
				|| (systemUsage != null && systemUsage.getTempUsage().isFull());

	}

    public boolean hasMessagesBufferedToDeliver() {
        return !isEmpty();
    }

    public void setSystemUsage(SystemUsage usageManager) {
        super.setSystemUsage(usageManager);
    }

    public void onUsageChanged(Usage usage, int oldPercentUsage,
            int newPercentUsage) {
        if (newPercentUsage >= getMemoryUsageHighWaterMark()) {
            synchronized (this) {
                flushRequired = true;
                if (!iterating) {
                    expireOldMessages();
                    if (!hasSpace()) {
                        flushToDisk();
                        flushRequired = false;
                    }
                }
            }
        }
    }
    
    public boolean isTransient() {
        return true;
    }

    protected boolean isSpaceInMemoryList() {
        return hasSpace() && isDiskListEmpty();
    }
    
    protected synchronized void expireOldMessages() {
        if (!memoryList.isEmpty()) {
            LinkedList<MessageReference> tmpList = new LinkedList<MessageReference>(this.memoryList);
            this.memoryList = new LinkedList<MessageReference>();
            while (!tmpList.isEmpty()) {
                MessageReference node = tmpList.removeFirst();
                if (node.isExpired()) {
                    discard(node);
                }else {
                    memoryList.add(node);
                }               
            }
        }

    }

    protected synchronized void flushToDisk() {
       
        if (!memoryList.isEmpty()) {
            while (!memoryList.isEmpty()) {
                MessageReference node = memoryList.removeFirst();
                node.decrementReferenceCount();
                getDiskList().addLast(node);
            }
            memoryList.clear();
        }
    }

    protected boolean isDiskListEmpty() {
        return diskList == null || diskList.isEmpty();
    }

    protected ListContainer<MessageReference> getDiskList() {
        if (diskList == null) {
            try {
                diskList = store.getListContainer(name, "TopicSubscription", true);
                diskList.setMarshaller(new CommandMarshaller(new OpenWireFormat()));
            } catch (IOException e) {
                LOG.error("Caught an IO Exception getting the DiskList " + name, e);
                throw new RuntimeException(e);
            }
        }
        return diskList;
    }
    
    protected void discard(MessageReference message) {
        message.decrementReferenceCount();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Discarding message " + message);
        }
        broker.getRoot().sendToDeadLetterQueue(new ConnectionContext(new NonCachedMessageEvaluationContext()), message);
    }
}
