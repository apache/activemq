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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.kaha.CommandMarshaller;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.MessageIdMarshaller;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreFactory;
import org.apache.activemq.kaha.impl.index.hash.HashIndex;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ReferenceStore;
import org.apache.activemq.store.ReferenceStoreAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TopicReferenceStore;
import org.apache.activemq.store.amq.AMQTx;
import org.apache.activemq.util.IOHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KahaReferenceStoreAdapter extends KahaPersistenceAdapter implements ReferenceStoreAdapter {

    

    private static final Logger LOG = LoggerFactory.getLogger(KahaReferenceStoreAdapter.class);
    private static final String STORE_STATE = "store-state";
    private static final String QUEUE_DATA = "queue-data";
    private static final String INDEX_VERSION_NAME = "INDEX_VERSION";
    private static final Integer INDEX_VERSION = new Integer(7);
    private static final String RECORD_REFERENCES = "record-references";
    private static final String TRANSACTIONS = "transactions-state";
    private MapContainer stateMap;
    private MapContainer<TransactionId, AMQTx> preparedTransactions;
    private Map<Integer, AtomicInteger> recordReferences = new HashMap<Integer, AtomicInteger>();
    private ListContainer<SubscriptionInfo> durableSubscribers;
    private boolean storeValid;
    private Store stateStore;
    private boolean persistentIndex = true;
    private int indexBinSize = HashIndex.DEFAULT_BIN_SIZE;
    private int indexKeySize = HashIndex.DEFAULT_KEY_SIZE;
    private int indexPageSize = HashIndex.DEFAULT_PAGE_SIZE;
    private int indexMaxBinSize = HashIndex.MAXIMUM_CAPACITY;
    private int indexLoadFactor = HashIndex.DEFAULT_LOAD_FACTOR;
   

    public KahaReferenceStoreAdapter(AtomicLong size){
        super(size);
    }
    
    public synchronized MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        throw new RuntimeException("Use createQueueReferenceStore instead");
    }

    public synchronized TopicMessageStore createTopicMessageStore(ActiveMQTopic destination)
        throws IOException {
        throw new RuntimeException("Use createTopicReferenceStore instead");
    }

    @Override
    public synchronized void start() throws Exception {
        super.start();
        Store store = getStateStore();        
        boolean empty = store.getMapContainerIds().isEmpty();
        stateMap = store.getMapContainer("state", STORE_STATE);
        stateMap.load();
        storeValid=true;
        if (!empty) {
            AtomicBoolean status = (AtomicBoolean)stateMap.get(STORE_STATE);
            if (status != null) {
                storeValid = status.get();
            }
           
            if (storeValid) {
                //check what version the indexes are at
                Integer indexVersion = (Integer) stateMap.get(INDEX_VERSION_NAME);
                if (indexVersion==null || indexVersion.intValue() < INDEX_VERSION.intValue()) {
                    storeValid = false;
                    LOG.warn("Indexes at an older version - need to regenerate");
                }
            }
            if (storeValid) {
                if (stateMap.containsKey(RECORD_REFERENCES)) {
                    recordReferences = (Map<Integer, AtomicInteger>)stateMap.get(RECORD_REFERENCES);
                }
            }
        }
        stateMap.put(STORE_STATE, new AtomicBoolean());
        stateMap.put(INDEX_VERSION_NAME, INDEX_VERSION);
        durableSubscribers = store.getListContainer("durableSubscribers");
        durableSubscribers.setMarshaller(new CommandMarshaller());
        preparedTransactions = store.getMapContainer("transactions", TRANSACTIONS, false);
        // need to set the Marshallers here
        preparedTransactions.setKeyMarshaller(Store.COMMAND_MARSHALLER);
        preparedTransactions.setValueMarshaller(new AMQTxMarshaller(wireFormat));
    }

    @Override
    public synchronized void stop() throws Exception {
        stateMap.put(RECORD_REFERENCES, recordReferences);
        stateMap.put(STORE_STATE, new AtomicBoolean(true));
        stateMap.put(INDEX_VERSION_NAME, INDEX_VERSION);
        if (this.stateStore != null) {
            this.stateStore.close();
            this.stateStore = null;
            this.stateMap = null;
        }
        super.stop();
    }
    
    public void commitTransaction(ConnectionContext context) throws IOException {
        //we don;t need to force on a commit - as the reference store
        //is rebuilt on a non clean shutdown
    }

    public boolean isStoreValid() {
        return storeValid;
    }

    public ReferenceStore createQueueReferenceStore(ActiveMQQueue destination) throws IOException {
        ReferenceStore rc = (ReferenceStore)queues.get(destination);
        if (rc == null) {
            rc = new KahaReferenceStore(this, getMapReferenceContainer(destination, QUEUE_DATA),
                                        destination);
            messageStores.put(destination, rc);
            // if(transactionStore!=null){
            // rc=transactionStore.proxy(rc);
            // }
            queues.put(destination, rc);
        }
        return rc;
    }

    public TopicReferenceStore createTopicReferenceStore(ActiveMQTopic destination) throws IOException {
        TopicReferenceStore rc = (TopicReferenceStore)topics.get(destination);
        if (rc == null) {
            Store store = getStore();
            MapContainer messageContainer = getMapReferenceContainer(destination.getPhysicalName(), "topic-data");
            MapContainer subsContainer = getSubsMapContainer(destination.getPhysicalName() + "-Subscriptions", "blob");
            ListContainer<TopicSubAck> ackContainer = store.getListContainer(destination.getPhysicalName(), "topic-acks");
            ackContainer.setMarshaller(new TopicSubAckMarshaller());
            rc = new KahaTopicReferenceStore(store, this, messageContainer, ackContainer, subsContainer,
                                             destination);
            messageStores.put(destination, rc);
            // if(transactionStore!=null){
            // rc=transactionStore.proxy(rc);
            // }
            topics.put(destination, rc);
        }
        return rc;
    }

    public void removeReferenceStore(KahaReferenceStore referenceStore) {
        ActiveMQDestination destination = referenceStore.getDestination();
        if (destination.isQueue()) {
            queues.remove(destination);
            try {
                getStore().deleteMapContainer(destination, QUEUE_DATA);
            } catch (IOException e) {
                LOG.error("Failed to delete " + QUEUE_DATA + " map container for destination: " + destination, e);
            }
        } else {
            topics.remove(destination);
        }
        messageStores.remove(destination);
    }
/*
    public void buildReferenceFileIdsInUse() throws IOException {
        recordReferences = new HashMap<Integer, AtomicInteger>();
        Set<ActiveMQDestination> destinations = getDestinations();
        for (ActiveMQDestination destination : destinations) {
            if (destination.isQueue()) {
                KahaReferenceStore store = (KahaReferenceStore)createQueueReferenceStore((ActiveMQQueue)destination);
                store.addReferenceFileIdsInUse();
            } else {
                KahaTopicReferenceStore store = (KahaTopicReferenceStore)createTopicReferenceStore((ActiveMQTopic)destination);
                store.addReferenceFileIdsInUse();
            }
        }
    }
    */

    protected MapContainer<MessageId, ReferenceRecord> getMapReferenceContainer(Object id,
                                                                                String containerName)
        throws IOException {
        Store store = getStore();
        MapContainer<MessageId, ReferenceRecord> container = store.getMapContainer(id, containerName,persistentIndex);
        container.setIndexBinSize(getIndexBinSize());
        container.setIndexKeySize(getIndexKeySize());
        container.setIndexPageSize(getIndexPageSize());
        container.setIndexMaxBinSize(getIndexMaxBinSize());
        container.setIndexLoadFactor(getIndexLoadFactor());
        container.setKeyMarshaller(new MessageIdMarshaller());
        container.setValueMarshaller(new ReferenceRecordMarshaller());
        container.load();
        return container;
    }

    synchronized void addInterestInRecordFile(int recordNumber) {
        Integer key = Integer.valueOf(recordNumber);
        AtomicInteger rr = recordReferences.get(key);
        if (rr == null) {
            rr = new AtomicInteger();
            recordReferences.put(key, rr);
        }
        rr.incrementAndGet();
    }

    synchronized void removeInterestInRecordFile(int recordNumber) {
        Integer key = Integer.valueOf(recordNumber);
        AtomicInteger rr = recordReferences.get(key);
        if (rr != null && rr.decrementAndGet() <= 0) {
            recordReferences.remove(key);
        }
    }

    /**
     * @return
     * @throws IOException
     * @see org.apache.activemq.store.ReferenceStoreAdapter#getReferenceFileIdsInUse()
     */
    public synchronized Set<Integer> getReferenceFileIdsInUse() throws IOException {
        Set inUse = new HashSet<Integer>(recordReferences.keySet());

        Iterator<Map.Entry<Integer, Set<Integer>>> ackReferences = ackMessageFileMap.entrySet().iterator();
        while (ackReferences.hasNext()) {
            Map.Entry<Integer, Set<Integer>> ackReference = ackReferences.next();
            if (!inUse.contains(ackReference.getKey())) {
                // should we keep this data file
                for (Integer referencedFileId : ackReference.getValue()) {
                    if (inUse.contains(referencedFileId)) {
                        // keep this ack file
                        inUse.add(ackReference.getKey());
                        LOG.debug("not removing data file: " + ackReference.getKey()
                                        + " as contained ack(s) refer to referencedFileId file: " + ackReference.getValue());
                        break;
                    }
                }
            }
            if (!inUse.contains(ackReference.getKey())) {
               ackReferences.remove();
            }
        }

        return inUse;
    }

    Map<Integer, Set<Integer>> ackMessageFileMap = new HashMap<Integer, Set<Integer>>();
    public synchronized void recordAckFileReferences(int ackDataFileId, int messageFileId) {
        Set<Integer> referenceFileIds = ackMessageFileMap.get(Integer.valueOf(ackDataFileId));
        if (referenceFileIds == null) {
            referenceFileIds = new HashSet<Integer>();
            referenceFileIds.add(Integer.valueOf(messageFileId));
            ackMessageFileMap.put(Integer.valueOf(ackDataFileId), referenceFileIds);
        } else {
            Integer id = Integer.valueOf(messageFileId);
            if (!referenceFileIds.contains(id)) {
                referenceFileIds.add(id);
            }
        }
    }

    /**
     *
     * @throws IOException
     * @see org.apache.activemq.store.ReferenceStoreAdapter#clearMessages()
     */
    public void clearMessages() throws IOException {
        //don't delete messages as it will clear state - call base
        //class method to clear out the data instead
        super.deleteAllMessages();
    }

    /**
     *
     * @throws IOException
     * @see org.apache.activemq.store.ReferenceStoreAdapter#recoverState()
     */

    public void recoverState() throws IOException {
        Set<SubscriptionInfo> set = new HashSet<SubscriptionInfo>(this.durableSubscribers);
        for (SubscriptionInfo info:set) {
            LOG.info("Recovering subscriber state for durable subscriber: " + info);
            TopicReferenceStore ts = createTopicReferenceStore((ActiveMQTopic)info.getDestination());
            ts.addSubsciption(info, false);
        }
    }
    
    public void recoverSubscription(SubscriptionInfo info) throws IOException {
        TopicReferenceStore ts = createTopicReferenceStore((ActiveMQTopic)info.getDestination());
        LOG.info("Recovering subscriber state for durable subscriber: " + info);
        ts.addSubsciption(info, false);
    }
    

    public Map<TransactionId, AMQTx> retrievePreparedState() throws IOException {
        Map<TransactionId, AMQTx> result = new HashMap<TransactionId, AMQTx>();
        preparedTransactions.load();
        for (Iterator<TransactionId> i = preparedTransactions.keySet().iterator(); i.hasNext();) {
            TransactionId key = i.next();
            AMQTx value = preparedTransactions.get(key);
            result.put(key, value);
        }
        return result;
    }

    public void savePreparedState(Map<TransactionId, AMQTx> map) throws IOException {
        preparedTransactions.clear();
        for (Iterator<Map.Entry<TransactionId, AMQTx>> iter = map.entrySet().iterator(); iter.hasNext();) {
            Map.Entry<TransactionId, AMQTx> entry = iter.next();
            preparedTransactions.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public synchronized void setDirectory(File directory) {
        File file = new File(directory, "data");
        super.setDirectory(file);
        this.stateStore = createStateStore(directory);
    }

    protected synchronized Store getStateStore() throws IOException {
        if (this.stateStore == null) {
            File stateDirectory = new File(getDirectory(), "kr-state");
            IOHelper.mkdirs(stateDirectory);
            this.stateStore = createStateStore(getDirectory());
        }
        return this.stateStore;
    }

    public void deleteAllMessages() throws IOException {
        super.deleteAllMessages();
        if (stateStore != null) {
            if (stateStore.isInitialized()) {
                stateStore.clear();
            } else {
                stateStore.delete();
            }
        } else {
            File stateDirectory = new File(getDirectory(), "kr-state");
            StoreFactory.delete(stateDirectory);
        }
    }

    public boolean isPersistentIndex() {
		return persistentIndex;
	}

	public void setPersistentIndex(boolean persistentIndex) {
		this.persistentIndex = persistentIndex;
	}

    private Store createStateStore(File directory) {
        File stateDirectory = new File(directory, "state");
        try {
            IOHelper.mkdirs(stateDirectory);
            return StoreFactory.open(stateDirectory, "rw");
        } catch (IOException e) {
            LOG.error("Failed to create the state store", e);
        }
        return null;
    }

    protected void addSubscriberState(SubscriptionInfo info) throws IOException {
        durableSubscribers.add(info);
    }

    protected void removeSubscriberState(SubscriptionInfo info) {
        durableSubscribers.remove(info);
    }

    public int getIndexBinSize() {
        return indexBinSize;
    }

    public void setIndexBinSize(int indexBinSize) {
        this.indexBinSize = indexBinSize;
    }

    public int getIndexKeySize() {
        return indexKeySize;
    }

    public void setIndexKeySize(int indexKeySize) {
        this.indexKeySize = indexKeySize;
    }

    public int getIndexPageSize() {
        return indexPageSize;
    }

    public void setIndexPageSize(int indexPageSize) {
        this.indexPageSize = indexPageSize;
    }

    public int getIndexMaxBinSize() {
        return indexMaxBinSize;
    }

    public void setIndexMaxBinSize(int maxBinSize) {
        this.indexMaxBinSize = maxBinSize;
    }

    /**
     * @return the loadFactor
     */
    public int getIndexLoadFactor() {
        return indexLoadFactor;
    }

    /**
     * @param loadFactor the loadFactor to set
     */
    public void setIndexLoadFactor(int loadFactor) {
        this.indexLoadFactor = loadFactor;
    }


}
