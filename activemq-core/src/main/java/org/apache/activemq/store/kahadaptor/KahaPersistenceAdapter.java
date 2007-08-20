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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.CommandMarshaller;
import org.apache.activemq.kaha.ContainerId;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.MessageIdMarshaller;
import org.apache.activemq.kaha.MessageMarshaller;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreFactory;
import org.apache.activemq.kaha.impl.StoreLockedExcpetion;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.IOHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @org.apache.xbean.XBean
 * @version $Revision: 1.4 $
 */
public class KahaPersistenceAdapter implements PersistenceAdapter {

    private static final int STORE_LOCKED_WAIT_DELAY = 10 * 1000;
    private static final Log LOG = LogFactory.getLog(KahaPersistenceAdapter.class);
    private static final String PREPARED_TRANSACTIONS_NAME = "PreparedTransactions";

    protected OpenWireFormat wireFormat = new OpenWireFormat();
    protected KahaTransactionStore transactionStore;
    protected ConcurrentHashMap<ActiveMQTopic, TopicMessageStore> topics = new ConcurrentHashMap<ActiveMQTopic, TopicMessageStore>();
    protected ConcurrentHashMap<ActiveMQQueue, MessageStore> queues = new ConcurrentHashMap<ActiveMQQueue, MessageStore>();
    protected ConcurrentHashMap<ActiveMQDestination, MessageStore> messageStores = new ConcurrentHashMap<ActiveMQDestination, MessageStore>();

    private long maxDataFileLength = 32 * 1024 * 1024;
    private File directory;
    private String brokerName;
    private Store theStore;
    private boolean initialized;
    private final AtomicLong storeSize;

    
    public KahaPersistenceAdapter(AtomicLong size) {
        this.storeSize=size;
    }
    
    public KahaPersistenceAdapter() {
        this(new AtomicLong());
    }
    public Set<ActiveMQDestination> getDestinations() {
        Set<ActiveMQDestination> rc = new HashSet<ActiveMQDestination>();
        try {
            Store store = getStore();
            for (Iterator i = store.getMapContainerIds().iterator(); i.hasNext();) {
                ContainerId id = (ContainerId)i.next();
                Object obj = id.getKey();
                if (obj instanceof ActiveMQDestination) {
                    rc.add((ActiveMQDestination)obj);
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to get destinations ", e);
        }
        return rc;
    }

    public synchronized MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        MessageStore rc = queues.get(destination);
        if (rc == null) {
            rc = new KahaMessageStore(getMapContainer(destination, "queue-data"), destination);
            messageStores.put(destination, rc);
            if (transactionStore != null) {
                rc = transactionStore.proxy(rc);
            }
            queues.put(destination, rc);
        }
        return rc;
    }

    public synchronized TopicMessageStore createTopicMessageStore(ActiveMQTopic destination)
        throws IOException {
        TopicMessageStore rc = topics.get(destination);
        if (rc == null) {
            Store store = getStore();
            MapContainer messageContainer = getMapContainer(destination, "topic-data");
            MapContainer subsContainer = getSubsMapContainer(destination.toString() + "-Subscriptions",
                                                             "topic-subs");
            ListContainer<TopicSubAck> ackContainer = store.getListContainer(destination.toString(),
                                                                             "topic-acks");
            ackContainer.setMarshaller(new TopicSubAckMarshaller());
            rc = new KahaTopicMessageStore(store, messageContainer, ackContainer, subsContainer, destination);
            messageStores.put(destination, rc);
            if (transactionStore != null) {
                rc = transactionStore.proxy(rc);
            }
            topics.put(destination, rc);
        }
        return rc;
    }

    protected MessageStore retrieveMessageStore(Object id) {
        MessageStore result = messageStores.get(id);
        return result;
    }

    public TransactionStore createTransactionStore() throws IOException {
        if (transactionStore == null) {
            while (true) {
                try {
                    Store store = getStore();
                    MapContainer container = store
                        .getMapContainer(PREPARED_TRANSACTIONS_NAME, "transactions");
                    container.setKeyMarshaller(new CommandMarshaller(wireFormat));
                    container.setValueMarshaller(new TransactionMarshaller(wireFormat));
                    container.load();
                    transactionStore = new KahaTransactionStore(this, container);
                    break;
                } catch (StoreLockedExcpetion e) {
                    LOG.info("Store is locked... waiting " + (STORE_LOCKED_WAIT_DELAY / 1000)
                             + " seconds for the Store to be unlocked.");
                    try {
                        Thread.sleep(STORE_LOCKED_WAIT_DELAY);
                    } catch (InterruptedException e1) {
                    }
                }
            }
        }
        return transactionStore;
    }

    public void beginTransaction(ConnectionContext context) {
    }

    public void commitTransaction(ConnectionContext context) throws IOException {
        if (theStore != null) {
            theStore.force();
        }
    }

    public void rollbackTransaction(ConnectionContext context) {
    }

    public void start() throws Exception {
        initialize();
    }

    public void stop() throws Exception {
        if (theStore != null) {
            theStore.close();
        }
    }

    public long getLastMessageBrokerSequenceId() throws IOException {
        return 0;
    }

    public void deleteAllMessages() throws IOException {
        if (theStore != null) {
            if (theStore.isInitialized()) {
                theStore.clear();
            } else {
                theStore.delete();
            }
        } else {
            StoreFactory.delete(getStoreName());
        }
    }

    protected MapContainer<MessageId, Message> getMapContainer(Object id, String containerName)
        throws IOException {
        Store store = getStore();
        MapContainer<MessageId, Message> container = store.getMapContainer(id, containerName);
        container.setKeyMarshaller(new MessageIdMarshaller());
        container.setValueMarshaller(new MessageMarshaller(wireFormat));
        container.load();
        return container;
    }

    protected MapContainer getSubsMapContainer(Object id, String containerName)
        throws IOException {
        Store store = getStore();
        MapContainer container = store.getMapContainer(id, containerName);
        container.setKeyMarshaller(Store.STRING_MARSHALLER);
        container.setValueMarshaller(createMessageMarshaller());
        container.load();
        return container;
    }

    protected Marshaller<Object> createMessageMarshaller() {
        return new CommandMarshaller(wireFormat);
    }

    protected ListContainer<TopicSubAck> getListContainer(Object id, String containerName) throws IOException {
        Store store = getStore();
        ListContainer<TopicSubAck> container = store.getListContainer(id, containerName);
        container.setMarshaller(createMessageMarshaller());
        container.load();
        return container;
    }

    /**
     * @param usageManager The UsageManager that is controlling the broker's
     *                memory usage.
     */
    public void setUsageManager(SystemUsage usageManager) {
    }

    /**
     * @return the maxDataFileLength
     */
    public long getMaxDataFileLength() {
        return maxDataFileLength;
    }

    /**
     * @param maxDataFileLength the maxDataFileLength to set
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryPropertyEditor"
     */
    public void setMaxDataFileLength(long maxDataFileLength) {
        this.maxDataFileLength = maxDataFileLength;
    }

    protected synchronized Store getStore() throws IOException {
        if (theStore == null) {
            theStore = StoreFactory.open(getStoreName(), "rw",storeSize);
            theStore.setMaxDataFileLength(maxDataFileLength);
        }
        return theStore;
    }

    private String getStoreName() {
        initialize();
        return directory.getAbsolutePath();
    }

    public String toString() {
        return "KahaPersistenceAdapter(" + getStoreName() + ")";
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public File getDirectory() {
        return this.directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public void checkpoint(boolean sync) throws IOException {
        if (sync) {
            getStore().force();
        }
    }
   
    public long size(){
       return storeSize.get();
    }

    private void initialize() {
        if (!initialized) {
            initialized = true;
            if (this.directory == null) {
                File file = new File(IOHelper.getDefaultDataDirectory());
                file = new File(file, brokerName + "-kahastore");
                setDirectory(file);
            }
            this.directory.mkdirs();
            wireFormat.setCacheEnabled(false);
            wireFormat.setTightEncodingEnabled(true);
        }
    }
  

}
