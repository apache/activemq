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
package org.apache.activemq.store.memory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.ProxyMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @org.apache.xbean.XBean
 *
 */
public class MemoryPersistenceAdapter implements PersistenceAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryPersistenceAdapter.class);

    MemoryTransactionStore transactionStore;
    ConcurrentHashMap<ActiveMQDestination, TopicMessageStore> topics = new ConcurrentHashMap<ActiveMQDestination, TopicMessageStore>();
    ConcurrentHashMap<ActiveMQDestination, MessageStore> queues = new ConcurrentHashMap<ActiveMQDestination, MessageStore>();
    private boolean useExternalMessageReferences;

    @Override
    public Set<ActiveMQDestination> getDestinations() {
        Set<ActiveMQDestination> rc = new HashSet<ActiveMQDestination>(queues.size() + topics.size());
        for (Iterator<ActiveMQDestination> iter = queues.keySet().iterator(); iter.hasNext();) {
            rc.add(iter.next());
        }
        for (Iterator<ActiveMQDestination> iter = topics.keySet().iterator(); iter.hasNext();) {
            rc.add(iter.next());
        }
        return rc;
    }

    public static MemoryPersistenceAdapter newInstance(File file) {
        return new MemoryPersistenceAdapter();
    }

    @Override
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        MessageStore rc = queues.get(destination);
        if (rc == null) {
            rc = new MemoryMessageStore(destination);
            if (transactionStore != null) {
                rc = transactionStore.proxy(rc);
            }
            queues.put(destination, rc);
        }
        return rc;
    }

    @Override
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        TopicMessageStore rc = topics.get(destination);
        if (rc == null) {
            rc = new MemoryTopicMessageStore(destination);
            if (transactionStore != null) {
                rc = transactionStore.proxy(rc);
            }
            topics.put(destination, rc);
        }
        return rc;
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     *
     * @param destination Destination to forget
     */
    @Override
    public void removeQueueMessageStore(ActiveMQQueue destination) {
        queues.remove(destination);
    }

    /**
     * Cleanup method to remove any state associated with the given destination
     *
     * @param destination Destination to forget
     */
    @Override
    public void removeTopicMessageStore(ActiveMQTopic destination) {
        topics.remove(destination);
    }

    @Override
    public TransactionStore createTransactionStore() throws IOException {
        if (transactionStore == null) {
            transactionStore = new MemoryTransactionStore(this);
        }
        return transactionStore;
    }

    @Override
    public void beginTransaction(ConnectionContext context) {
    }

    @Override
    public void commitTransaction(ConnectionContext context) {
    }

    @Override
    public void rollbackTransaction(ConnectionContext context) {
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public long getLastMessageBrokerSequenceId() throws IOException {
        return 0;
    }

    @Override
    public void deleteAllMessages() throws IOException {
        for (Iterator<TopicMessageStore> iter = topics.values().iterator(); iter.hasNext();) {
            MemoryMessageStore store = asMemoryMessageStore(iter.next());
            if (store != null) {
                store.delete();
            }
        }
        for (Iterator<MessageStore> iter = queues.values().iterator(); iter.hasNext();) {
            MemoryMessageStore store = asMemoryMessageStore(iter.next());
            if (store != null) {
                store.delete();
            }
        }

        if (transactionStore != null) {
            transactionStore.delete();
        }
    }

    public boolean isUseExternalMessageReferences() {
        return useExternalMessageReferences;
    }

    public void setUseExternalMessageReferences(boolean useExternalMessageReferences) {
        this.useExternalMessageReferences = useExternalMessageReferences;
    }

    protected MemoryMessageStore asMemoryMessageStore(Object value) {
        if (value instanceof MemoryMessageStore) {
            return (MemoryMessageStore)value;
        }
        if (value instanceof ProxyMessageStore) {
            MessageStore delegate = ((ProxyMessageStore)value).getDelegate();
            if (delegate instanceof MemoryMessageStore) {
                return (MemoryMessageStore) delegate;
            }
        }
        LOG.warn("Expected an instance of MemoryMessageStore but was: " + value);
        return null;
    }

    /**
     * @param usageManager The UsageManager that is controlling the broker's
     *                memory usage.
     */
    @Override
    public void setUsageManager(SystemUsage usageManager) {
    }

    @Override
    public String toString() {
        return "MemoryPersistenceAdapter";
    }

    @Override
    public void setBrokerName(String brokerName) {
    }

    @Override
    public void setDirectory(File dir) {
    }

    @Override
    public File getDirectory(){
        return null;
    }

    @Override
    public void checkpoint(boolean sync) throws IOException {
    }

    @Override
    public long size(){
        return 0;
    }

    public void setCreateTransactionStore(boolean create) throws IOException {
        if (create) {
            createTransactionStore();
        }
    }

    @Override
    public long getLastProducerSequenceId(ProducerId id) {
        // memory map does duplicate suppression
        return -1;
    }

    @Override
    public JobSchedulerStore createJobSchedulerStore() throws IOException, UnsupportedOperationException {
        // We could eventuall implement an in memory scheduler.
        throw new UnsupportedOperationException();
    }
}
