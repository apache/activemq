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
package org.apache.activemq.store.kahadb;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import javax.xml.bind.annotation.XmlAnyAttribute;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.filter.AnyDestination;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;
import org.apache.activemq.store.kahadb.data.KahaXATransactionId;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.IOHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link org.apache.activemq.store.PersistenceAdapter}  that supports
 * distribution of destinations across multiple kahaDB persistence adapters
 *
 * @org.apache.xbean.XBean element="mKahaDB"
 */
public class MultiKahaDBPersistenceAdapter extends DestinationMap implements PersistenceAdapter, BrokerServiceAware {
    static final Logger LOG = LoggerFactory.getLogger(MultiKahaDBPersistenceAdapter.class);

    final static ActiveMQDestination matchAll = new AnyDestination(new ActiveMQDestination[]{new ActiveMQQueue(">"), new ActiveMQTopic(">")});
    final int LOCAL_FORMAT_ID_MAGIC = Integer.valueOf(System.getProperty("org.apache.activemq.store.kahadb.MultiKahaDBTransactionStore.localXaFormatId", "61616"));

    BrokerService brokerService;
    List<KahaDBPersistenceAdapter> adapters = new LinkedList<KahaDBPersistenceAdapter>();
    private File directory = new File(IOHelper.getDefaultDataDirectory() + File.separator + "mKahaDB");

    MultiKahaDBTransactionStore transactionStore = new MultiKahaDBTransactionStore(this);

    // all local store transactions are XA, 2pc if more than one adapter involved
    TransactionIdTransformer transactionIdTransformer = new TransactionIdTransformer() {
        @Override
        public KahaTransactionInfo transform(TransactionId txid) {
            if (txid == null) {
                return null;
            }
            KahaTransactionInfo rc = new KahaTransactionInfo();
            KahaXATransactionId kahaTxId = new KahaXATransactionId();
            if (txid.isLocalTransaction()) {
                LocalTransactionId t = (LocalTransactionId) txid;
                kahaTxId.setBranchQualifier(new Buffer(Long.toString(t.getValue()).getBytes(Charset.forName("utf-8"))));
                kahaTxId.setGlobalTransactionId(new Buffer(t.getConnectionId().getValue().getBytes(Charset.forName("utf-8"))));
                kahaTxId.setFormatId(LOCAL_FORMAT_ID_MAGIC);
            } else {
                XATransactionId t = (XATransactionId) txid;
                kahaTxId.setBranchQualifier(new Buffer(t.getBranchQualifier()));
                kahaTxId.setGlobalTransactionId(new Buffer(t.getGlobalTransactionId()));
                kahaTxId.setFormatId(t.getFormatId());
            }
            rc.setXaTransacitonId(kahaTxId);
            return rc;
        }
    };

    /**
     * Sets the  FilteredKahaDBPersistenceAdapter entries
     *
     * @org.apache.xbean.ElementType class="org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter"
     */
    public void setFilteredPersistenceAdapters(List entries) {
        for (Object entry : entries) {
            FilteredKahaDBPersistenceAdapter filteredAdapter = (FilteredKahaDBPersistenceAdapter) entry;
            KahaDBPersistenceAdapter adapter = filteredAdapter.getPersistenceAdapter();
            if (filteredAdapter.getDestination() == null) {
                filteredAdapter.setDestination(matchAll);
            }
            adapter.setDirectory(new File(getDirectory(), nameFromDestinationFilter(filteredAdapter.getDestination())));

            // need a per store factory that will put the store in the branch qualifier to disiambiguate xid mbeans
            adapter.getStore().setTransactionIdTransformer(transactionIdTransformer);
            adapters.add(adapter);
        }
        super.setEntries(entries);
    }

    private String nameFromDestinationFilter(ActiveMQDestination destination) {
        return IOHelper.toFileSystemSafeName(destination.getQualifiedName());
    }

    public boolean isLocalXid(TransactionId xid) {
        return xid instanceof XATransactionId &&
                ((XATransactionId)xid).getFormatId() == LOCAL_FORMAT_ID_MAGIC;
    }

    public void beginTransaction(ConnectionContext context) throws IOException {
        throw new IllegalStateException();
    }

    public void checkpoint(final boolean sync) throws IOException {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.checkpoint(sync);
        }
    }

    public void commitTransaction(ConnectionContext context) throws IOException {
        throw new IllegalStateException();
    }

    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        PersistenceAdapter persistenceAdapter = getMatchingPersistenceAdapter(destination);
        return transactionStore.proxy(persistenceAdapter.createTransactionStore(), persistenceAdapter.createQueueMessageStore(destination));
    }

    private PersistenceAdapter getMatchingPersistenceAdapter(ActiveMQDestination destination) {
        Object result = this.chooseValue(destination);
        if (result == null) {
            throw new RuntimeException("No matching persistence adapter configured for destination: " + destination + ", options:" + adapters);
        }
        return ((FilteredKahaDBPersistenceAdapter) result).getPersistenceAdapter();
    }

    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        PersistenceAdapter persistenceAdapter = getMatchingPersistenceAdapter(destination);
        return transactionStore.proxy(persistenceAdapter.createTransactionStore(), persistenceAdapter.createTopicMessageStore(destination));
    }

    public TransactionStore createTransactionStore() throws IOException {
        return transactionStore;
    }

    public void deleteAllMessages() throws IOException {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.deleteAllMessages();
        }
        transactionStore.deleteAllMessages();
    }

    public Set<ActiveMQDestination> getDestinations() {
        Set<ActiveMQDestination> results = new HashSet<ActiveMQDestination>();
        for (PersistenceAdapter persistenceAdapter : adapters) {
            results.addAll(persistenceAdapter.getDestinations());
        }
        return results;
    }

    public long getLastMessageBrokerSequenceId() throws IOException {
        long maxId = -1;
        for (PersistenceAdapter persistenceAdapter : adapters) {
            maxId = Math.max(maxId, persistenceAdapter.getLastMessageBrokerSequenceId());
        }
        return maxId;
    }

    public long getLastProducerSequenceId(ProducerId id) throws IOException {
        long maxId = -1;
        for (PersistenceAdapter persistenceAdapter : adapters) {
            maxId = Math.max(maxId, persistenceAdapter.getLastProducerSequenceId(id));
        }
        return maxId;
    }

    public void removeQueueMessageStore(ActiveMQQueue destination) {
        getMatchingPersistenceAdapter(destination).removeQueueMessageStore(destination);
    }

    public void removeTopicMessageStore(ActiveMQTopic destination) {
        getMatchingPersistenceAdapter(destination).removeTopicMessageStore(destination);
    }

    public void rollbackTransaction(ConnectionContext context) throws IOException {
        throw new IllegalStateException();
    }

    public void setBrokerName(String brokerName) {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.setBrokerName(brokerName);
        }
    }

    public void setUsageManager(SystemUsage usageManager) {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.setUsageManager(usageManager);
        }
    }

    public long size() {
        long size = 0;
        for (PersistenceAdapter persistenceAdapter : adapters) {
            size += persistenceAdapter.size();
        }
        return size;
    }

    public void start() throws Exception {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.start();
        }
    }

    public void stop() throws Exception {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.stop();
        }
    }

    public File getDirectory() {
        return this.directory;
    }

    @Override
    public void setDirectory(File dir) {
        this.directory = directory;
    }

    public void setBrokerService(BrokerService brokerService) {
        for (KahaDBPersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.setBrokerService(brokerService);
        }
        this.brokerService = brokerService;
    }

    public BrokerService getBrokerService() {
        return brokerService;
    }

    public void setTransactionStore(MultiKahaDBTransactionStore transactionStore) {
        this.transactionStore = transactionStore;
    }

    /**
     * Set the max file length of the transaction journal
     * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can
     * be used
     *
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryIntPropertyEditor"
     */
    public void setJournalMaxFileLength(int maxFileLength) {
        transactionStore.setJournalMaxFileLength(maxFileLength);
    }

    public int getJournalMaxFileLength() {
        return transactionStore.getJournalMaxFileLength();
    }

    /**
     * Set the max write batch size of  the transaction journal
     * When set using Xbean, values of the form "20 Mb", "1024kb", and "1g" can
     * be used
     *
     * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.MemoryIntPropertyEditor"
     */
    public void setJournalWriteBatchSize(int journalWriteBatchSize) {
        transactionStore.setJournalMaxWriteBatchSize(journalWriteBatchSize);
    }

    public int getJournalMaxWriteBatchSize() {
        return transactionStore.getJournalMaxWriteBatchSize();
    }

    @Override
    public String toString() {
        String path = getDirectory() != null ? getDirectory().getAbsolutePath() : "DIRECTORY_NOT_SET";
        return "MultiKahaDBPersistenceAdapter[" + path + "]" + adapters;
    }

}
