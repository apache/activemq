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
import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.transaction.xa.Xid;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.Lockable;
import org.apache.activemq.broker.LockableServiceSupport;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.filter.AnyDestination;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.NoLocalSubscriptionAware;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.SharedFileLocker;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionIdTransformer;
import org.apache.activemq.store.TransactionIdTransformerAware;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.store.kahadb.MessageDatabase.DEFAULT_DIRECTORY;

/**
 * An implementation of {@link org.apache.activemq.store.PersistenceAdapter}  that supports
 * distribution of destinations across multiple kahaDB persistence adapters
 *
 * @org.apache.xbean.XBean element="mKahaDB"
 */
public class MultiKahaDBPersistenceAdapter extends LockableServiceSupport implements PersistenceAdapter,
    BrokerServiceAware, NoLocalSubscriptionAware {

    static final Logger LOG = LoggerFactory.getLogger(MultiKahaDBPersistenceAdapter.class);

    final static ActiveMQDestination matchAll = new AnyDestination(new ActiveMQDestination[]{new ActiveMQQueue(">"), new ActiveMQTopic(">")});
    final int LOCAL_FORMAT_ID_MAGIC = Integer.valueOf(System.getProperty("org.apache.activemq.store.kahadb.MultiKahaDBTransactionStore.localXaFormatId", "61616"));

    final class DelegateDestinationMap extends DestinationMap {
        @Override
        public void setEntries(List<DestinationMapEntry>  entries) {
            super.setEntries(entries);
        }
    };
    final DelegateDestinationMap destinationMap = new DelegateDestinationMap();

    List<PersistenceAdapter> adapters = new CopyOnWriteArrayList<PersistenceAdapter>();
    private File directory = new File(IOHelper.getDefaultDataDirectory() + File.separator + "mKahaDB");

    MultiKahaDBTransactionStore transactionStore = new MultiKahaDBTransactionStore(this);

    // all local store transactions are XA, 2pc if more than one adapter involved
    TransactionIdTransformer transactionIdTransformer = new TransactionIdTransformer() {
        @Override
        public TransactionId transform(TransactionId txid) {
            if (txid == null) {
                return null;
            }
            if (txid.isLocalTransaction()) {
                final LocalTransactionId t = (LocalTransactionId) txid;
                return new XATransactionId(new Xid() {
                    @Override
                    public int getFormatId() {
                        return LOCAL_FORMAT_ID_MAGIC;
                    }

                    @Override
                    public byte[] getGlobalTransactionId() {
                        return t.getConnectionId().getValue().getBytes(Charset.forName("utf-8"));
                    }

                    @Override
                    public byte[] getBranchQualifier() {
                        return Long.toString(t.getValue()).getBytes(Charset.forName("utf-8"));
                    }
                });
            } else {
                return txid;
            }
        }
    };

    /**
     * Sets the  FilteredKahaDBPersistenceAdapter entries
     *
     * @org.apache.xbean.ElementType class="org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter"
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void setFilteredPersistenceAdapters(List entries) {
        for (Object entry : entries) {
            FilteredKahaDBPersistenceAdapter filteredAdapter = (FilteredKahaDBPersistenceAdapter) entry;
            PersistenceAdapter adapter = filteredAdapter.getPersistenceAdapter();
            if (filteredAdapter.getDestination() == null) {
                filteredAdapter.setDestination(matchAll);
            }

            if (filteredAdapter.isPerDestination()) {
                configureDirectory(adapter, null);
                // per destination adapters will be created on demand or during recovery
                continue;
            } else {
                configureDirectory(adapter, nameFromDestinationFilter(filteredAdapter.getDestination()));
            }

            configureAdapter(adapter);
            adapters.add(adapter);
        }
        destinationMap.setEntries(entries);
    }

    public static String nameFromDestinationFilter(ActiveMQDestination destination) {
        if (destination.getQualifiedName().length() > IOHelper.getMaxFileNameLength()) {
            LOG.warn("Destination name is longer than 'MaximumFileNameLength' system property, " +
                     "potential problem with recovery can result from name truncation.");
        }

        return IOHelper.toFileSystemSafeName(destination.getQualifiedName());
    }

    public boolean isLocalXid(TransactionId xid) {
        return xid instanceof XATransactionId &&
                ((XATransactionId)xid).getFormatId() == LOCAL_FORMAT_ID_MAGIC;
    }

    @Override
    public void beginTransaction(ConnectionContext context) throws IOException {
        throw new IllegalStateException();
    }

    @Override
    public void checkpoint(final boolean cleanup) throws IOException {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.checkpoint(cleanup);
        }
    }

    @Override
    public void commitTransaction(ConnectionContext context) throws IOException {
        throw new IllegalStateException();
    }

    @Override
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        PersistenceAdapter persistenceAdapter = getMatchingPersistenceAdapter(destination);
        return transactionStore.proxy(persistenceAdapter.createTransactionStore(), persistenceAdapter.createQueueMessageStore(destination));
    }

    private PersistenceAdapter getMatchingPersistenceAdapter(ActiveMQDestination destination) throws IOException {
        Object result = destinationMap.chooseValue(destination);
        if (result == null) {
            throw new RuntimeException("No matching persistence adapter configured for destination: " + destination + ", options:" + adapters);
        }
        FilteredKahaDBPersistenceAdapter filteredAdapter = (FilteredKahaDBPersistenceAdapter) result;
        if (filteredAdapter.getDestination() == matchAll && filteredAdapter.isPerDestination()) {
            filteredAdapter = addAdapter(filteredAdapter, destination);
            if (LOG.isTraceEnabled()) {
                LOG.trace("created per destination adapter for: " + destination  + ", " + result);
            }
        }
        startAdapter(filteredAdapter.getPersistenceAdapter(), destination.getQualifiedName());
        LOG.debug("destination {} matched persistence adapter {}", destination.getQualifiedName(), filteredAdapter.getPersistenceAdapter());
        return filteredAdapter.getPersistenceAdapter();
    }

    private void startAdapter(PersistenceAdapter kahaDBPersistenceAdapter, String destination) {
        try {
            kahaDBPersistenceAdapter.start();
        } catch (Exception e) {
            RuntimeException detail = new RuntimeException("Failed to start per destination persistence adapter for destination: " + destination + ", options:" + adapters, e);
            LOG.error(detail.toString(), e);
            throw detail;
        }
    }

    private void stopAdapter(PersistenceAdapter kahaDBPersistenceAdapter, String destination) {
        try {
            kahaDBPersistenceAdapter.stop();
        } catch (Exception e) {
            RuntimeException detail = new RuntimeException("Failed to stop per destination persistence adapter for destination: " + destination + ", options:" + adapters, e);
            LOG.error(detail.toString(), e);
            throw detail;
        }
    }

    @Override
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        PersistenceAdapter persistenceAdapter = getMatchingPersistenceAdapter(destination);
        return transactionStore.proxy(persistenceAdapter.createTransactionStore(), persistenceAdapter.createTopicMessageStore(destination));
    }

    @Override
    public TransactionStore createTransactionStore() throws IOException {
        return transactionStore;
    }

    @Override
    public void deleteAllMessages() throws IOException {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.deleteAllMessages();
        }
        transactionStore.deleteAllMessages();
        IOHelper.deleteChildren(getDirectory());
        for (Object o : destinationMap.get(new AnyDestination(new ActiveMQDestination[]{new ActiveMQQueue(">"), new ActiveMQTopic(">")}))) {
            if (o instanceof FilteredKahaDBPersistenceAdapter) {
                FilteredKahaDBPersistenceAdapter filteredKahaDBPersistenceAdapter = (FilteredKahaDBPersistenceAdapter) o;
                if (filteredKahaDBPersistenceAdapter.getPersistenceAdapter().getDirectory() != DEFAULT_DIRECTORY) {
                    IOHelper.deleteChildren(filteredKahaDBPersistenceAdapter.getPersistenceAdapter().getDirectory());
                }
                if (filteredKahaDBPersistenceAdapter.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter) {
                    KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter) filteredKahaDBPersistenceAdapter.getPersistenceAdapter();
                    if (kahaDBPersistenceAdapter.getIndexDirectory() != null) {
                        IOHelper.deleteChildren(kahaDBPersistenceAdapter.getIndexDirectory());
                    }
                }
            }
        }
    }

    @Override
    public Set<ActiveMQDestination> getDestinations() {
        Set<ActiveMQDestination> results = new HashSet<ActiveMQDestination>();
        for (PersistenceAdapter persistenceAdapter : adapters) {
            results.addAll(persistenceAdapter.getDestinations());
        }
        return results;
    }

    @Override
    public long getLastMessageBrokerSequenceId() throws IOException {
        long maxId = -1;
        for (PersistenceAdapter persistenceAdapter : adapters) {
            maxId = Math.max(maxId, persistenceAdapter.getLastMessageBrokerSequenceId());
        }
        return maxId;
    }

    @Override
    public long getLastProducerSequenceId(ProducerId id) throws IOException {
        long maxId = -1;
        for (PersistenceAdapter persistenceAdapter : adapters) {
            maxId = Math.max(maxId, persistenceAdapter.getLastProducerSequenceId(id));
        }
        return maxId;
    }

    @Override
    public void allowIOResumption() {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.allowIOResumption();
        }
    }

    @Override
    public void removeQueueMessageStore(ActiveMQQueue destination) {
        PersistenceAdapter adapter = null;
        try {
            adapter = getMatchingPersistenceAdapter(destination);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (adapter instanceof PersistenceAdapter && adapter.getDestinations().isEmpty()) {
            adapter.removeQueueMessageStore(destination);
            removeMessageStore(adapter, destination);
            destinationMap.remove(destination, adapter);
        }
    }

    @Override
    public void removeTopicMessageStore(ActiveMQTopic destination) {
        PersistenceAdapter adapter = null;
        try {
            adapter = getMatchingPersistenceAdapter(destination);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (adapter instanceof PersistenceAdapter && adapter.getDestinations().isEmpty()) {
            adapter.removeTopicMessageStore(destination);
            removeMessageStore(adapter, destination);
            destinationMap.remove(destination, adapter);
        }
    }

    private void removeMessageStore(PersistenceAdapter adapter, ActiveMQDestination destination) {
        stopAdapter(adapter, destination.toString());
        File adapterDir = adapter.getDirectory();
        if (adapterDir != null) {
            if (IOHelper.deleteFile(adapterDir)) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("deleted per destination adapter directory for: " + destination);
                }
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("failed to deleted per destination adapter directory for: " + destination);
                }
            }
        }
    }

    @Override
    public void rollbackTransaction(ConnectionContext context) throws IOException {
        throw new IllegalStateException();
    }

    @Override
    public void setBrokerName(String brokerName) {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.setBrokerName(brokerName);
        }
    }

    @Override
    public void setUsageManager(SystemUsage usageManager) {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.setUsageManager(usageManager);
        }
    }

    @Override
    public long size() {
        long size = 0;
        for (PersistenceAdapter persistenceAdapter : adapters) {
            size += persistenceAdapter.size();
        }
        return size;
    }

    @Override
    public void doStart() throws Exception {
        Object result = destinationMap.chooseValue(matchAll);
        if (result != null) {
            FilteredKahaDBPersistenceAdapter filteredAdapter = (FilteredKahaDBPersistenceAdapter) result;
            if (filteredAdapter.getDestination() == matchAll && filteredAdapter.isPerDestination()) {
                findAndRegisterExistingAdapters(filteredAdapter);
            }
        }
        for (PersistenceAdapter persistenceAdapter : adapters) {
            persistenceAdapter.start();
        }
    }

    private void findAndRegisterExistingAdapters(FilteredKahaDBPersistenceAdapter template) throws IOException {
        FileFilter destinationNames = new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.getName().startsWith("queue#") || file.getName().startsWith("topic#");
            }
        };
        File[] candidates = template.getPersistenceAdapter().getDirectory().listFiles(destinationNames);
        if (candidates != null) {
            for (File candidate : candidates) {
                registerExistingAdapter(template, candidate);
            }
        }
    }

    private void registerExistingAdapter(FilteredKahaDBPersistenceAdapter filteredAdapter, File candidate) throws IOException {
        PersistenceAdapter adapter = adapterFromTemplate(filteredAdapter, candidate.getName());
        startAdapter(adapter, candidate.getName());
        Set<ActiveMQDestination> destinations = adapter.getDestinations();
        if (destinations.size() != 0) {
            registerAdapter(filteredAdapter, adapter, destinations.toArray(new ActiveMQDestination[]{})[0]);
        } else {
            stopAdapter(adapter, candidate.getName());
        }
    }

    private FilteredKahaDBPersistenceAdapter addAdapter(FilteredKahaDBPersistenceAdapter filteredAdapter, ActiveMQDestination destination) throws IOException {
        PersistenceAdapter adapter = adapterFromTemplate(filteredAdapter, nameFromDestinationFilter(destination));
        return registerAdapter(filteredAdapter, adapter, destination);
    }

    private PersistenceAdapter adapterFromTemplate(FilteredKahaDBPersistenceAdapter template, String destinationName) throws IOException {
        PersistenceAdapter adapter = kahaDBFromTemplate(template.getPersistenceAdapter());
        configureAdapter(adapter);
        configureDirectory(adapter, destinationName);
        configureIndexDirectory(adapter, template.getPersistenceAdapter(), destinationName);
        return adapter;
    }

    private void configureIndexDirectory(PersistenceAdapter adapter, PersistenceAdapter template, String destinationName) {
        if (template instanceof KahaDBPersistenceAdapter) {
            KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter) template;
            if (kahaDBPersistenceAdapter.getIndexDirectory() != null) {
                if (adapter instanceof KahaDBPersistenceAdapter) {
                    File directory = kahaDBPersistenceAdapter.getIndexDirectory();
                    if (destinationName != null) {
                        directory = new File(directory, destinationName);
                    }
                    ((KahaDBPersistenceAdapter)adapter).setIndexDirectory(directory);
                }
            }
        }
    }

    private void configureDirectory(PersistenceAdapter adapter, String fileName) {
        File directory = null;
        File defaultDir = DEFAULT_DIRECTORY;
        try {
            defaultDir = adapter.getClass().newInstance().getDirectory();
        } catch (Exception e) {
        }
        if (defaultDir.equals(adapter.getDirectory())) {
            // not set so inherit from mkahadb
            directory = getDirectory();
        } else {
            directory = adapter.getDirectory();
        }

        if (fileName != null) {
            directory = new File(directory, fileName);
        }
        adapter.setDirectory(directory);
    }

    private FilteredKahaDBPersistenceAdapter registerAdapter(FilteredKahaDBPersistenceAdapter template, PersistenceAdapter adapter, ActiveMQDestination destination) {
        adapters.add(adapter);
        FilteredKahaDBPersistenceAdapter result = new FilteredKahaDBPersistenceAdapter(template, destination, adapter);
        destinationMap.put(destination, result);
        return result;
    }

    private void configureAdapter(PersistenceAdapter adapter) {
        // need a per store factory that will put the store in the branch qualifier to disiambiguate xid mbeans
        ((TransactionIdTransformerAware)adapter).setTransactionIdTransformer(transactionIdTransformer);
        if (isUseLock()) {
            if( adapter instanceof Lockable ) {
                ((Lockable)adapter).setUseLock(false);
            }
        }
        if( adapter instanceof BrokerServiceAware ) {
            ((BrokerServiceAware)adapter).setBrokerService(getBrokerService());
        }
    }

    private PersistenceAdapter kahaDBFromTemplate(PersistenceAdapter template) throws IOException {
        try {
            Map<String, Object> configuration = new HashMap<String, Object>();
            IntrospectionSupport.getProperties(template, configuration, null);
            PersistenceAdapter adapter = template.getClass().newInstance();
            IntrospectionSupport.setProperties(adapter, configuration);
            return adapter;
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        for (PersistenceAdapter persistenceAdapter : adapters) {
            stopper.stop(persistenceAdapter);
        }
    }

    @Override
    public File getDirectory() {
        return this.directory;
    }

    @Override
    public void setDirectory(File directory) {
        this.directory = directory;
    }

    @Override
    public void init() throws Exception {
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        super.setBrokerService(brokerService);
        for (PersistenceAdapter persistenceAdapter : adapters) {
            if( persistenceAdapter instanceof BrokerServiceAware ) {
                ((BrokerServiceAware)persistenceAdapter).setBrokerService(getBrokerService());
            }
        }
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

    public int getJournalWriteBatchSize() {
        return transactionStore.getJournalMaxWriteBatchSize();
    }


    public void setJournalCleanupInterval(long journalCleanupInterval) {
        transactionStore.setJournalCleanupInterval(journalCleanupInterval);
    }

    public long getJournalCleanupInterval() {
        return transactionStore.getJournalCleanupInterval();
    }

    public void setCheckForCorruption(boolean checkForCorruption) {
        transactionStore.setCheckForCorruption(checkForCorruption);
    }

    public boolean isCheckForCorruption() {
        return transactionStore.isCheckForCorruption();
    }

    public List<PersistenceAdapter> getAdapters() {
        return Collections.unmodifiableList(adapters);
    }

    @Override
    public String toString() {
        String path = getDirectory() != null ? getDirectory().getAbsolutePath() : "DIRECTORY_NOT_SET";
        return "MultiKahaDBPersistenceAdapter[" + path + "]" + adapters;
    }

    @Override
    public Locker createDefaultLocker() throws IOException {
        SharedFileLocker locker = new SharedFileLocker();
        locker.configure(this);
        return locker;
    }

    @Override
    public JobSchedulerStore createJobSchedulerStore() throws IOException, UnsupportedOperationException {
        return new JobSchedulerStoreImpl();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.store.NoLocalSubscriptionAware#isPersistNoLocal()
     */
    @Override
    public boolean isPersistNoLocal() {
        // Prior to v11 the broker did not store the noLocal value for durable subs.
        return brokerService.getStoreOpenWireVersion() >= 11;
    }
}
