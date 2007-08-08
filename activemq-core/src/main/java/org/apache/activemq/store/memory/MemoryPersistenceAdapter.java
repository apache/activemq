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

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @org.apache.xbean.XBean
 * @version $Revision: 1.4 $
 */
public class MemoryPersistenceAdapter implements PersistenceAdapter {
    private static final Log log = LogFactory.getLog(MemoryPersistenceAdapter.class);

    MemoryTransactionStore transactionStore;
    ConcurrentHashMap topics = new ConcurrentHashMap();
    ConcurrentHashMap queues = new ConcurrentHashMap();
    private boolean useExternalMessageReferences;

    public Set getDestinations() {
        Set rc = new HashSet(queues.size() + topics.size());
        for (Iterator iter = queues.keySet().iterator(); iter.hasNext();) {
            rc.add(iter.next());
        }
        for (Iterator iter = topics.keySet().iterator(); iter.hasNext();) {
            rc.add(iter.next());
        }
        return rc;
    }

    public static MemoryPersistenceAdapter newInstance(File file) {
        return new MemoryPersistenceAdapter();
    }

    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        MessageStore rc = (MessageStore)queues.get(destination);
        if (rc == null) {
            rc = new MemoryMessageStore(destination);
            if (transactionStore != null) {
                rc = transactionStore.proxy(rc);
            }
            queues.put(destination, rc);
        }
        return rc;
    }

    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        TopicMessageStore rc = (TopicMessageStore)topics.get(destination);
        if (rc == null) {
            rc = new MemoryTopicMessageStore(destination);
            if (transactionStore != null) {
                rc = transactionStore.proxy(rc);
            }
            topics.put(destination, rc);
        }
        return rc;
    }

    public TransactionStore createTransactionStore() throws IOException {
        if (transactionStore == null) {
            transactionStore = new MemoryTransactionStore();
        }
        return transactionStore;
    }

    public void beginTransaction(ConnectionContext context) {
    }

    public void commitTransaction(ConnectionContext context) {
    }

    public void rollbackTransaction(ConnectionContext context) {
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public long getLastMessageBrokerSequenceId() throws IOException {
        return 0;
    }

    public void deleteAllMessages() throws IOException {
        for (Iterator iter = topics.values().iterator(); iter.hasNext();) {
            MemoryMessageStore store = asMemoryMessageStore(iter.next());
            if (store != null) {
                store.delete();
            }
        }
        for (Iterator iter = queues.values().iterator(); iter.hasNext();) {
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
        log.warn("Expected an instance of MemoryMessageStore but was: " + value);
        return null;
    }

    /**
     * @param usageManager The UsageManager that is controlling the broker's
     *                memory usage.
     */
    public void setUsageManager(UsageManager usageManager) {
    }

    public String toString() {
        return "MemoryPersistenceAdapter";
    }

    public void setBrokerName(String brokerName) {
    }

    public void setDirectory(File dir) {
    }

    public void checkpoint(boolean sync) throws IOException {
    }
}
