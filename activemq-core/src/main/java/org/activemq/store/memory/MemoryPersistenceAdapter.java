/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.store.memory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.activemq.broker.ConnectionContext;
import org.activemq.command.ActiveMQQueue;
import org.activemq.command.ActiveMQTopic;
import org.activemq.store.MessageStore;
import org.activemq.store.PersistenceAdapter;
import org.activemq.store.TopicMessageStore;
import org.activemq.store.TransactionStore;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * @org.xbean.XBean
 * 
 * @version $Revision: 1.4 $
 */
public class MemoryPersistenceAdapter implements PersistenceAdapter {

    MemoryTransactionStore transactionStore;
    ConcurrentHashMap topics = new ConcurrentHashMap();
    ConcurrentHashMap queues = new ConcurrentHashMap();
    private boolean useExternalMessageReferences;
    
    public Set getDestinations() {
        Set rc = new HashSet(queues.size()+topics.size());
        for (Iterator iter = queues.keySet().iterator(); iter.hasNext();) {
            rc.add( iter.next() );
        }
        for (Iterator iter = topics.keySet().iterator(); iter.hasNext();) {
            rc.add( iter.next() );
        }
        return rc;
    }

    public static MemoryPersistenceAdapter newInstance(File file) {
        return new MemoryPersistenceAdapter();
    }
    
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException {
        MessageStore rc = (MessageStore)queues.get(destination);
        if(rc==null) {
            rc = new MemoryMessageStore(destination);
            if( transactionStore !=null ) {
                rc = transactionStore.proxy(rc);
            }
            queues.put(destination, rc);
        }
        return rc;
    }

    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException {
        TopicMessageStore rc = (TopicMessageStore)topics.get(destination);
        if(rc==null) {
            rc = new MemoryTopicMessageStore(destination);
            if( transactionStore !=null ) {
                rc = transactionStore.proxy(rc);
            }
            topics.put(destination, rc);
        }
        return rc;
    }

    public TransactionStore createTransactionStore() throws IOException {
        if( transactionStore==null ) {
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
            MemoryMessageStore store = (MemoryMessageStore) iter.next();
            store.delete();
        }
        for (Iterator iter = queues.values().iterator(); iter.hasNext();) {
            MemoryMessageStore store = (MemoryMessageStore) iter.next();
            store.delete();
        }
        transactionStore.delete();
    }

    public boolean isUseExternalMessageReferences() {
        return useExternalMessageReferences;
    }

    public void setUseExternalMessageReferences(boolean useExternalMessageReferences) {
        this.useExternalMessageReferences = useExternalMessageReferences;
    }

}
