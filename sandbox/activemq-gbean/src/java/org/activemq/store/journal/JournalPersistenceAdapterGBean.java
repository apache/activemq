/**
 * 
 * Copyright 2004 Hiram Chirino
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
 * 
 **/
package org.activemq.store.journal;

import java.util.Map;

import javax.jms.JMSException;

import org.apache.geronimo.gbean.GBeanInfo;
import org.apache.geronimo.gbean.GBeanInfoBuilder;
import org.apache.geronimo.gbean.GBeanLifecycle;
import org.apache.geronimo.system.serverinfo.ServerInfo;
import org.activemq.store.MessageStore;
import org.activemq.store.PersistenceAdapter;
import org.activemq.store.TopicMessageStore;
import org.activemq.store.TransactionStore;

/**
 *
 */
public class JournalPersistenceAdapterGBean implements GBeanLifecycle, PersistenceAdapter {

    private final PersistenceAdapter longTermPersistence;
    private final ServerInfo serverInfo;
    private final String directory;
    private JournalPersistenceAdapter persistenceAdapter;
    private final String journalType;
    
    public JournalPersistenceAdapterGBean() {
        this(null, null, null, null);
    }
    
    public JournalPersistenceAdapterGBean(ServerInfo serverInfo, PersistenceAdapter longTermPersistence, String directory, String journalType) {
        this.serverInfo = serverInfo;
        this.longTermPersistence = longTermPersistence;
        this.directory = directory;
        this.journalType = journalType;
    }
        
    public void doStart() throws Exception {
        persistenceAdapter = new JournalPersistenceAdapter();
        persistenceAdapter.setLongTermPersistence(longTermPersistence);
        persistenceAdapter.setDirectory(serverInfo.resolve(directory));
        persistenceAdapter.setJournalType(journalType);
        persistenceAdapter.start();
    }

    public void doStop() throws Exception {
        persistenceAdapter.stop();
        persistenceAdapter = null;
    }
    
    public void doFail() {
    }
    
    public static final GBeanInfo GBEAN_INFO;
    static {
        GBeanInfoBuilder infoFactory = new GBeanInfoBuilder("ActiveMQ Persistence Journal", JournalPersistenceAdapterGBean.class, "JMSPersistence");
        infoFactory.addReference("serverInfo", ServerInfo.class);
        infoFactory.addReference("longTermPersistence", PersistenceAdapter.class);
        infoFactory.addAttribute("directory", String.class, true);
        infoFactory.addAttribute("journalType", String.class, true);
        infoFactory.addInterface(PersistenceAdapter.class);
        infoFactory.setConstructor(new String[]{"serverInfo", "longTermPersistence", "directory", "journalType"});
        GBEAN_INFO = infoFactory.getBeanInfo();
    }
    public static GBeanInfo getGBeanInfo() {
        return GBEAN_INFO;
    }

    public void beginTransaction() throws JMSException {
        persistenceAdapter.beginTransaction();
    }
    public void commitTransaction() throws JMSException {
        persistenceAdapter.commitTransaction();
    }
    
    public MessageStore createQueueMessageStore(String destinationName) throws JMSException {
        return persistenceAdapter.createQueueMessageStore(destinationName);
    }
    public TopicMessageStore createTopicMessageStore(String destinationName) throws JMSException {
        return persistenceAdapter.createTopicMessageStore(destinationName);
    }
    public TransactionStore createTransactionStore() throws JMSException {
        return persistenceAdapter.createTransactionStore();
    }
    public Map getInitialDestinations() {
        return persistenceAdapter.getInitialDestinations();
    }
    public void rollbackTransaction() {
        persistenceAdapter.rollbackTransaction();
    }
    public void start() throws JMSException {
    }
    public void stop() throws JMSException {
    }
}
