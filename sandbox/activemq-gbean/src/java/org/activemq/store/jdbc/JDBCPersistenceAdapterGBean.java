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
package org.activemq.store.jdbc;

import java.util.Map;

import javax.jms.JMSException;
import javax.sql.DataSource;

import org.apache.geronimo.gbean.GBeanInfo;
import org.apache.geronimo.gbean.GBeanInfoBuilder;
import org.apache.geronimo.gbean.GBeanLifecycle;
import org.activemq.store.MessageStore;
import org.activemq.store.PersistenceAdapter;
import org.activemq.store.TopicMessageStore;
import org.activemq.store.TransactionStore;

/**
 *
 */
public class JDBCPersistenceAdapterGBean implements GBeanLifecycle, PersistenceAdapter {

    JDBCPersistenceAdapter pa;
    private final ResourceManager resourceManager;
    
    public JDBCPersistenceAdapterGBean() {
        this(null);
    }
    
    public JDBCPersistenceAdapterGBean(ResourceManager dataSource) {
        this.resourceManager = dataSource;
    }
        
    public void doStart() throws Exception {
        pa = new JDBCPersistenceAdapter();
        pa.setDataSource((DataSource) resourceManager.$getResource());
        pa.start();
    }

    public void doStop() throws Exception {
        pa.stop();
        pa = null;
    }
    
    public void doFail() {
    }
    
    public static final GBeanInfo GBEAN_INFO;
    static {
        GBeanInfoBuilder infoFactory = new GBeanInfoBuilder("ActiveMQ JDBC Persistence", JDBCPersistenceAdapterGBean.class, "JMSPersistence");
        infoFactory.addReference("dataSource", ResourceManager.class);
        infoFactory.addInterface(PersistenceAdapter.class);
        infoFactory.setConstructor(new String[]{"dataSource"});
        GBEAN_INFO = infoFactory.getBeanInfo();
    }
    public static GBeanInfo getGBeanInfo() {
        return GBEAN_INFO;
    }

    public void beginTransaction() throws JMSException {
        pa.beginTransaction();
    }
    public void commitTransaction() throws JMSException {
        pa.commitTransaction();
    }
    public MessageStore createQueueMessageStore(String destinationName) throws JMSException {
        return pa.createQueueMessageStore(destinationName);
    }
    public TopicMessageStore createTopicMessageStore(String destinationName) throws JMSException {
        return pa.createTopicMessageStore(destinationName);
    }
    public TransactionStore createTransactionStore() throws JMSException {
        return pa.createTransactionStore();
    }
    public Map getInitialDestinations() {
        return pa.getInitialDestinations();
    }
    public void rollbackTransaction() {
        pa.rollbackTransaction();
    }
    public void start() throws JMSException {
    }
    public void stop() throws JMSException {
    }
}
