/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.store.kahadaptor;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.activeio.command.WireFormat;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreFactory;
import org.apache.activemq.kaha.StringMarshaller;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionStore;
import org.apache.activemq.store.memory.MemoryTransactionStore;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
/**
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 1.4 $
 */
public class KahaPersistentAdaptor implements PersistenceAdapter{
    MemoryTransactionStore transactionStore;
    ConcurrentHashMap topics=new ConcurrentHashMap();
    ConcurrentHashMap queues=new ConcurrentHashMap();
    private boolean useExternalMessageReferences;
    private WireFormat wireFormat = new OpenWireFormat();
    Store store;

    public KahaPersistentAdaptor(File dir) throws IOException{
        if (!dir.exists()){
            dir.mkdirs();
        }
        String name = dir.getAbsolutePath() + File.separator + "kaha.db";
        store=StoreFactory.open(name,"rw");
        
    }

    public Set getDestinations(){
        Set rc=new HashSet();
        for(Iterator i=store.getMapContainerIds().iterator();i.hasNext();){
            Object obj=i.next();
            if(obj instanceof ActiveMQDestination){
                rc.add(obj);
            }
        }
        return rc;
    }

    public synchronized MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException{
        MessageStore rc=(MessageStore) queues.get(destination);
        if(rc==null){
            rc=new KahaMessageStore(getMapContainer(destination),destination);
            if(transactionStore!=null){
                rc=transactionStore.proxy(rc);
            }
            queues.put(destination,rc);
        }
        return rc;
    }

    public synchronized TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException{
        TopicMessageStore rc=(TopicMessageStore) topics.get(destination);
        if(rc==null){
            MapContainer messageContainer=getMapContainer(destination);
            MapContainer subsContainer=getMapContainer(destination.toString()+"-Subscriptions");
            MapContainer ackContainer=store.getMapContainer(destination.toString()+"-Acks");
            ackContainer.setKeyMarshaller(new StringMarshaller());
            ackContainer.setValueMarshaller(new AtomicIntegerMarshaller());
            ackContainer.load();
            rc=new KahaTopicMessageStore(store,messageContainer,ackContainer,subsContainer,destination);
            if(transactionStore!=null){
                rc=transactionStore.proxy(rc);
            }
            topics.put(destination,rc);
        }
        return rc;
    }

    public TransactionStore createTransactionStore() throws IOException{
        if(transactionStore==null){
            transactionStore=new MemoryTransactionStore();
        }
        return transactionStore;
    }

    public void beginTransaction(ConnectionContext context){}

    public void commitTransaction(ConnectionContext context) throws IOException{
        store.force();
    }

    public void rollbackTransaction(ConnectionContext context){}

    public void start() throws Exception{}

    public void stop() throws Exception{}

    public long getLastMessageBrokerSequenceId() throws IOException{
        return 0;
    }

    public void deleteAllMessages() throws IOException{
        if(store!=null){
            store.clear();
        }
        if(transactionStore!=null){
            transactionStore.delete();
        }
    }

    public boolean isUseExternalMessageReferences(){
        return useExternalMessageReferences;
    }

    public void setUseExternalMessageReferences(boolean useExternalMessageReferences){
        this.useExternalMessageReferences=useExternalMessageReferences;
    }

    protected MapContainer getMapContainer(Object id) throws IOException{
        MapContainer container=store.getMapContainer(id);
        container.setKeyMarshaller(new StringMarshaller());
        if(useExternalMessageReferences){
            container.setValueMarshaller(new StringMarshaller());
        }else{
            container.setValueMarshaller(new CommandMarshaller(wireFormat));
        }
        container.load();
        return container;
    }

    /**
     * @param usageManager The UsageManager that is controlling the broker's memory usage.
     */
    public void setUsageManager(UsageManager usageManager) {
    }
}
