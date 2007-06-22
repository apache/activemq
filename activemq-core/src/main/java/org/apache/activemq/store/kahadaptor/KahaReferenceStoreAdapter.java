/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.kahadaptor;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.kaha.CommandMarshaller;
import org.apache.activemq.kaha.ContainerId;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.MessageIdMarshaller;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreFactory;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ReferenceStore;
import org.apache.activemq.store.ReferenceStoreAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TopicReferenceStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KahaReferenceStoreAdapter extends KahaPersistenceAdapter implements ReferenceStoreAdapter {
    private static final Log log = LogFactory.getLog(KahaPersistenceAdapter.class);
   private static final String STORE_STATE = "store-state";
   private static final String RECORD_REFERENCES = "record-references";
    private MapContainer stateMap;
	private Map<Integer,AtomicInteger>recordReferences = new HashMap<Integer,AtomicInteger>();
    private ListContainer durableSubscribers;
    private boolean storeValid;
    private Store stateStore;

	
    public synchronized MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException{
    	throw new RuntimeException("Use createQueueReferenceStore instead");
    }

    public synchronized TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException{
    	throw new RuntimeException("Use createTopicReferenceStore instead");
    }
    
    @Override
    public synchronized void start() throws Exception{
        super.start();
        Store store=getStateStore();
        boolean empty=store.getMapContainerIds().isEmpty();
        stateMap=store.getMapContainer("state",STORE_STATE);
        stateMap.load();
        if(!empty){
            AtomicBoolean status=(AtomicBoolean)stateMap.get(STORE_STATE);
            if(status!=null){
                storeValid=status.get();
            }
            if(storeValid){
                if(stateMap.containsKey(RECORD_REFERENCES)){
                    recordReferences=(Map<Integer,AtomicInteger>)stateMap.get(RECORD_REFERENCES);
                }
            }
        }
        stateMap.put(STORE_STATE,new AtomicBoolean());
        durableSubscribers = store.getListContainer("durableSubscribers");
        durableSubscribers.setMarshaller(new CommandMarshaller());
    }
       
    @Override
    public synchronized void stop() throws Exception {
        stateMap.put(RECORD_REFERENCES,recordReferences);
        stateMap.put(STORE_STATE,new AtomicBoolean(true));
        if (this.stateStore != null) {
            this.stateStore.close();
            this.stateStore = null;
            this.stateMap = null;
        }
        super.stop();        
    }
    
    
    public boolean isStoreValid() {
        return storeValid;
    }
    

	public ReferenceStore createQueueReferenceStore(ActiveMQQueue destination) throws IOException {
		ReferenceStore rc=(ReferenceStore)queues.get(destination);
        if(rc==null){
            rc=new KahaReferenceStore(this,getMapReferenceContainer(destination,"queue-data"),destination);
            messageStores.put(destination,rc);
//            if(transactionStore!=null){
//                rc=transactionStore.proxy(rc);
//            }
            queues.put(destination,rc);
        }
        return rc;
	}

	public TopicReferenceStore createTopicReferenceStore(ActiveMQTopic destination) throws IOException {
		TopicReferenceStore rc=(TopicReferenceStore)topics.get(destination);
        if(rc==null){
            Store store=getStore();
            MapContainer messageContainer=getMapReferenceContainer(destination,"topic-data");
            MapContainer subsContainer=getSubsMapContainer(destination.toString()+"-Subscriptions","blob");
            ListContainer ackContainer=store.getListContainer(destination.toString(),"topic-acks");
            ackContainer.setMarshaller(new TopicSubAckMarshaller());
            rc=new KahaTopicReferenceStore(store,this,messageContainer,ackContainer,subsContainer,destination);
            messageStores.put(destination,rc);
//            if(transactionStore!=null){
//                rc=transactionStore.proxy(rc);
//            }
            topics.put(destination,rc);
        }
        return rc;
	}

	public void buildReferenceFileIdsInUse() throws IOException {
		
        recordReferences = new HashMap<Integer,AtomicInteger>();
		
		Set<ActiveMQDestination> destinations = getDestinations();
		for (ActiveMQDestination destination : destinations) {
			if( destination.isQueue() ) {
				KahaReferenceStore store = (KahaReferenceStore) createQueueReferenceStore((ActiveMQQueue) destination);
				store.addReferenceFileIdsInUse();
			} else {
				KahaTopicReferenceStore store = (KahaTopicReferenceStore) createTopicReferenceStore((ActiveMQTopic) destination);
				store.addReferenceFileIdsInUse();
			}
        }		
	}
    
        
    protected MapContainer<MessageId,ReferenceRecord> getMapReferenceContainer(Object id,String containerName) throws IOException{
        Store store=getStore();
        MapContainer<MessageId, ReferenceRecord> container=store.getMapContainer(id,containerName);
        container.setKeyMarshaller(new MessageIdMarshaller());
        container.setValueMarshaller(new ReferenceRecordMarshaller());        
        container.load();
        return container;
    }
    
    synchronized void addInterestInRecordFile(int recordNumber) {
        Integer key = new Integer(recordNumber);
        AtomicInteger rr = recordReferences.get(key);
        if (rr == null) {
            rr = new AtomicInteger();
            recordReferences.put(key,rr);
        }
        rr.incrementAndGet();
    }
    
    synchronized void removeInterestInRecordFile(int recordNumber) {
        Integer key = new Integer(recordNumber);
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
    public Set<Integer> getReferenceFileIdsInUse() throws IOException{
        return recordReferences.keySet();
    }

    /**
     * 
     * @throws IOException 
     * @see org.apache.activemq.store.ReferenceStoreAdapter#clearMessages()
     */
    public void clearMessages() throws IOException{
        deleteAllMessages();
    }
    
    /**
     * 
     * @throws IOException 
     * @see org.apache.activemq.store.ReferenceStoreAdapter#recoverState()
     */
    public void recoverState() throws IOException{
        for (Iterator i = durableSubscribers.iterator();i.hasNext();) {
            SubscriptionInfo info = (SubscriptionInfo)i.next();
            TopicReferenceStore ts = createTopicReferenceStore((ActiveMQTopic)info.getDestination());
            ts.addSubsciption(info.getClientId(),info.getSubcriptionName(),info.getSelector(),false);
        }
        
    }
    
    @Override
    public void setDirectory(File directory){
        File file = new File(directory,"data");
        super.setDirectory(file);
        this.stateStore=createStateStore(directory);
    }
    
    protected synchronized Store getStateStore() throws IOException{
        if(this.stateStore==null){
            File stateDirectory=new File(getDirectory(),"kr-state");
            stateDirectory.mkdirs();
            this.stateStore=createStateStore(getDirectory());
        }
        return this.stateStore;
    }
    
    private Store createStateStore(File directory) {
        File stateDirectory=new File(directory,"state");
        stateDirectory.mkdirs();
        try{
            return StoreFactory.open(stateDirectory.getAbsolutePath(),"rw");
        }catch(IOException e){
            log.error("Failed to create the state store",e);
        }
        return null;
        
    }
    
    protected void addSubscriberState(SubscriptionInfo info) throws IOException {
        durableSubscribers.add(info);
    }
    
    protected void removeSubscriberState(SubscriptionInfo info) {
        durableSubscribers.remove(info);
    }

   
        
}
    
	

