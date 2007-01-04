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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.ReferenceStore;
import org.apache.activemq.store.ReferenceStoreAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TopicReferenceStore;
import org.apache.activemq.store.ReferenceStore.ReferenceData;

public class KahaReferenceStoreAdapter extends KahaPersistenceAdapter implements ReferenceStoreAdapter {

	private MapContainer<Integer, Integer> fileReferences;

	public KahaReferenceStoreAdapter(File dir) throws IOException {
		super(dir);
	}

    public synchronized MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException{
    	throw new RuntimeException("Use createQueueReferenceStore instead");
    }

    public synchronized TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException{
    	throw new RuntimeException("Use createTopicReferenceStore instead");
    }
    
    @Override
    public void start() throws Exception {
    	super.start();
    	
        Store store=getStore();
        fileReferences=store.getMapContainer("file-references");
        fileReferences.setKeyMarshaller(new IntegerMarshaller());
        fileReferences.setValueMarshaller(new IntegerMarshaller());
        fileReferences.load();        
    }
    
    public static class ReferenceRecord {

    	public String messageId;
    	public ReferenceData data;

		public ReferenceRecord() {			
		}
		public ReferenceRecord(String messageId, ReferenceData data) {
			this.messageId = messageId;
			this.data = data;
		}
	}

    protected Marshaller<Object> createMessageMarshaller() {
		return new Marshaller<Object>() {
		    public void writePayload(Object object,DataOutput dataOut) throws IOException{
		    	ReferenceRecord rr = (ReferenceRecord) object;
		        dataOut.writeUTF(rr.messageId);
		        dataOut.writeInt(rr.data.getFileId());
		        dataOut.writeInt(rr.data.getOffset());
		        dataOut.writeLong(rr.data.getExpiration());		        
		    }
		    public Object readPayload(DataInput dataIn) throws IOException{
		    	ReferenceRecord rr = new ReferenceRecord();
		    	rr.messageId = dataIn.readUTF();
		    	rr.data = new ReferenceData();
		    	rr.data.setFileId(dataIn.readInt());
		    	rr.data.setOffset(dataIn.readInt());
		    	rr.data.setExpiration(dataIn.readLong());
		    	return rr;
		    }
		};
	}

	public ReferenceStore createQueueReferenceStore(ActiveMQQueue destination) throws IOException {
		ReferenceStore rc=(ReferenceStore)queues.get(destination);
        if(rc==null){
            rc=new KahaReferenceStore(getListContainer(destination,"queue-data"),destination,maximumDestinationCacheSize, fileReferences);
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
            ListContainer messageContainer=getListContainer(destination,"topic-data");
            MapContainer subsContainer=getMapContainer(destination.toString()+"-Subscriptions","topic-subs");
            ListContainer ackContainer=store.getListContainer(destination.toString(),"topic-acks");
            ackContainer.setMarshaller(new TopicSubAckMarshaller());
            rc=new KahaTopicReferenceStore(store,messageContainer,ackContainer,subsContainer,destination,maximumDestinationCacheSize, fileReferences);
            messageStores.put(destination,rc);
//            if(transactionStore!=null){
//                rc=transactionStore.proxy(rc);
//            }
            topics.put(destination,rc);
        }
        return rc;
	}

	public Set<Integer> getReferenceFileIdsInUse() throws IOException {
		
		Set<Integer> rc = new HashSet<Integer>();
		
		Set<ActiveMQDestination> destinations = getDestinations();
		for (ActiveMQDestination destination : destinations) {
			if( destination.isQueue() ) {
				KahaReferenceStore store = (KahaReferenceStore) createQueueReferenceStore((ActiveMQQueue) destination);
				store.addReferenceFileIdsInUse(rc);
			} else {
				KahaTopicReferenceStore store = (KahaTopicReferenceStore) createTopicReferenceStore((ActiveMQTopic) destination);
				store.addReferenceFileIdsInUse(rc);
			}
		}
		
		return rc;
		
	}
	
	
}
