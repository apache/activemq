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

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicReferenceStore;
import org.apache.activemq.store.kahadaptor.KahaReferenceStoreAdapter.ReferenceRecord;

public class KahaTopicReferenceStore extends KahaTopicMessageStore implements TopicReferenceStore {

	private final MapContainer<Integer, Integer> fileReferences;

	public KahaTopicReferenceStore(Store store, ListContainer messageContainer, ListContainer ackContainer, MapContainer subsContainer, ActiveMQDestination destination, int maximumCacheSize, MapContainer<Integer, Integer> fileReferences) throws IOException {
		super(store, messageContainer, ackContainer, subsContainer, destination, maximumCacheSize);
		this.fileReferences = fileReferences;
	}

	@Override
	protected MessageId getMessageId(Object object) {
		return new MessageId(((ReferenceRecord)object).messageId);
	}

	@Override
	public synchronized void addMessage(ConnectionContext context, Message message) throws IOException {
		throw new RuntimeException("Use addMessageReference instead");
	}
		
	@Override
	public synchronized Message getMessage(MessageId identity) throws IOException {
		throw new RuntimeException("Use addMessageReference instead");
	}
	
	@Override
	protected void recover(MessageRecoveryListener listener, Object msg) throws Exception {
		ReferenceRecord record = (ReferenceRecord) msg;
		listener.recoverMessageReference(new MessageId(record.messageId));
	}

	public void addMessageReference(ConnectionContext context, MessageId messageId, ReferenceData data) throws IOException {
		
		ReferenceRecord record = new ReferenceRecord(messageId.toString(), data);
		
        int subscriberCount=subscriberMessages.size();
        if(subscriberCount>0){
            StoreEntry messageEntry=messageContainer.placeLast(record);
            TopicSubAck tsa=new TopicSubAck();
            tsa.setCount(subscriberCount);
            tsa.setMessageEntry(messageEntry);
            StoreEntry ackEntry=ackContainer.placeLast(tsa);
            for(Iterator i=subscriberMessages.values().iterator();i.hasNext();){
                TopicSubContainer container=(TopicSubContainer)i.next();
                ConsumerMessageRef ref=new ConsumerMessageRef();
                ref.setAckEntry(ackEntry);
                ref.setMessageEntry(messageEntry);
                container.add(ref);
            }
        }
        
	}

	public ReferenceData getMessageReference(MessageId identity) throws IOException {
		
		ReferenceRecord result=null;
        StoreEntry entry=(StoreEntry)cache.get(identity);
        if(entry!=null){
            entry = messageContainer.refresh(entry);
            result = (ReferenceRecord)messageContainer.get(entry);
        }else{    
            for (entry = messageContainer.getFirst();entry != null; entry = messageContainer.getNext(entry)) {
            	ReferenceRecord msg=(ReferenceRecord)messageContainer.get(entry);
                if(msg.messageId.equals(identity.toString())){
                    result=msg;
                    cache.put(identity,entry);
                    break;
                }
            }
        }
        if( result == null )
        	return null;
        return result.data;
	}

	public void addReferenceFileIdsInUse(Set<Integer> rc) {
        for (StoreEntry entry = ackContainer.getFirst();entry != null; entry = ackContainer.getNext(entry)) {
        	TopicSubAck subAck=(TopicSubAck)ackContainer.get(entry);
        	if( subAck.getCount() > 0 ) {
        		ReferenceRecord rr = (ReferenceRecord)messageContainer.get(subAck.getMessageEntry());
                rc.add(rr.data.getFileId());
        	}
        }
	}
	

}
