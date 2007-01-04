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

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.memory.UsageListener;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.util.LRUCache;
/**
 * An implementation of {@link org.apache.activemq.store.MessageStore} which uses a JPS Container
 * 
 * @version $Revision: 1.7 $
 */
public class KahaMessageStore implements MessageStore, UsageListener{
    protected final ActiveMQDestination destination;
    protected final ListContainer<Object> messageContainer;
    protected StoreEntry batchEntry = null;
    protected final LRUCache<MessageId, StoreEntry> cache;
    protected UsageManager usageManager;

    public KahaMessageStore(ListContainer<Object> container,ActiveMQDestination destination, int maximumCacheSize) throws IOException{
        this.messageContainer=container;
        this.destination=destination;
        this.cache=new LRUCache<MessageId, StoreEntry>(maximumCacheSize,maximumCacheSize,0.75f,false);
        // populate the cache
        StoreEntry entry=messageContainer.getFirst();
        int count = 0;
        if(entry!=null){
            do{
                MessageId id = getMessageId(messageContainer.get(entry));
                cache.put(id,entry);
                entry = messageContainer.getNext(entry);
                count++;
            }while(entry!=null && count < maximumCacheSize);
        }
    }
    
    protected MessageId getMessageId(Object object) {
		return ((Message)object).getMessageId();
	}

	public Object getId(){
        return messageContainer.getId();
    }

    public synchronized void addMessage(ConnectionContext context,Message message) throws IOException{
        StoreEntry item=messageContainer.placeLast(message);
        // TODO: we should do the following but it is not need if the message is being added within a persistence
        // transaction
        // but since I can't tell if one is running right now.. I'll leave this out for now.
        // if( message.isResponseRequired() ) {
        // messageContainer.force();
        // }
        cache.put(message.getMessageId(),item);
    }

    public synchronized Message getMessage(MessageId identity) throws IOException{
        Message result=null;
        StoreEntry entry=cache.get(identity);
        if(entry!=null){
            entry = messageContainer.refresh(entry);
            result = (Message)messageContainer.get(entry);
        }else{    
            for (entry = messageContainer.getFirst();entry != null; entry = messageContainer.getNext(entry)) {
                Message msg=(Message)messageContainer.get(entry);
                if(msg.getMessageId().equals(identity)){
                    result=msg;
                    cache.put(identity,entry);
                    break;
                }
            }
        }
        return result;
    }

    protected void recover(MessageRecoveryListener listener, Object msg) throws Exception {
        listener.recoverMessage((Message)msg);
	}

    public void removeMessage(ConnectionContext context,MessageAck ack) throws IOException{
        removeMessage(ack.getLastMessageId());
    }

    public synchronized void removeMessage(MessageId msgId) throws IOException{
        StoreEntry entry=cache.remove(msgId);
        if(entry!=null){
            entry = messageContainer.refresh(entry);
            messageContainer.remove(entry);
        }else{
            for (entry = messageContainer.getFirst();entry != null; entry = messageContainer.getNext(entry)) {
                MessageId id=getMessageId(messageContainer.get(entry));
                if(id.equals(msgId)){
                    messageContainer.remove(entry);
                    break;
                }
            }
        }
        if (messageContainer.isEmpty()) {
            resetBatching();
        }
    }

    public synchronized void recover(MessageRecoveryListener listener) throws Exception{
        for(Iterator iter=messageContainer.iterator();iter.hasNext();){
            recover(listener, iter.next());
        }
        listener.finished();
    }

    public void start() {
        if( this.usageManager != null )
            this.usageManager.addUsageListener(this);
    }

    public void stop() {
        if( this.usageManager != null )
            this.usageManager.removeUsageListener(this);
    }

    public synchronized void removeAllMessages(ConnectionContext context) throws IOException{
        messageContainer.clear();
        cache.clear();
    }

    public ActiveMQDestination getDestination(){
        return destination;
    }

    public synchronized void delete(){
        messageContainer.clear();
        cache.clear();
    }
    
    /**
     * @param usageManager The UsageManager that is controlling the destination's memory usage.
     */
    public void setUsageManager(UsageManager usageManager) {
        this.usageManager = usageManager;
    }

    /**
     * @return the number of messages held by this destination
     * @see org.apache.activemq.store.MessageStore#getMessageCount()
     */
    public int getMessageCount(){
       return messageContainer.size();
    }

    /**
     * @param id
     * @return null
     * @throws Exception
     * @see org.apache.activemq.store.MessageStore#getPreviousMessageIdToDeliver(org.apache.activemq.command.MessageId)
     */
    public MessageId getPreviousMessageIdToDeliver(MessageId id) throws Exception{
        return null;
    }

    /**
     * @param lastMessageId
     * @param maxReturned
     * @param listener
     * @throws Exception
     * @see org.apache.activemq.store.MessageStore#recoverNextMessages(org.apache.activemq.command.MessageId, int, org.apache.activemq.store.MessageRecoveryListener)
     */
    public synchronized void recoverNextMessages(int maxReturned,MessageRecoveryListener listener) throws Exception{
        StoreEntry entry = batchEntry;
        if (entry == null) {
            entry= messageContainer.getFirst();
        }else {
            entry=messageContainer.refresh(entry);
            entry=messageContainer.getNext(entry);
        }
        if(entry!=null){
            int count = 0;
            do{
                Object msg=messageContainer.get(entry);
                if(msg!=null){
                	recover(listener, msg);
                    count++;
                }
                batchEntry = entry;
                entry=messageContainer.getNext(entry);
            }while(entry!=null&&count<maxReturned&&listener.hasSpace());
        }
        listener.finished();
        
    }

    /**
     * @param nextToDispatch
     * @see org.apache.activemq.store.MessageStore#resetBatching(org.apache.activemq.command.MessageId)
     */
    public void resetBatching(){
        batchEntry = null;
        
    }
    
    /**
     * @return true if the store supports cursors
     */
    public boolean isSupportForCursors() {
        return true;
    }

    /**
     * @param memoryManager
     * @param oldPercentUsage
     * @param newPercentUsage
     * @see org.apache.activemq.memory.UsageListener#onMemoryUseChanged(org.apache.activemq.memory.UsageManager, int, int)
     */
    public synchronized void onMemoryUseChanged(UsageManager memoryManager,int oldPercentUsage,int newPercentUsage){
        if(newPercentUsage==100){
            cache.clear();
        }
    }

}
