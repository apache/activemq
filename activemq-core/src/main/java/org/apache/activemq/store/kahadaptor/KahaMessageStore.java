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
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.util.LRUCache;
/**
 * An implementation of {@link org.apache.activemq.store.MessageStore} which uses a JPS Container
 * 
 * @version $Revision: 1.7 $
 */
public class KahaMessageStore implements MessageStore{
    protected final ActiveMQDestination destination;
    protected final ListContainer messageContainer;
    protected final LRUCache cache;

    public KahaMessageStore(ListContainer container,ActiveMQDestination destination, int maximumCacheSize) throws IOException{
        this.messageContainer=container;
        this.destination=destination;
        this.cache=new LRUCache(maximumCacheSize,maximumCacheSize,0.75f,false);
        // populate the cache
        StoreEntry entry=messageContainer.getFirst();
        int count = 0;
        if(entry!=null){
            do{
                Message msg = (Message)messageContainer.get(entry);
                cache.put(msg.getMessageId(),entry);
                entry = messageContainer.getNext(entry);
                count++;
            }while(entry!=null && count < maximumCacheSize);
        }
    }
    
    public Object getId(){
        return messageContainer.getId();
    }

    public synchronized void addMessage(ConnectionContext context,Message message) throws IOException{
        StoreEntry item = messageContainer.placeLast(message);
        cache.put(message.getMessageId(),item);
    }

    public synchronized void addMessageReference(ConnectionContext context,MessageId messageId,long expirationTime,String messageRef)
                    throws IOException{
        throw new RuntimeException("Not supported");
    }

    public synchronized Message getMessage(MessageId identity) throws IOException{
        Message result=null;
        StoreEntry entry=(StoreEntry)cache.remove(identity);
        if(entry!=null){
            result = (Message)messageContainer.get(entry);
        }else{
       
        for(Iterator i=messageContainer.iterator();i.hasNext();){
            Message msg=(Message)i.next();
            if(msg.getMessageId().equals(identity)){
                result=msg;
                break;
            }
        }
        }
        return result;
    }

    public String getMessageReference(MessageId identity) throws IOException{
        return null;
    }

    public void removeMessage(ConnectionContext context,MessageAck ack) throws IOException{
        removeMessage(ack.getLastMessageId());
    }

    public synchronized void removeMessage(MessageId msgId) throws IOException{
        StoreEntry entry=(StoreEntry)cache.remove(msgId);
        if(entry!=null){
            messageContainer.remove(entry);
        }else{
            for(Iterator i=messageContainer.iterator();i.hasNext();){
                Message msg=(Message)i.next();
                if(msg.getMessageId().equals(msgId)){
                    i.remove();
                    break;
                }
            }
        }
    }

    public synchronized void recover(MessageRecoveryListener listener) throws Exception{
        for(Iterator iter=messageContainer.iterator();iter.hasNext();){
            listener.recoverMessage((Message)iter.next());
        }
        listener.finished();
    }

    public void start() {}

    public void stop() {}

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
    }

}
