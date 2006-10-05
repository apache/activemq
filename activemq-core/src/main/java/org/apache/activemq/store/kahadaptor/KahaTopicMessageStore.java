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
import java.util.Map;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.StringMarshaller;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;
/**
 * @version $Revision: 1.5 $
 */
public class KahaTopicMessageStore  implements TopicMessageStore{
    private ActiveMQDestination destination;
    private ListContainer ackContainer;
    private ListContainer messageContainer;
    private Map subscriberContainer;
    private Store store;
    private Map subscriberAcks=new ConcurrentHashMap();

    public KahaTopicMessageStore(Store store,ListContainer messageContainer,ListContainer ackContainer,
                    MapContainer subsContainer,ActiveMQDestination destination) throws IOException{
        this.messageContainer = messageContainer;
        this.destination = destination;
        this.store=store;
        this.ackContainer=ackContainer;
        subscriberContainer=subsContainer;
        // load all the Ack containers
        for(Iterator i=subscriberContainer.keySet().iterator();i.hasNext();){
            Object key=i.next();
            addSubscriberAckContainer(key);
        }
    }

    public synchronized void addMessage(ConnectionContext context,Message message) throws IOException{
        int subscriberCount=subscriberAcks.size();
        if(subscriberCount>0){
            StoreEntry entry = messageContainer.placeLast(message);
            TopicSubAck tsa = new TopicSubAck();
            tsa.setCount(subscriberCount);
            tsa.setStoreEntry(entry);
            StoreEntry ackEntry = ackContainer.placeLast(tsa);
            for(Iterator i=subscriberAcks.keySet().iterator();i.hasNext();){
                Object key=i.next();
                ListContainer container=store.getListContainer(key,"durable-subs");
                container.add(ackEntry);
            }
            
        }
    }

    public synchronized void acknowledge(ConnectionContext context,String clientId,String subscriptionName,
            MessageId messageId) throws IOException{
        String subcriberId=getSubscriptionKey(clientId,subscriptionName);
        ListContainer container=(ListContainer)subscriberAcks.get(subcriberId);
        if(container!=null){
            StoreEntry ackEntry=(StoreEntry)container.removeFirst();
            if(ackEntry!=null){
                TopicSubAck tsa=(TopicSubAck)ackContainer.get(ackEntry);
                if(tsa!=null){
                    if(tsa.decrementCount()<=0){
                        ackContainer.remove(ackEntry);
                        messageContainer.remove(tsa.getStoreEntry());
                    }else {
                       ackContainer.update(ackEntry,tsa);
                    }
                }
            }
        }
    }

    public SubscriptionInfo lookupSubscription(String clientId,String subscriptionName) throws IOException{
        return (SubscriptionInfo) subscriberContainer.get(getSubscriptionKey(clientId,subscriptionName));
    }

    public synchronized void addSubsciption(String clientId,String subscriptionName,String selector,boolean retroactive)
                    throws IOException{
        SubscriptionInfo info=new SubscriptionInfo();
        info.setDestination(destination);
        info.setClientId(clientId);
        info.setSelector(selector);
        info.setSubcriptionName(subscriptionName);
        String key=getSubscriptionKey(clientId,subscriptionName);
        // if already exists - won't add it again as it causes data files
        // to hang around
        if(!subscriberContainer.containsKey(key)){
            subscriberContainer.put(key,info);
        }
        addSubscriberAckContainer(key);
    }

    public synchronized void deleteSubscription(String clientId,String subscriptionName){
        String key=getSubscriptionKey(clientId,subscriptionName);
        subscriberContainer.remove(key);
        ListContainer list=(ListContainer) subscriberAcks.get(key);
        for(Iterator i=list.iterator();i.hasNext();){
            StoreEntry ackEntry=(StoreEntry)i.next();
            if(ackEntry!=null){
                TopicSubAck tsa=(TopicSubAck)ackContainer.get(ackEntry);
                if(tsa!=null){
                    if(tsa.decrementCount()<=0){
                        ackContainer.remove(ackEntry);
                        messageContainer.remove(tsa.getStoreEntry());
                    }else {
                       ackContainer.update(ackEntry,tsa);
                    }
                }
            }
        }
    }

    public void recoverSubscription(String clientId,String subscriptionName,MessageRecoveryListener listener)
                    throws Exception{
        String key=getSubscriptionKey(clientId,subscriptionName);
        ListContainer list=(ListContainer) subscriberAcks.get(key);
        if(list!=null){
            for(Iterator i=list.iterator();i.hasNext();){
                StoreEntry entry = (StoreEntry)i.next();
                Object msg=messageContainer.get(entry);
                if(msg!=null){
                    if(msg.getClass()==String.class){
                        listener.recoverMessageReference((String) msg);
                    }else{
                        listener.recoverMessage((Message) msg);
                    }
                }
                listener.finished();
            }
        }else{
            listener.finished();
        }
    }
    
    public void recoverNextMessages(String clientId,String subscriptionName,MessageId lastMessageId,int maxReturned,
                    MessageRecoveryListener listener) throws Exception{
        String key=getSubscriptionKey(clientId,subscriptionName);
        ListContainer list=(ListContainer) subscriberAcks.get(key);
        if(list!=null){
            boolean startFound=false;
            int count = 0;
            for(Iterator i=list.iterator();i.hasNext() && count < maxReturned;){
                StoreEntry entry = (StoreEntry)i.next();
                Object msg=messageContainer.get(entry);
                if(msg!=null){
                    if(msg.getClass()==String.class){
                        String ref=msg.toString();
                        if (startFound || lastMessageId == null){
                            listener.recoverMessageReference(ref);
                            count++;
                        }
                        else if(startFound||ref.equals(lastMessageId.toString())){
                            startFound=true;
                        }
                    }else{
                        Message message=(Message) msg;
                        if(startFound||message.getMessageId().equals(lastMessageId)){
                            startFound=true;
                        }else{
                            listener.recoverMessage(message);
                            count++;
                        }
                    }
                }
                listener.finished();
            }
        }else{
            listener.finished();
        }
    }

    public void delete(){
        messageContainer.clear();
        ackContainer.clear();
        subscriberContainer.clear();
    }

    public SubscriptionInfo[] getAllSubscriptions() throws IOException{
        return (SubscriptionInfo[]) subscriberContainer.values().toArray(
                        new SubscriptionInfo[subscriberContainer.size()]);
    }

    protected String getSubscriptionKey(String clientId,String subscriberName){
        String result=clientId+":";
        result+=subscriberName!=null?subscriberName:"NOT_SET";
        return result;
    }

    protected void addSubscriberAckContainer(Object key) throws IOException{
        ListContainer container=store.getListContainer(key,"topic-subs");
        Marshaller marshaller=new StoreEntryMarshaller();
        container.setMarshaller(marshaller);
        subscriberAcks.put(key,container);
    }

    public Message getNextMessageToDeliver(String clientId,String subscriptionName) throws IOException{
        String key=getSubscriptionKey(clientId,subscriptionName);
        ListContainer list=(ListContainer) subscriberAcks.get(key);
        StoreEntry entry = (StoreEntry)list.get(0);
        Message msg=(Message)messageContainer.get(entry);
        return msg;
    }

    public int getMessageCount(String clientId,String subscriberName) throws IOException{
        String key=getSubscriptionKey(clientId,subscriberName);
        ListContainer list=(ListContainer) subscriberAcks.get(key);
        return list.size();
    }

    /**
     * @param context
     * @param messageId
     * @param expirationTime
     * @param messageRef
     * @throws IOException
     * @see org.apache.activemq.store.MessageStore#addMessageReference(org.apache.activemq.broker.ConnectionContext, org.apache.activemq.command.MessageId, long, java.lang.String)
     */
    public void addMessageReference(ConnectionContext context,MessageId messageId,long expirationTime,String messageRef) throws IOException{
        messageContainer.add(messageRef);
        
    }

    /**
     * @return the destination
     * @see org.apache.activemq.store.MessageStore#getDestination()
     */
    public ActiveMQDestination getDestination(){
       return destination;
    }

    /**
     * @param identity
     * @return the Message
     * @throws IOException
     * @see org.apache.activemq.store.MessageStore#getMessage(org.apache.activemq.command.MessageId)
     */
    public Message getMessage(MessageId identity) throws IOException{
        Message result = null;
        for (Iterator i = messageContainer.iterator(); i.hasNext();){
            Message msg = (Message)i.next();
            if (msg.getMessageId().equals(identity)) {
                result = msg;
                break;
            }
        }
        return result;
    }

    /**
     * @param identity
     * @return String
     * @throws IOException
     * @see org.apache.activemq.store.MessageStore#getMessageReference(org.apache.activemq.command.MessageId)
     */
    public String getMessageReference(MessageId identity) throws IOException{
        return null;
    }

    /**
     * @throws Exception
     * @see org.apache.activemq.store.MessageStore#recover(org.apache.activemq.store.MessageRecoveryListener)
     */
    public void recover(MessageRecoveryListener listener) throws Exception{
        for(Iterator iter=messageContainer.iterator();iter.hasNext();){
            Object msg=iter.next();
            if(msg.getClass()==String.class){
                listener.recoverMessageReference((String) msg);
            }else{
                listener.recoverMessage((Message) msg);
            }
        }
        listener.finished();
        
    }

    /**
     * @param context
     * @throws IOException
     * @see org.apache.activemq.store.MessageStore#removeAllMessages(org.apache.activemq.broker.ConnectionContext)
     */
    public void removeAllMessages(ConnectionContext context) throws IOException{
        messageContainer.clear();
        
    }

    /**
     * @param context
     * @param ack
     * @throws IOException
     * @see org.apache.activemq.store.MessageStore#removeMessage(org.apache.activemq.broker.ConnectionContext, org.apache.activemq.command.MessageAck)
     */
    public void removeMessage(ConnectionContext context,MessageAck ack) throws IOException{
        for (Iterator i = messageContainer.iterator(); i.hasNext();){
            Message msg = (Message)i.next();
            if (msg.getMessageId().equals(ack.getLastMessageId())) {
               i.remove();
                break;
            }
        }
        
    }

    /**
     * @param usageManager
     * @see org.apache.activemq.store.MessageStore#setUsageManager(org.apache.activemq.memory.UsageManager)
     */
    public void setUsageManager(UsageManager usageManager){
        // TODO Auto-generated method stub
        
    }

    /**
     * @throws Exception
     * @see org.apache.activemq.Service#start()
     */
    public void start() throws Exception{
        // TODO Auto-generated method stub
        
    }

    /**
     * @throws Exception
     * @see org.apache.activemq.Service#stop()
     */
    public void stop() throws Exception{
        // TODO Auto-generated method stub
        
    }

    
}
