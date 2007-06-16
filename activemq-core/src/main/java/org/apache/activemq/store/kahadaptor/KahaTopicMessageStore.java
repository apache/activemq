/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.store.kahadaptor;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;

/**
 * @version $Revision: 1.5 $
 */
public class KahaTopicMessageStore extends KahaMessageStore implements TopicMessageStore{

    protected ListContainer<TopicSubAck> ackContainer;
    private Map subscriberContainer;
    private Store store;
    protected Map subscriberMessages=new ConcurrentHashMap();

    public KahaTopicMessageStore(Store store,MapContainer messageContainer,ListContainer<TopicSubAck> ackContainer,
            MapContainer subsContainer,ActiveMQDestination destination) throws IOException{
        super(messageContainer,destination);
        this.store=store;
        this.ackContainer=ackContainer;
        subscriberContainer=subsContainer;
        // load all the Ack containers
        for(Iterator i=subscriberContainer.keySet().iterator();i.hasNext();){
            Object key=i.next();
            addSubscriberMessageContainer(key);
        }
    }

    @Override
    public synchronized void addMessage(ConnectionContext context,Message message) throws IOException{
        int subscriberCount=subscriberMessages.size();
        if(subscriberCount>0){
            MessageId id = message.getMessageId();
            StoreEntry messageEntry=messageContainer.place(id,message);
            TopicSubAck tsa=new TopicSubAck();
            tsa.setCount(subscriberCount);
            tsa.setMessageEntry(messageEntry);
            StoreEntry ackEntry=ackContainer.placeLast(tsa);
            for(Iterator i=subscriberMessages.values().iterator();i.hasNext();){
                TopicSubContainer container=(TopicSubContainer)i.next();
                ConsumerMessageRef ref=new ConsumerMessageRef();
                ref.setAckEntry(ackEntry);
                ref.setMessageEntry(messageEntry);
                ref.setMessageId(id);
                container.add(ref);
            }
        }
    }

    public synchronized void acknowledge(ConnectionContext context,String clientId,String subscriptionName,
            MessageId messageId) throws IOException{
        String subcriberId=getSubscriptionKey(clientId,subscriptionName);
        TopicSubContainer container=(TopicSubContainer)subscriberMessages.get(subcriberId);
        if(container!=null){
            ConsumerMessageRef ref=container.remove(messageId);
            if(container.isEmpty()){
                container.reset();
            }
            if(ref!=null){
                TopicSubAck tsa=(TopicSubAck)ackContainer.get(ref.getAckEntry());
                if(tsa!=null){
                    if(tsa.decrementCount()<=0){
                        StoreEntry entry = ref.getAckEntry();
                        entry = ackContainer.refresh(entry);
                        ackContainer.remove(entry);
                        entry = tsa.getMessageEntry();
                        entry =messageContainer.refresh(entry);
                        messageContainer.remove(entry);
                    }else{
                        ackContainer.update(ref.getAckEntry(),tsa);
                    }
                }
            }
        }
    }

    public SubscriptionInfo lookupSubscription(String clientId,String subscriptionName) throws IOException{
        return (SubscriptionInfo)subscriberContainer.get(getSubscriptionKey(clientId,subscriptionName));
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
        // add the subscriber
        ListContainer container=addSubscriberMessageContainer(key);
        /*
        if(retroactive){
            for(StoreEntry entry=ackContainer.getFirst();entry!=null;entry=ackContainer.getNext(entry)){
                TopicSubAck tsa=(TopicSubAck)ackContainer.get(entry);
                ConsumerMessageRef ref=new ConsumerMessageRef();
                ref.setAckEntry(entry);
                ref.setMessageEntry(tsa.getMessageEntry());
                container.add(ref);
            }
        }
        */
    }

    public synchronized void deleteSubscription(String clientId,String subscriptionName) throws IOException{
        String key=getSubscriptionKey(clientId,subscriptionName);
        removeSubscriberMessageContainer(key);
    }

    public void recoverSubscription(String clientId,String subscriptionName,MessageRecoveryListener listener)
            throws Exception{
        String key=getSubscriptionKey(clientId,subscriptionName);
        TopicSubContainer container=(TopicSubContainer)subscriberMessages.get(key);
        if(container!=null){
            for(Iterator i=container.iterator();i.hasNext();){
                ConsumerMessageRef ref=(ConsumerMessageRef)i.next();
                Message msg=messageContainer.get(ref.getMessageEntry());
                if(msg!=null){
                	recoverMessage(listener, msg);
                }
            }
        }
        listener.finished();
    }

	public void recoverNextMessages(String clientId,String subscriptionName,int maxReturned,
            MessageRecoveryListener listener) throws Exception{
        String key=getSubscriptionKey(clientId,subscriptionName);
        TopicSubContainer container=(TopicSubContainer)subscriberMessages.get(key);
        if(container!=null){
            int count=0;
            StoreEntry entry=container.getBatchEntry();
            if(entry==null){
                entry=container.getEntry();
            }else{
                entry=container.refreshEntry(entry);
                if(entry!=null){
                    entry=container.getNextEntry(entry);
                }
            }
            if(entry!=null){
                do{
                    ConsumerMessageRef consumerRef=container.get(entry);
                    Message msg=messageContainer.getValue(consumerRef.getMessageEntry());
                    if(msg!=null){
                    	recoverMessage(listener, msg);
                        count++;
                        container.setBatchEntry(msg.getMessageId().toString(),entry);
                    }else {
                        container.reset();
                    }
                    
                    entry=container.getNextEntry(entry);
                }while(entry!=null&&count<maxReturned&&listener.hasSpace());
            }
        }
        listener.finished();
    }

    public void delete(){
        super.delete();
        ackContainer.clear();
        subscriberContainer.clear();
    }

    public SubscriptionInfo[] getAllSubscriptions() throws IOException{
        return (SubscriptionInfo[])subscriberContainer.values().toArray(new SubscriptionInfo[subscriberContainer.size()]);
    }

    protected String getSubscriptionKey(String clientId,String subscriberName){
        String result=clientId+":";
        result+=subscriberName!=null?subscriberName:"NOT_SET";
        return result;
    }

    protected ListContainer addSubscriberMessageContainer(Object key) throws IOException{
        ListContainer container=store.getListContainer(key,"topic-subs");
        Marshaller marshaller=new ConsumerMessageRefMarshaller();
        container.setMarshaller(marshaller);
        TopicSubContainer tsc=new TopicSubContainer(container);
        subscriberMessages.put(key,tsc);
        return container;
    }

    protected void removeSubscriberMessageContainer(Object key) throws IOException{
        subscriberContainer.remove(key);
        TopicSubContainer container=(TopicSubContainer)subscriberMessages.remove(key);
        for(Iterator i=container.iterator();i.hasNext();){
            ConsumerMessageRef ref=(ConsumerMessageRef)i.next();
            if(ref!=null){
                TopicSubAck tsa=(TopicSubAck)ackContainer.get(ref.getAckEntry());
                if(tsa!=null){
                    if(tsa.decrementCount()<=0){
                        ackContainer.remove(ref.getAckEntry());
                        messageContainer.remove(tsa.getMessageEntry());
                    }else{
                        ackContainer.update(ref.getAckEntry(),tsa);
                    }
                }
            }
        }
        store.deleteListContainer(key,"topic-subs");
    }

    public int getMessageCount(String clientId,String subscriberName) throws IOException{
        String key=getSubscriptionKey(clientId,subscriberName);
        TopicSubContainer container=(TopicSubContainer)subscriberMessages.get(key);
        return  container != null ? container.size() : 0;
    }

    /**
     * @param context
     * @throws IOException
     * @see org.apache.activemq.store.MessageStore#removeAllMessages(org.apache.activemq.broker.ConnectionContext)
     */
    public synchronized void removeAllMessages(ConnectionContext context) throws IOException{
        messageContainer.clear();
        ackContainer.clear();
        for(Iterator i=subscriberMessages.values().iterator();i.hasNext();){
            TopicSubContainer container=(TopicSubContainer)i.next();
            container.clear();
        }
    }

    public synchronized void resetBatching(String clientId,String subscriptionName){
        String key=getSubscriptionKey(clientId,subscriptionName);
        TopicSubContainer topicSubContainer=(TopicSubContainer)subscriberMessages.get(key);
        if(topicSubContainer!=null){
            topicSubContainer.reset();
        }
    }
}
