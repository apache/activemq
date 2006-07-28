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
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StringMarshaller;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;
/**
 * @version $Revision: 1.5 $
 */
public class KahaTopicMessageStore extends KahaMessageStore implements TopicMessageStore{
    private Map ackContainer;
    private Map subscriberContainer;
    private Store store;
    private Map subscriberAcks=new ConcurrentHashMap();

    public KahaTopicMessageStore(Store store,MapContainer messageContainer,MapContainer ackContainer,
                    MapContainer subsContainer,ActiveMQDestination destination) throws IOException{
        super(messageContainer,destination);
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
            String id=message.getMessageId().toString();
            ackContainer.put(id,new AtomicInteger(subscriberCount));
            for(Iterator i=subscriberAcks.keySet().iterator();i.hasNext();){
                Object key=i.next();
                ListContainer container=store.getListContainer(key,"durable-subs");
                container.add(id);
            }
            super.addMessage(context,message);
        }
    }

    public synchronized void acknowledge(ConnectionContext context,String clientId,String subscriptionName,
                    MessageId messageId) throws IOException{
        String subcriberId=getSubscriptionKey(clientId,subscriptionName);
        String id=messageId.toString();
        ListContainer container=(ListContainer) subscriberAcks.get(subcriberId);
        if(container!=null){
            //container.remove(id);
            container.removeFirst();
            AtomicInteger count=(AtomicInteger) ackContainer.remove(id);
            if(count!=null){
                if(count.decrementAndGet()>0){
                    ackContainer.put(id,count);
                }else{
                    // no more references to message messageContainer so remove it
                    super.removeMessage(messageId);
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
            String id=i.next().toString();
            AtomicInteger count=(AtomicInteger) ackContainer.remove(id);
            if(count!=null){
                if(count.decrementAndGet()>0){
                    ackContainer.put(id,count);
                }else{
                    // no more references to message messageContainer so remove it
                    messageContainer.remove(id);
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
                Object msg=messageContainer.get(i.next());
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

    public void delete(){
        super.delete();
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
        Marshaller marshaller=new StringMarshaller();
        container.setMarshaller(marshaller);
        subscriberAcks.put(key,container);
    }
}
