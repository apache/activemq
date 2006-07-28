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
package org.apache.activemq.store.rapid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.activeio.journal.RecordLocation;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.JournalTopicAck;
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
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.SubscriptionKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;

/**
 * A MessageStore that uses a Journal to store it's messages.
 * 
 * @version $Revision: 1.13 $
 */
public class RapidTopicMessageStore extends RapidMessageStore implements TopicMessageStore {
    
    private static final Log log = LogFactory.getLog(RapidTopicMessageStore.class);

    private HashMap ackedLastAckLocations = new HashMap();
    private final MapContainer subscriberContainer;
    private final MapContainer ackContainer;
    private final Store store;
    private Map subscriberAcks=new ConcurrentHashMap();

    public RapidTopicMessageStore(RapidPersistenceAdapter adapter, ActiveMQTopic destination, MapContainer messageContainer, MapContainer subsContainer, MapContainer ackContainer) throws IOException {
        super(adapter, destination, messageContainer);
        this.subscriberContainer = subsContainer;
        this.ackContainer = ackContainer;
        this.store=adapter.getStore();
        
        for(Iterator i=subscriberContainer.keySet().iterator();i.hasNext();){
            Object key=i.next();
            addSubscriberAckContainer(key);
        }
    }

    public void recoverSubscription(String clientId, String subscriptionName, final MessageRecoveryListener listener) throws Exception {

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
        } else {
            listener.finished();
        }
        
    }

    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return (SubscriptionInfo) subscriberContainer.get(getSubscriptionKey(clientId,subscriptionName));
    }

    public void addSubsciption(String clientId, String subscriptionName, String selector, boolean retroactive) throws IOException {
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
    
    
    /**
     */
    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName, final MessageId messageId) throws IOException {
        final boolean debug = log.isDebugEnabled();
        
        JournalTopicAck ack = new JournalTopicAck();
        ack.setDestination(destination);
        ack.setMessageId(messageId);
        ack.setMessageSequenceId(messageId.getBrokerSequenceId());
        ack.setSubscritionName(subscriptionName);
        ack.setClientId(clientId);
        ack.setTransactionId( context.getTransaction()!=null ? context.getTransaction().getTransactionId():null);
        final RecordLocation location = peristenceAdapter.writeCommand(ack, false);
        
        final SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);        
        if( !context.isInTransaction() ) {
            if( debug )
                log.debug("Journalled acknowledge for: "+messageId+", at: "+location);
            acknowledge(messageId, location, key);
        } else {
            if( debug )
                log.debug("Journalled transacted acknowledge for: "+messageId+", at: "+location);
            synchronized (this) {
                inFlightTxLocations.add(location);
            }
            transactionStore.acknowledge(this, ack, location);
            context.getTransaction().addSynchronization(new Synchronization(){
                public void afterCommit() throws Exception {                    
                    if( debug )
                        log.debug("Transacted acknowledge commit for: "+messageId+", at: "+location);
                    synchronized (RapidTopicMessageStore.this) {
                        inFlightTxLocations.remove(location);
                        acknowledge(messageId, location, key);
                    }
                }
                public void afterRollback() throws Exception {                    
                    if( debug )
                        log.debug("Transacted acknowledge rollback for: "+messageId+", at: "+location);
                    synchronized (RapidTopicMessageStore.this) {
                        inFlightTxLocations.remove(location);
                    }
                }
            });
        }
        
    }
    
    public void replayAcknowledge(ConnectionContext context, String clientId, String subscritionName, MessageId messageId) {
        try {
            synchronized(this) {
                String subcriberId=getSubscriptionKey(clientId,subscritionName);
                String id=messageId.toString();
                ListContainer container=(ListContainer) subscriberAcks.get(subcriberId);
                if(container!=null){
                    //container.remove(id);
                    container.removeFirst();
                    AtomicInteger count=(AtomicInteger) ackContainer.remove(id);
                    if(count!=null){
                        if(count.decrementAndGet()>0){
                            ackContainer.put(id,count);
                        } else {
                            // no more references to message messageContainer so remove it
                            messageContainer.remove(messageId.toString());
                        }
                    }
                }
            }
        }
        catch (Throwable e) {
            log.debug("Could not replay acknowledge for message '" + messageId + "'.  Message may have already been acknowledged. reason: " + e);
        }
    }
        

    /**
     * @param messageId
     * @param location
     * @param key
     */
    private void acknowledge(MessageId messageId, RecordLocation location, SubscriptionKey key) {
        synchronized(this) {
		    lastLocation = location;
		    ackedLastAckLocations.put(key, messageId);
            
            String subcriberId=getSubscriptionKey(key.getClientId(),key.getSubscriptionName());
            String id=messageId.toString();
            ListContainer container=(ListContainer) subscriberAcks.get(subcriberId);
            if(container!=null){
                //container.remove(id);
                container.removeFirst();
                AtomicInteger count=(AtomicInteger) ackContainer.remove(id);
                if(count!=null){
                    if(count.decrementAndGet()>0){
                        ackContainer.put(id,count);
                    } else {
                        // no more references to message messageContainer so remove it
                        messageContainer.remove(messageId.toString());
                    }
                }
            }
		}
    }
    
    protected String getSubscriptionKey(String clientId,String subscriberName){
        String result=clientId+":";
        result+=subscriberName!=null?subscriberName:"NOT_SET";
        return result;
    }

    
    public RecordLocation checkpoint() throws IOException {
        
		ArrayList cpAckedLastAckLocations;

        // swap out the hash maps..
        synchronized (this) {
            cpAckedLastAckLocations = new ArrayList(this.ackedLastAckLocations.values());
            this.ackedLastAckLocations = new HashMap();
        }

        RecordLocation rc = super.checkpoint();
        if(!cpAckedLastAckLocations.isEmpty()) {
            Collections.sort(cpAckedLastAckLocations);
            RecordLocation t = (RecordLocation) cpAckedLastAckLocations.get(0);
            if( rc == null || t.compareTo(rc)<0 ) {
                rc = t;
            }
        }
        
        return rc;
    }


    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
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

    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        return (SubscriptionInfo[]) subscriberContainer.values().toArray(
                new SubscriptionInfo[subscriberContainer.size()]);
    }

    protected void addSubscriberAckContainer(Object key) throws IOException{
        ListContainer container=store.getListContainer(key,"topic-subs");
        Marshaller marshaller=new StringMarshaller();
        container.setMarshaller(marshaller);
        subscriberAcks.put(key,container);
    }

}