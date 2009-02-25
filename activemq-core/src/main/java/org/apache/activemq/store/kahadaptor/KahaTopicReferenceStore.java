/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.kahadaptor;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
import org.apache.activemq.store.TopicReferenceStore;

public class KahaTopicReferenceStore extends KahaReferenceStore implements TopicReferenceStore {

    protected ListContainer<TopicSubAck> ackContainer;
    protected Map<String, TopicSubContainer> subscriberMessages = new ConcurrentHashMap<String, TopicSubContainer>();
    private MapContainer<String, SubscriptionInfo> subscriberContainer;
    private Store store;
    private static final String TOPIC_SUB_NAME = "tsn";

    public KahaTopicReferenceStore(Store store, KahaReferenceStoreAdapter adapter,
                                   MapContainer<MessageId, ReferenceRecord> messageContainer, ListContainer<TopicSubAck> ackContainer,
                                   MapContainer<String, SubscriptionInfo> subsContainer, ActiveMQDestination destination)
        throws IOException {
        super(adapter, messageContainer, destination);
        this.store = store;
        this.ackContainer = ackContainer;
        subscriberContainer = subsContainer;
        // load all the Ack containers
        for (Iterator<SubscriptionInfo> i = subscriberContainer.values().iterator(); i.hasNext();) {
            SubscriptionInfo info = i.next();
            addSubscriberMessageContainer(info.getClientId(), info.getSubscriptionName());
        }
    }

    public void dispose(ConnectionContext context) {
        super.dispose(context);
        subscriberContainer.delete();
    }

    protected MessageId getMessageId(Object object) {
        return new MessageId(((ReferenceRecord)object).getMessageId());
    }

    public void addMessage(ConnectionContext context, Message message) throws IOException {
        throw new RuntimeException("Use addMessageReference instead");
    }

    public Message getMessage(MessageId identity) throws IOException {
        throw new RuntimeException("Use addMessageReference instead");
    }

    public  void addMessageReference(final ConnectionContext context, final MessageId messageId,
                                    final ReferenceData data) {
        lock.lock();
        try {
            final ReferenceRecord record = new ReferenceRecord(messageId.toString(), data);
            final int subscriberCount = subscriberMessages.size();
            if (subscriberCount > 0) {
                final StoreEntry messageEntry = messageContainer.place(messageId, record);
                addInterest(record);
                final TopicSubAck tsa = new TopicSubAck();
                tsa.setCount(subscriberCount);
                tsa.setMessageEntry(messageEntry);
                final StoreEntry ackEntry = ackContainer.placeLast(tsa);
                for (final Iterator<TopicSubContainer> i = subscriberMessages.values().iterator(); i.hasNext();) {
                    final TopicSubContainer container = i.next();
                    final ConsumerMessageRef ref = new ConsumerMessageRef();
                    ref.setAckEntry(ackEntry);
                    ref.setMessageEntry(messageEntry);
                    ref.setMessageId(messageId);
                    container.add(ref);
                }
            }
        }finally {
            lock.unlock();
        }
    }

    public ReferenceData getMessageReference(final MessageId identity) throws IOException {
        final ReferenceRecord result = messageContainer.get(identity);
        if (result == null) {
            return null;
        }
        return result.getData();
    }

    public void addReferenceFileIdsInUse() {
        for (StoreEntry entry = ackContainer.getFirst(); entry != null; entry = ackContainer.getNext(entry)) {
            TopicSubAck subAck = ackContainer.get(entry);
            if (subAck.getCount() > 0) {
                ReferenceRecord rr = messageContainer.getValue(subAck.getMessageEntry());
                addInterest(rr);
            }
        }
    }

    
    protected MapContainer addSubscriberMessageContainer(String clientId, String subscriptionName) throws IOException {
        String containerName = getSubscriptionContainerName(getSubscriptionKey(clientId, subscriptionName));
        MapContainer container = store.getMapContainer(containerName,containerName);
        container.setKeyMarshaller(Store.MESSAGEID_MARSHALLER);
        Marshaller marshaller = new ConsumerMessageRefMarshaller();
        container.setValueMarshaller(marshaller);
        TopicSubContainer tsc = new TopicSubContainer(container);
        subscriberMessages.put(getSubscriptionKey(clientId, subscriptionName), tsc);
        return container;
    }

    public boolean acknowledgeReference(ConnectionContext context,
            String clientId, String subscriptionName, MessageId messageId)
            throws IOException {
        boolean removeMessage = false;
        lock.lock();
            try {
            String key = getSubscriptionKey(clientId, subscriptionName);
    
            TopicSubContainer container = subscriberMessages.get(key);
            if (container != null) {
                ConsumerMessageRef ref = null;
                if((ref = container.remove(messageId)) != null) {
                    StoreEntry entry = ref.getAckEntry();
                    //ensure we get up to-date pointers
                    entry = ackContainer.refresh(entry);
                    TopicSubAck tsa = ackContainer.get(entry);
                    if (tsa != null) {
                        if (tsa.decrementCount() <= 0) {
                            ackContainer.remove(entry);
                            ReferenceRecord rr = messageContainer.get(messageId);
                            if (rr != null) {
                                entry = tsa.getMessageEntry();
                                entry = messageContainer.refresh(entry);
                                messageContainer.remove(entry);
                                removeInterest(rr);
                                removeMessage = true;
                            }
                        }else {
                            ackContainer.update(entry,tsa);
                        }
                    }
                }else{
           
                    if (ackContainer.isEmpty() || isUnreferencedBySubscribers(subscriberMessages, messageId)) {
                        // no message reference held        
                        removeMessage = true;
                    }
                }
            }
        }finally {
            lock.unlock();
        }
        return removeMessage;
    }
    
    // verify that no subscriber has a reference to this message. In the case where the subscribers
    // references are persisted but more than the persisted consumers get the message, the ack from the non
    // persisted consumer would remove the message in error
    //
    // see: https://issues.apache.org/activemq/browse/AMQ-2123
    private boolean isUnreferencedBySubscribers(
            Map<String, TopicSubContainer> subscriberContainers, MessageId messageId) {
        boolean isUnreferenced = true;
        for (TopicSubContainer container: subscriberContainers.values()) {
            if (!container.isEmpty()) {
                for (Iterator i = container.iterator(); i.hasNext();) {
                    ConsumerMessageRef ref = (ConsumerMessageRef) i.next();
                    if (messageId.equals(ref.getMessageId())) {
                        isUnreferenced = false;
                        break;
                    }
                }
            }
        }
        return isUnreferenced; 
    }

    public void acknowledge(ConnectionContext context,
			String clientId, String subscriptionName, MessageId messageId) throws IOException {
	    acknowledgeReference(context, clientId, subscriptionName, messageId);
	}

    public void addSubsciption(SubscriptionInfo info, boolean retroactive) throws IOException {
        String key = getSubscriptionKey(info.getClientId(), info.getSubscriptionName());
        lock.lock();
        try {
            // if already exists - won't add it again as it causes data files
            // to hang around
            if (!subscriberContainer.containsKey(key)) {
                subscriberContainer.put(key, info);
                adapter.addSubscriberState(info);
            }
            // add the subscriber
            addSubscriberMessageContainer(info.getClientId(), info.getSubscriptionName());
            if (retroactive) {
                /*
                 * for(StoreEntry
                 * entry=ackContainer.getFirst();entry!=null;entry=ackContainer.getNext(entry)){
                 * TopicSubAck tsa=(TopicSubAck)ackContainer.get(entry);
                 * ConsumerMessageRef ref=new ConsumerMessageRef();
                 * ref.setAckEntry(entry);
                 * ref.setMessageEntry(tsa.getMessageEntry()); container.add(ref); }
                 */
            }
        }finally {
            lock.unlock();
        }
    }

    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        lock.lock();
        try {
            SubscriptionInfo info = lookupSubscription(clientId, subscriptionName);
            if (info != null) {
                adapter.removeSubscriberState(info);
            }
        removeSubscriberMessageContainer(clientId,subscriptionName);
        }finally {
            lock.unlock();
        }
    }

    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        SubscriptionInfo[] result = subscriberContainer.values()
            .toArray(new SubscriptionInfo[subscriberContainer.size()]);
        return result;
    }

    public int getMessageCount(String clientId, String subscriberName) throws IOException {
        String key = getSubscriptionKey(clientId, subscriberName);
        TopicSubContainer container = subscriberMessages.get(key);
        return container != null ? container.size() : 0;
    }

    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return subscriberContainer.get(getSubscriptionKey(clientId, subscriptionName));
    }

    public void recoverNextMessages(String clientId, String subscriptionName, int maxReturned,
                                                 MessageRecoveryListener listener) throws Exception {
        String key = getSubscriptionKey(clientId, subscriptionName);
        lock.lock();
        try {
            TopicSubContainer container = subscriberMessages.get(key);
            if (container != null) {
                int count = 0;
                StoreEntry entry = container.getBatchEntry();
                if (entry == null) {
                    entry = container.getEntry();
                } else {
                    entry = container.refreshEntry(entry);
                    if (entry != null) {
                        entry = container.getNextEntry(entry);
                    }
                }
               
                if (entry != null) {
                    do {
                        ConsumerMessageRef consumerRef = container.get(entry);
                        ReferenceRecord msg = messageContainer.getValue(consumerRef
                                .getMessageEntry());
                        if (msg != null) {
                            if (recoverReference(listener, msg)) {
                                count++;
                                container.setBatchEntry(msg.getMessageId(), entry);
                            } else {
                                break;
                            }
                        } else {
                            container.reset();
                        }
    
                        entry = container.getNextEntry(entry);
                    } while (entry != null && count < maxReturned && listener.hasSpace());
                }
            }
        }finally {
            lock.unlock();
        }
    }

    public void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener)
        throws Exception {
        String key = getSubscriptionKey(clientId, subscriptionName);
        TopicSubContainer container = subscriberMessages.get(key);
        if (container != null) {
            for (Iterator i = container.iterator(); i.hasNext();) {
                ConsumerMessageRef ref = (ConsumerMessageRef)i.next();
                ReferenceRecord msg = messageContainer.get(ref.getMessageEntry());
                if (msg != null) {
                    if (!recoverReference(listener, msg)) {
                        break;
                    }
                }
            }
        }
    }

    public void resetBatching(String clientId, String subscriptionName) {
        lock.lock();
        try {
            String key = getSubscriptionKey(clientId, subscriptionName);
            TopicSubContainer topicSubContainer = subscriberMessages.get(key);
            if (topicSubContainer != null) {
                topicSubContainer.reset();
            }
        }finally {
            lock.unlock();
        }
    }
    
    public void removeAllMessages(ConnectionContext context) throws IOException {
        lock.lock();
        try {
            Set<String> tmpSet = new HashSet<String>(subscriberContainer.keySet());
            for (String key:tmpSet) {
                TopicSubContainer container = subscriberMessages.get(key);
                if (container != null) {
                    container.clear();
                }
            }
            ackContainer.clear();
        }finally {
            lock.unlock();
        }
        super.removeAllMessages(context);
    }

    protected void removeSubscriberMessageContainer(String clientId, String subscriptionName) throws IOException {
        String subscriberKey = getSubscriptionKey(clientId, subscriptionName);
        String containerName = getSubscriptionContainerName(subscriberKey);
        subscriberContainer.remove(subscriberKey);
        TopicSubContainer container = subscriberMessages.remove(subscriberKey);
        if (container != null) {
            for (Iterator i = container.iterator(); i.hasNext();) {
                ConsumerMessageRef ref = (ConsumerMessageRef)i.next();
                if (ref != null) {
                    TopicSubAck tsa = ackContainer.get(ref.getAckEntry());
                    if (tsa != null) {
                        if (tsa.decrementCount() <= 0) {
                            ackContainer.remove(ref.getAckEntry());
                            messageContainer.remove(tsa.getMessageEntry());
                        } else {
                            ackContainer.update(ref.getAckEntry(), tsa);
                        }
                    }
                }
            }
        }
        store.deleteMapContainer(containerName,containerName);
    }

    protected String getSubscriptionKey(String clientId, String subscriberName) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(clientId).append(":");  
        String name = subscriberName != null ? subscriberName : "NOT_SET";
        return buffer.append(name).toString();
    }
    
    private String getSubscriptionContainerName(String subscriptionKey) {
        StringBuffer result = new StringBuffer(TOPIC_SUB_NAME);
        result.append(destination.getQualifiedName());
        result.append(subscriptionKey);
        return result.toString();
    }
}
