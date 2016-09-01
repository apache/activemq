/*
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
package org.apache.activemq.store.memory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStoreStatistics;
import org.apache.activemq.store.MessageStoreSubscriptionStatistics;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.SubscriptionKey;

public class MemoryTopicMessageStore extends MemoryMessageStore implements TopicMessageStore {

    private Map<SubscriptionKey, SubscriptionInfo> subscriberDatabase;
    private Map<SubscriptionKey, MemoryTopicSub> topicSubMap;
    private final Map<MessageId, Message> originalMessageTable;

    public MemoryTopicMessageStore(ActiveMQDestination destination) {
        this(destination, new MemoryTopicMessageStoreLRUCache(100, 100, 0.75f, false), makeSubscriptionInfoMap());

        // Set the messageStoreStatistics after the super class is initialized
        // so that the stats can be properly updated on cache eviction
        MemoryTopicMessageStoreLRUCache cache = (MemoryTopicMessageStoreLRUCache) originalMessageTable;
        cache.setMessageStoreStatistics(messageStoreStatistics);
    }

    public MemoryTopicMessageStore(ActiveMQDestination destination, Map<MessageId, Message> messageTable,
        Map<SubscriptionKey, SubscriptionInfo> subscriberDatabase) {
        super(destination, messageTable);
        this.subscriberDatabase = subscriberDatabase;
        this.topicSubMap = makeSubMap();
        // this is only necessary so that messageStoreStatistics can be set if
        // necessary We need the original reference since messageTable is wrapped
        // in a synchronized map in the parent class
        this.originalMessageTable = messageTable;
    }

    protected static Map<SubscriptionKey, SubscriptionInfo> makeSubscriptionInfoMap() {
        return Collections.synchronizedMap(new HashMap<SubscriptionKey, SubscriptionInfo>());
    }

    protected static Map<SubscriptionKey, MemoryTopicSub> makeSubMap() {
        return Collections.synchronizedMap(new HashMap<SubscriptionKey, MemoryTopicSub>());
    }

    @Override
    public synchronized void addMessage(ConnectionContext context, Message message) throws IOException {
        super.addMessage(context, message);
        for (MemoryTopicSub sub : topicSubMap.values()) {
            sub.addMessage(message.getMessageId(), message);
        }
    }

    @Override
    public synchronized void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId, MessageAck ack) throws IOException {
        super.removeMessage(messageId);
        SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
        MemoryTopicSub sub = topicSubMap.get(key);
        if (sub != null) {
            sub.removeMessage(messageId);
        }
    }

    @Override
    public synchronized SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return subscriberDatabase.get(new SubscriptionKey(clientId, subscriptionName));
    }

    @Override
    public synchronized void addSubscription(SubscriptionInfo info, boolean retroactive) throws IOException {
        SubscriptionKey key = new SubscriptionKey(info);
        MemoryTopicSub sub = new MemoryTopicSub(key);
        topicSubMap.put(key, sub);
        if (retroactive) {
            for (Map.Entry<MessageId, Message> entry : messageTable.entrySet()) {
                sub.addMessage(entry.getKey(), entry.getValue());
            }
        }
        subscriberDatabase.put(key, info);
    }

    @Override
    public synchronized void deleteSubscription(String clientId, String subscriptionName) {
        SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
        subscriberDatabase.remove(key);
        MemoryTopicSub subscription = topicSubMap.get(key);
        if (subscription != null) {
            List<Message> storedMessages = subscription.getStoredMessages();
            for (Message message : storedMessages) {
                try {
                    acknowledge(null, key.getClientId(), key.getSubscriptionName(), message.getMessageId(), null);
                } catch (IOException e) {
                }
            }
        }

        subscriberDatabase.remove(key);
        topicSubMap.remove(key);
    }

    @Override
    public synchronized void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener) throws Exception {
        MemoryTopicSub sub = topicSubMap.get(new SubscriptionKey(clientId, subscriptionName));
        if (sub != null) {
            sub.recoverSubscription(listener);
        }
    }

    @Override
    public synchronized void delete() {
        super.delete();
        subscriberDatabase.clear();
        topicSubMap.clear();
    }

    @Override
    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        return subscriberDatabase.values().toArray(new SubscriptionInfo[subscriberDatabase.size()]);
    }

    @Override
    public synchronized int getMessageCount(String clientId, String subscriberName) throws IOException {
        int result = 0;
        MemoryTopicSub sub = topicSubMap.get(new SubscriptionKey(clientId, subscriberName));
        if (sub != null) {
            result = sub.size();
        }
        return result;
    }

    @Override
    public synchronized long getMessageSize(String clientId, String subscriberName) throws IOException {
        long result = 0;
        MemoryTopicSub sub = topicSubMap.get(new SubscriptionKey(clientId, subscriberName));
        if (sub != null) {
            result = sub.messageSize();
        }
        return result;
    }

    @Override
    public synchronized void recoverNextMessages(String clientId, String subscriptionName, int maxReturned, MessageRecoveryListener listener) throws Exception {
        MemoryTopicSub sub = this.topicSubMap.get(new SubscriptionKey(clientId, subscriptionName));
        if (sub != null) {
            sub.recoverNextMessages(maxReturned, listener);
        }
    }

    @Override
    public void resetBatching(String clientId, String subscriptionName) {
        MemoryTopicSub sub = topicSubMap.get(new SubscriptionKey(clientId, subscriptionName));
        if (sub != null) {
            sub.resetBatching();
        }
    }

    // Disabled for the memory store, can be enabled later if necessary
    private final MessageStoreSubscriptionStatistics stats = new MessageStoreSubscriptionStatistics(false);

    @Override
    public MessageStoreSubscriptionStatistics getMessageStoreSubStatistics() {
        return stats;
    }

    /**
     * Since we initialize the store with a LRUCache in some cases, we need to
     * account for cache evictions when computing the message store statistics.
     *
     */
    private static class MemoryTopicMessageStoreLRUCache extends LRUCache<MessageId, Message> {
        private static final long serialVersionUID = -342098639681884413L;
        private MessageStoreStatistics messageStoreStatistics;

        public MemoryTopicMessageStoreLRUCache(int initialCapacity, int maximumCacheSize, float loadFactor, boolean accessOrder) {
            super(initialCapacity, maximumCacheSize, loadFactor, accessOrder);
        }

        public void setMessageStoreStatistics(MessageStoreStatistics messageStoreStatistics) {
            this.messageStoreStatistics = messageStoreStatistics;
        }

        @Override
        protected void onCacheEviction(Map.Entry<MessageId, Message> eldest) {
            decMessageStoreStatistics(messageStoreStatistics, eldest.getValue());

            // We aren't tracking this anymore so remove our reference to it.
            eldest.getValue().decrementReferenceCount();
        }
    }
}
