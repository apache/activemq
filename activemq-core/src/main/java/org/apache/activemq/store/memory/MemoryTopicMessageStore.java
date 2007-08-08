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

package org.apache.activemq.store.memory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.SubscriptionKey;

/**
 * @version $Revision: 1.5 $
 */
public class MemoryTopicMessageStore extends MemoryMessageStore implements TopicMessageStore {

    private Map subscriberDatabase;
    private Map topicSubMap;

    public MemoryTopicMessageStore(ActiveMQDestination destination) {
        this(destination, new LRUCache(100, 100, 0.75f, false), makeMap());
    }

    protected static Map makeMap() {
        return Collections.synchronizedMap(new HashMap());
    }

    public MemoryTopicMessageStore(ActiveMQDestination destination, Map messageTable, Map subscriberDatabase) {
        super(destination, messageTable);
        this.subscriberDatabase = subscriberDatabase;
        this.topicSubMap = makeMap();
    }

    public synchronized void addMessage(ConnectionContext context, Message message) throws IOException {
        super.addMessage(context, message);
        for (Iterator i = topicSubMap.values().iterator(); i.hasNext();) {
            MemoryTopicSub sub = (MemoryTopicSub)i.next();
            sub.addMessage(message.getMessageId(), message);
        }
    }

    public synchronized void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId) throws IOException {
        SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
        MemoryTopicSub sub = (MemoryTopicSub)topicSubMap.get(key);
        if (sub != null) {
            sub.removeMessage(messageId);
        }
    }

    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return (SubscriptionInfo)subscriberDatabase.get(new SubscriptionKey(clientId, subscriptionName));
    }

    public synchronized void addSubsciption(SubscriptionInfo info, boolean retroactive) throws IOException {
        SubscriptionKey key = new SubscriptionKey(info);
        MemoryTopicSub sub = new MemoryTopicSub();
        topicSubMap.put(key, sub);
        if (retroactive) {
            for (Iterator i = messageTable.entrySet().iterator(); i.hasNext();) {
                Map.Entry entry = (Entry)i.next();
                sub.addMessage((MessageId)entry.getKey(), (Message)entry.getValue());
            }
        }
        subscriberDatabase.put(key, info);
    }

    public void deleteSubscription(String clientId, String subscriptionName) {
        org.apache.activemq.util.SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
        subscriberDatabase.remove(key);
        topicSubMap.remove(key);
    }

    public void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener) throws Exception {
        MemoryTopicSub sub = (MemoryTopicSub)topicSubMap.get(new SubscriptionKey(clientId, subscriptionName));
        if (sub != null) {
            sub.recoverSubscription(listener);
        }
    }

    public void delete() {
        super.delete();
        subscriberDatabase.clear();
        topicSubMap.clear();
    }

    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        return (SubscriptionInfo[])subscriberDatabase.values().toArray(new SubscriptionInfo[subscriberDatabase.size()]);
    }

    public synchronized int getMessageCount(String clientId, String subscriberName) throws IOException {
        int result = 0;
        MemoryTopicSub sub = (MemoryTopicSub)topicSubMap.get(new SubscriptionKey(clientId, subscriberName));
        if (sub != null) {
            result = sub.size();
        }
        return result;
    }

    public void recoverNextMessages(String clientId, String subscriptionName, int maxReturned, MessageRecoveryListener listener) throws Exception {
        MemoryTopicSub sub = (MemoryTopicSub)topicSubMap.get(new SubscriptionKey(clientId, subscriptionName));
        if (sub != null) {
            sub.recoverNextMessages(maxReturned, listener);
        }
    }

    public void resetBatching(String clientId, String subscriptionName) {
        MemoryTopicSub sub = (MemoryTopicSub)topicSubMap.get(new SubscriptionKey(clientId, subscriptionName));
        if (sub != null) {
            sub.resetBatching();
        }
    }
}
