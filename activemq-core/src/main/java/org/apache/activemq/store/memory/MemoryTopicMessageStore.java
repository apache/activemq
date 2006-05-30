/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.util.SubscriptionKey;

/**
 * @version $Revision: 1.5 $
 */
public class MemoryTopicMessageStore extends MemoryMessageStore implements TopicMessageStore {

    private Map ackDatabase;
    private Map subscriberDatabase;
    MessageId lastMessageId;
    
    public MemoryTopicMessageStore(ActiveMQDestination destination) {
        this(destination, new LinkedHashMap(), makeMap(), makeMap());
    }
    protected static Map makeMap() {
        return Collections.synchronizedMap(new HashMap());
    }
    
    public MemoryTopicMessageStore(ActiveMQDestination destination, Map messageTable, Map subscriberDatabase, Map ackDatabase) {
        super(destination, messageTable);
        this.subscriberDatabase = subscriberDatabase;
        this.ackDatabase = ackDatabase;
    }

    public synchronized void addMessage(ConnectionContext context, Message message) throws IOException {
        super.addMessage(context, message);
        lastMessageId = message.getMessageId();
    }

    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId) throws IOException {
        ackDatabase.put(new SubscriptionKey(clientId, subscriptionName), messageId);
    }

    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return (SubscriptionInfo) subscriberDatabase.get(new SubscriptionKey(clientId, subscriptionName));
    }

    public void addSubsciption(String clientId, String subscriptionName, String selector, boolean retroactive) throws IOException {
        SubscriptionInfo info = new SubscriptionInfo();
        info.setDestination(destination);
        info.setClientId(clientId);
        info.setSelector(selector);
        info.setSubcriptionName(subscriptionName);
        SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
        subscriberDatabase.put(key, info);
        MessageId l=retroactive ? null : lastMessageId;
        if( l!=null ) {
            ackDatabase.put(key, l);
        }
    }
    
    public void deleteSubscription(String clientId, String subscriptionName) {
        org.apache.activemq.util.SubscriptionKey key = new SubscriptionKey(clientId, subscriptionName);
        ackDatabase.remove(key);
        subscriberDatabase.remove(key);
    }
    
    public void recoverSubscription(String clientId,String subscriptionName,MessageRecoveryListener listener)
                    throws Exception{
        MessageId lastAck=(MessageId) ackDatabase.get(new SubscriptionKey(clientId,subscriptionName));
        boolean pastLastAck=lastAck==null;
        // the message table is a synchronizedMap - so just have to synchronize here
        synchronized(messageTable){
            for(Iterator iter=messageTable.entrySet().iterator();iter.hasNext();){
                Map.Entry entry=(Entry) iter.next();
                if(pastLastAck){
                    Object msg=entry.getValue();
                    if(msg.getClass()==String.class){
                        listener.recoverMessageReference((String) msg);
                    }else{
                        listener.recoverMessage((Message) msg);
                    }
                }else{
                    pastLastAck=entry.getKey().equals(lastAck);
                }
            }
            listener.finished();
        }
    }

    public void delete() {
        super.delete();
        ackDatabase.clear();
        subscriberDatabase.clear();
        lastMessageId=null;
    }
    
    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        return (SubscriptionInfo[]) subscriberDatabase.values().toArray(new SubscriptionInfo[subscriberDatabase.size()]);
    }
}
