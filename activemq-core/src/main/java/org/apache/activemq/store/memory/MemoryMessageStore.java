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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.memory.UsageManager;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;

/**
 * An implementation of {@link org.apache.activemq.store.MessageStore} which
 * uses a
 * 
 * @version $Revision: 1.7 $
 */
public class MemoryMessageStore implements MessageStore {

    protected final ActiveMQDestination destination;
    protected final Map messageTable;
    protected MessageId lastBatchId;

    public MemoryMessageStore(ActiveMQDestination destination) {
        this(destination, new LinkedHashMap());
    }

    public MemoryMessageStore(ActiveMQDestination destination, Map messageTable) {
        this.destination = destination;
        this.messageTable = Collections.synchronizedMap(messageTable);
    }

    public synchronized void addMessage(ConnectionContext context, Message message) throws IOException {
        synchronized (messageTable) {
            messageTable.put(message.getMessageId(), message);
        }
    }

    // public void addMessageReference(ConnectionContext context,MessageId
    // messageId,long expirationTime,String messageRef)
    // throws IOException{
    // synchronized(messageTable){
    // messageTable.put(messageId,messageRef);
    // }
    // }

    public Message getMessage(MessageId identity) throws IOException {
        return (Message)messageTable.get(identity);
    }

    // public String getMessageReference(MessageId identity) throws IOException{
    // return (String)messageTable.get(identity);
    // }

    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
        removeMessage(ack.getLastMessageId());
    }

    public void removeMessage(MessageId msgId) throws IOException {
        synchronized (messageTable) {
            messageTable.remove(msgId);
            if ((lastBatchId != null && lastBatchId.equals(msgId)) || messageTable.isEmpty()) {
                lastBatchId = null;
            }
        }
    }

    public void recover(MessageRecoveryListener listener) throws Exception {
        // the message table is a synchronizedMap - so just have to synchronize
        // here
        synchronized (messageTable) {
            for (Iterator iter = messageTable.values().iterator(); iter.hasNext();) {
                Object msg = (Object)iter.next();
                if (msg.getClass() == MessageId.class) {
                    listener.recoverMessageReference((MessageId)msg);
                } else {
                    listener.recoverMessage((Message)msg);
                }
            }
        }
    }

    public void start() {
    }

    public void stop() {
    }

    public void removeAllMessages(ConnectionContext context) throws IOException {
        synchronized (messageTable) {
            messageTable.clear();
        }
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void delete() {
        synchronized (messageTable) {
            messageTable.clear();
        }
    }

    /**
     * @param usageManager The UsageManager that is controlling the
     *                destination's memory usage.
     */
    public void setUsageManager(UsageManager usageManager) {
    }

    public int getMessageCount() {
        return messageTable.size();
    }

    public void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception {
        synchronized (messageTable) {
            boolean pastLackBatch = lastBatchId == null;
            int count = 0;
            for (Iterator iter = messageTable.entrySet().iterator(); iter.hasNext();) {
                Map.Entry entry = (Entry)iter.next();
                if (pastLackBatch) {
                    count++;
                    Object msg = entry.getValue();
                    lastBatchId = (MessageId)entry.getKey();
                    if (msg.getClass() == MessageId.class) {
                        listener.recoverMessageReference((MessageId)msg);
                    } else {
                        listener.recoverMessage((Message)msg);
                    }
                } else {
                    pastLackBatch = entry.getKey().equals(lastBatchId);
                }
            }
        }
    }

    public void resetBatching() {
        lastBatchId = null;
    }
}
