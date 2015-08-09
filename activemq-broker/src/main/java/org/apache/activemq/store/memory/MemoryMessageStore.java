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
import org.apache.activemq.store.IndexListener;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageStoreStatistics;

/**
 * An implementation of {@link org.apache.activemq.store.MessageStore} which
 * uses a
 *
 *
 */
public class MemoryMessageStore extends AbstractMessageStore {

    protected final Map<MessageId, Message> messageTable;
    protected MessageId lastBatchId;
    protected long sequenceId;

    public MemoryMessageStore(ActiveMQDestination destination) {
        this(destination, new LinkedHashMap<MessageId, Message>());
    }

    public MemoryMessageStore(ActiveMQDestination destination, Map<MessageId, Message> messageTable) {
        super(destination);
        this.messageTable = Collections.synchronizedMap(messageTable);
    }

    @Override
    public synchronized void addMessage(ConnectionContext context, Message message) throws IOException {
        synchronized (messageTable) {
            messageTable.put(message.getMessageId(), message);
            incMessageStoreStatistics(getMessageStoreStatistics(), message);
        }
        message.incrementReferenceCount();
        message.getMessageId().setFutureOrSequenceLong(sequenceId++);
        if (indexListener != null) {
            indexListener.onAdd(new IndexListener.MessageContext(context, message, null));
        }
    }

    // public void addMessageReference(ConnectionContext context,MessageId
    // messageId,long expirationTime,String messageRef)
    // throws IOException{
    // synchronized(messageTable){
    // messageTable.put(messageId,messageRef);
    // }
    // }

    @Override
    public Message getMessage(MessageId identity) throws IOException {
        return messageTable.get(identity);
    }

    // public String getMessageReference(MessageId identity) throws IOException{
    // return (String)messageTable.get(identity);
    // }

    @Override
    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
        removeMessage(ack.getLastMessageId());
    }

    public void removeMessage(MessageId msgId) throws IOException {
        synchronized (messageTable) {
            Message removed = messageTable.remove(msgId);
            if( removed !=null ) {
                removed.decrementReferenceCount();
                decMessageStoreStatistics(getMessageStoreStatistics(), removed);
            }
            if ((lastBatchId != null && lastBatchId.equals(msgId)) || messageTable.isEmpty()) {
                lastBatchId = null;
            }
        }
    }

    @Override
    public void recover(MessageRecoveryListener listener) throws Exception {
        // the message table is a synchronizedMap - so just have to synchronize
        // here
        synchronized (messageTable) {
            for (Iterator<Message> iter = messageTable.values().iterator(); iter.hasNext();) {
                Object msg = iter.next();
                if (msg.getClass() == MessageId.class) {
                    listener.recoverMessageReference((MessageId)msg);
                } else {
                    listener.recoverMessage((Message)msg);
                }
            }
        }
    }

    @Override
    public void removeAllMessages(ConnectionContext context) throws IOException {
        synchronized (messageTable) {
            messageTable.clear();
            getMessageStoreStatistics().reset();
        }
    }

    public void delete() {
        synchronized (messageTable) {
            messageTable.clear();
            getMessageStoreStatistics().reset();
        }
    }

    @Override
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

    @Override
    public void resetBatching() {
        lastBatchId = null;
    }

    @Override
    public void setBatch(MessageId messageId) {
        lastBatchId = messageId;
    }

    @Override
    public void updateMessage(Message message) {
        synchronized (messageTable) {
            Message original = messageTable.get(message.getMessageId());

            //if can't be found then increment count, else remove old size
            if (original == null) {
                getMessageStoreStatistics().getMessageCount().increment();
            } else {
                getMessageStoreStatistics().getMessageSize().addSize(-original.getSize());
            }
            messageTable.put(message.getMessageId(), message);
            getMessageStoreStatistics().getMessageSize().addSize(message.getSize());
        }
    }

    @Override
    public void recoverMessageStoreStatistics() throws IOException {
        synchronized (messageTable) {
            long size = 0;
            int count = 0;
            for (Iterator<Message> iter = messageTable.values().iterator(); iter
                    .hasNext();) {
                Message msg = iter.next();
                size += msg.getSize();
            }

            getMessageStoreStatistics().reset();
            getMessageStoreStatistics().getMessageCount().setCount(count);
            getMessageStoreStatistics().getMessageSize().setTotalSize(size);
        }
    }

    protected static final void incMessageStoreStatistics(final MessageStoreStatistics stats, final Message message) {
        if (stats != null && message != null) {
            stats.getMessageCount().increment();
            stats.getMessageSize().addSize(message.getSize());
        }
    }

    protected static final void decMessageStoreStatistics(final MessageStoreStatistics stats, final Message message) {
        if (stats != null && message != null) {
            stats.getMessageCount().decrement();
            stats.getMessageSize().addSize(-message.getSize());
        }
    }

}
