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
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;

/**
 * An implementation of {@link org.apache.activemq.store.MessageStore} which
 * uses a JPS Container
 * 
 * @version $Revision: 1.7 $
 */
public class KahaMessageStore extends AbstractMessageStore {

    protected final MapContainer<MessageId, Message> messageContainer;
    protected StoreEntry batchEntry;

    public KahaMessageStore(MapContainer<MessageId, Message> container, ActiveMQDestination destination)
        throws IOException {
        super(destination);
        this.messageContainer = container;
    }

    protected MessageId getMessageId(Object object) {
        return ((Message)object).getMessageId();
    }

    public Object getId() {
        return messageContainer.getId();
    }

    public synchronized void addMessage(ConnectionContext context, Message message) throws IOException {
        messageContainer.put(message.getMessageId(), message);
        // TODO: we should do the following but it is not need if the message is
        // being added within a persistence
        // transaction
        // but since I can't tell if one is running right now.. I'll leave this
        // out for now.
        // if( message.isResponseRequired() ) {
        // messageContainer.force();
        // }
    }

    public synchronized Message getMessage(MessageId identity) throws IOException {
        Message result = messageContainer.get(identity);
        return result;
    }

    protected boolean recoverMessage(MessageRecoveryListener listener, Message msg) throws Exception {
        listener.recoverMessage(msg);
        return listener.hasSpace();
    }

    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
        removeMessage(ack.getLastMessageId());
    }

    public synchronized void removeMessage(MessageId msgId) throws IOException {
        StoreEntry entry = messageContainer.getEntry(msgId);
        if (entry != null) {
            messageContainer.remove(entry);
            if (messageContainer.isEmpty() || (batchEntry != null && batchEntry.equals(entry))) {
                resetBatching();
            }
        }
    }

    public synchronized void recover(MessageRecoveryListener listener) throws Exception {
        for (StoreEntry entry = messageContainer.getFirst(); entry != null; entry = messageContainer
            .getNext(entry)) {
            Message msg = (Message)messageContainer.getValue(entry);
            if (!recoverMessage(listener, msg)) {
                break;
            }
        }
    }

    public synchronized void removeAllMessages(ConnectionContext context) throws IOException {
        messageContainer.clear();
    }

    public synchronized void delete() {
        messageContainer.clear();
    }

    /**
     * @return the number of messages held by this destination
     * @see org.apache.activemq.store.MessageStore#getMessageCount()
     */
    public int getMessageCount() {
        return messageContainer.size();
    }

    /**
     * @param id
     * @return null
     * @throws Exception
     * @see org.apache.activemq.store.MessageStore#getPreviousMessageIdToDeliver(org.apache.activemq.command.MessageId)
     */
    public MessageId getPreviousMessageIdToDeliver(MessageId id) throws Exception {
        return null;
    }

    /**
     * @param lastMessageId
     * @param maxReturned
     * @param listener
     * @throws Exception
     * @see org.apache.activemq.store.MessageStore#recoverNextMessages(org.apache.activemq.command.MessageId,
     *      int, org.apache.activemq.store.MessageRecoveryListener)
     */
    public synchronized void recoverNextMessages(int maxReturned, MessageRecoveryListener listener)
        throws Exception {
        StoreEntry entry = batchEntry;
        if (entry == null) {
            entry = messageContainer.getFirst();
        } else {
            entry = messageContainer.refresh(entry);
            entry = messageContainer.getNext(entry);
            if (entry == null) {
                batchEntry = null;
            }
        }
        if (entry != null) {
            int count = 0;
            do {
                Message msg = messageContainer.getValue(entry);
                if (msg != null) {
                    recoverMessage(listener, msg);
                    count++;
                }
                batchEntry = entry;
                entry = messageContainer.getNext(entry);
            } while (entry != null && count < maxReturned && listener.hasSpace());
        }
    }

    /**
     * @param nextToDispatch
     * @see org.apache.activemq.store.MessageStore#resetBatching(org.apache.activemq.command.MessageId)
     */
    public synchronized void resetBatching() {
        batchEntry = null;
    }

    /**
     * @return true if the store supports cursors
     */
    public boolean isSupportForCursors() {
        return true;
    }

    @Override
    public void setBatch(MessageId messageId) {
        batchEntry = messageContainer.getEntry(messageId);
    }
    
}
