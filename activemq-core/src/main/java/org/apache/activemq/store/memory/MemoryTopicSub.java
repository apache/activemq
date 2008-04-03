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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;

/**
 * A holder for a durable subscriber
 * 
 * @version $Revision: 1.7 $
 */
class MemoryTopicSub {

    private Map<MessageId, Message> map = new LinkedHashMap<MessageId, Message>();
    private MessageId lastBatch;

    void addMessage(MessageId id, Message message) {
        synchronized(this) {
            map.put(id, message);
        }
        message.incrementReferenceCount();
    }

    void removeMessage(MessageId id) {
        Message removed;
        synchronized(this) {
            removed = map.remove(id);
            if ((lastBatch != null && lastBatch.equals(id)) || map.isEmpty()) {
                resetBatching();
            }
        }
        if( removed!=null ) {
            removed.decrementReferenceCount();
        }
    }

    synchronized int size() {
        return map.size();
    }

    synchronized void recoverSubscription(MessageRecoveryListener listener) throws Exception {
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Entry)iter.next();
            Object msg = entry.getValue();
            if (msg.getClass() == MessageId.class) {
                listener.recoverMessageReference((MessageId)msg);
            } else {
                listener.recoverMessage((Message)msg);
            }
        }
    }

    synchronized void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception {
        boolean pastLackBatch = lastBatch == null;
        MessageId lastId = null;
        // the message table is a synchronizedMap - so just have to synchronize
        // here
        int count = 0;
        for (Iterator iter = map.entrySet().iterator(); iter.hasNext() && count < maxReturned;) {
            Map.Entry entry = (Entry)iter.next();
            if (pastLackBatch) {
                count++;
                Object msg = entry.getValue();
                lastId = (MessageId)entry.getKey();
                if (msg.getClass() == MessageId.class) {
                    listener.recoverMessageReference((MessageId)msg);
                } else {
                    listener.recoverMessage((Message)msg);
                }
            } else {
                pastLackBatch = entry.getKey().equals(lastBatch);
            }
        }
        if (lastId != null) {
            lastBatch = lastId;
        }

    }

    synchronized void resetBatching() {
        lastBatch = null;
    }
}
