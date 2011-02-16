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

import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.StoreEntry;

/**
 * Holds information for location of message
 * 
 * 
 */
public class ConsumerMessageRef {

    private MessageId messageId;
    private StoreEntry messageEntry;
    private StoreEntry ackEntry;

    /**
     * @return the ackEntry
     */
    public StoreEntry getAckEntry() {
        return this.ackEntry;
    }

    /**
     * @param ackEntry the ackEntry to set
     */
    public void setAckEntry(StoreEntry ackEntry) {
        this.ackEntry = ackEntry;
    }

    /**
     * @return the messageEntry
     */
    public StoreEntry getMessageEntry() {
        return this.messageEntry;
    }

    /**
     * @param messageEntry the messageEntry to set
     */
    public void setMessageEntry(StoreEntry messageEntry) {
        this.messageEntry = messageEntry;
    }

    /**
     * @return the messageId
     */
    public MessageId getMessageId() {
        return this.messageId;
    }

    /**
     * @param messageId the messageId to set
     */
    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }

    public String toString() {
        return "ConsumerMessageRef[" + messageId + "]";
    }

}
