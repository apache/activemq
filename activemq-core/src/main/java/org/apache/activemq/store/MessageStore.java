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
package org.apache.activemq.store;

import java.io.IOException;

import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;

/**
 * Represents a message store which is used by the persistent {@link org.apache.activemq.service.MessageContainer}
 * implementations
 *
 * @version $Revision: 1.5 $
 */
public interface MessageStore extends Service {
    
    /**
     * Adds a message to the message store
     * @param context TODO
     */
    public void addMessage(ConnectionContext context, Message message) throws IOException;

    /**
     * Adds a message reference to the message store
     * @param context TODO
     * @param messageId TODO
     * @param expirationTime TODO
     */
    public void addMessageReference(ConnectionContext context, MessageId messageId, long expirationTime, String messageRef) throws IOException;

    /**
     * Looks up a message using either the String messageID or
     * the messageNumber. Implementations are encouraged to fill in the missing
     * key if its easy to do so.
     * @param identity which contains either the messageID or the messageNumber
     * @return the message or null if it does not exist
     */
    public Message getMessage(MessageId identity) throws IOException;

    /**
     * Looks up a message using either the String messageID or
     * the messageNumber. Implementations are encouraged to fill in the missing
     * key if its easy to do so.
     * @param identity which contains either the messageID or the messageNumber
     * @return the message or null if it does not exist
     */
    public String getMessageReference(MessageId identity) throws IOException;

    /**
     * Removes a message from the message store.
     * @param context TODO
     * @param ack the ack request that cause the message to be removed.  It conatins 
     *   the identity which contains the messageID of the message that needs to be removed.
     */
    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException;

    /**
     * Removes all the messages from the message store.
     * @param context TODO
     */
    public void removeAllMessages(ConnectionContext context) throws IOException;
    
    /**
     * Recover any messages to be delivered.
     *
     * @param container
     * @throws Exception 
     */
    public void recover(MessageRecoveryListener container) throws Exception;

    /**
     * The destination that the message store is holding messages for.
     * @return
     */
    public ActiveMQDestination getDestination();
        
}
