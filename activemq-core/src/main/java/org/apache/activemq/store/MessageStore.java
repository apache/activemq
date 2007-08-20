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
package org.apache.activemq.store;

import java.io.IOException;

import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;

/**
 * Represents a message store which is used by the persistent implementations
 * 
 * @version $Revision: 1.5 $
 */
public interface MessageStore extends Service {

    /**
     * Adds a message to the message store
     * 
     * @param context context
     * @param message
     * @throws IOException
     */
    void addMessage(ConnectionContext context, Message message) throws IOException;

    /**
     * Looks up a message using either the String messageID or the
     * messageNumber. Implementations are encouraged to fill in the missing key
     * if its easy to do so.
     * 
     * @param identity which contains either the messageID or the messageNumber
     * @return the message or null if it does not exist
     * @throws IOException
     */
    Message getMessage(MessageId identity) throws IOException;

    /**
     * Removes a message from the message store.
     * 
     * @param context
     * @param ack the ack request that cause the message to be removed. It
     *                conatins the identity which contains the messageID of the
     *                message that needs to be removed.
     * @throws IOException
     */
    void removeMessage(ConnectionContext context, MessageAck ack) throws IOException;

    /**
     * Removes all the messages from the message store.
     * 
     * @param context
     * @throws IOException
     */
    void removeAllMessages(ConnectionContext context) throws IOException;

    /**
     * Recover any messages to be delivered.
     * 
     * @param container
     * @throws Exception
     */
    void recover(MessageRecoveryListener container) throws Exception;

    /**
     * The destination that the message store is holding messages for.
     * 
     * @return the destination
     */
    ActiveMQDestination getDestination();

    /**
     * @param memoeyUSage The SystemUsage that is controlling the
     *                destination's memory usage.
     */
    void setMemoryUsage(MemoryUsage memoeyUSage);

    /**
     * @return the number of messages ready to deliver
     * @throws IOException
     * 
     */
    int getMessageCount() throws IOException;

    /**
     * A hint to the Store to reset any batching state for the Destination
     * 
     */
    void resetBatching();

    void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception;

}
