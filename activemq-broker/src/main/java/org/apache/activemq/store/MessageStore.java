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

/**
 * Represents a message store which is used by the persistent implementations
 *
 *
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
     * Adds a message to the message store
     *
     * @param context context
     * @param message
     * @param canOptimizeHint - give a hint to the store that the message may be consumed before it hits the disk
     * @throws IOException
     */
    void addMessage(ConnectionContext context, Message message, boolean canOptimizeHint) throws IOException;

    /**
     * Adds a message to the message store
     *
     * @param context context
     * @param message
     * @return a Future to track when this is complete
     * @throws IOException
     * @throws IOException
     */
    ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, Message message) throws IOException;

    /**
     * Adds a message to the message store
     *
     * @param context context
     * @param message
     * @param canOptimizeHint - give a hint to the store that the message may be consumed before it hits the disk
     * @return a Future to track when this is complete
     * @throws IOException
     * @throws IOException
     */
    ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, Message message, boolean canOptimizeHint) throws IOException;

    /**
     * Adds a message to the message store
     *
     * @param context context
     * @param message
     * @return a ListenableFuture to track when this is complete
     * @throws IOException
     * @throws IOException
     */
    ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, Message message) throws IOException;

    /**
     * Adds a message to the message store
     *
     * @param context context
     * @param message
     *  @param canOptimizeHint - give a hint to the store that the message may be consumed before it hits the disk
     * @return a ListenableFuture to track when this is complete
     * @throws IOException
     * @throws IOException
     */
    ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, Message message, boolean canOptimizeHint) throws IOException;

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

    void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException;

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
     * @param memoryUsage The SystemUsage that is controlling the
     *                destination's memory usage.
     */
    void setMemoryUsage(MemoryUsage memoryUsage);

    /**
     * @return the number of messages ready to deliver
     * @throws IOException
     *
     */
    int getMessageCount() throws IOException;

    /**
     * @return the size of the messages ready to deliver
     * @throws IOException
     */
    long getMessageSize() throws IOException;


    /**
     * @return The statistics bean for this message store
     */
    MessageStoreStatistics getMessageStoreStatistics();

    /**
     * A hint to the Store to reset any batching state for the Destination
     *
     */
    void resetBatching();

    void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception;

    void dispose(ConnectionContext context);

    /**
     * allow caching cursors to set the current batch offset when cache is exhausted
     * @param messageId
     * @throws Exception
     */
    void setBatch(MessageId messageId) throws Exception;

    /**
     * flag to indicate if the store is empty
     * @return true if the message count is 0
     * @throws Exception
     */
    boolean isEmpty() throws Exception;

    /**
     * A hint to the store to try recover messages according to priority
     * @param prioritizedMessages
     */
    public void setPrioritizedMessages(boolean prioritizedMessages);

    /**
     *
     * @return true if store is trying to recover messages according to priority
     */
    public boolean isPrioritizedMessages();

    void updateMessage(Message message) throws IOException;

    void registerIndexListener(IndexListener indexListener);
}
