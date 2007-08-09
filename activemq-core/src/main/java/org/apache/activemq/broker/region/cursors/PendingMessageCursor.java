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
package org.apache.activemq.broker.region.cursors;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.memory.UsageManager;

/**
 * Interface to pending message (messages awaiting disptach to a consumer)
 * cursor
 * 
 * @version $Revision$
 */
public interface PendingMessageCursor extends Service {

    /**
     * Add a destination
     * 
     * @param context
     * @param destination
     * @throws Exception
     */
    public void add(ConnectionContext context, Destination destination) throws Exception;

    /**
     * remove a destination
     * 
     * @param context
     * @param destination
     * @throws Exception
     */
    public void remove(ConnectionContext context, Destination destination) throws Exception;

    /**
     * @return true if there are no pending messages
     */
    public boolean isEmpty();

    /**
     * check if a Destination is Empty for this cursor
     * 
     * @param destination
     * @return true id the Destination is empty
     */
    public boolean isEmpty(Destination destination);

    /**
     * reset the cursor
     */
    public void reset();

    /**
     * hint to the cursor to release any locks it might have grabbed after a
     * reset
     */
    public void release();

    /**
     * add message to await dispatch
     * 
     * @param node
     * @throws IOException
     * @throws Exception
     */
    public void addMessageLast(MessageReference node) throws Exception;

    /**
     * add message to await dispatch
     * 
     * @param node
     * @throws Exception
     */
    public void addMessageFirst(MessageReference node) throws Exception;

    /**
     * Add a message recovered from a retroactive policy
     * 
     * @param node
     * @throws Exception
     */
    public void addRecoveredMessage(MessageReference node) throws Exception;

    /**
     * @return true if there pending messages to dispatch
     */
    public boolean hasNext();

    /**
     * @return the next pending message
     */
    public MessageReference next();

    /**
     * remove the message at the cursor position
     */
    public void remove();

    /**
     * @return the number of pending messages
     */
    public int size();

    /**
     * clear all pending messages
     */
    public void clear();

    /**
     * Informs the Broker if the subscription needs to intervention to recover
     * it's state e.g. DurableTopicSubscriber may do
     * 
     * @return true if recovery required
     */
    public boolean isRecoveryRequired();

    /**
     * @return the maximum batch size
     */
    public int getMaxBatchSize();

    /**
     * Set the max batch size
     * 
     * @param maxBatchSize
     */
    public void setMaxBatchSize(int maxBatchSize);

    /**
     * Give the cursor a hint that we are about to remove messages from memory
     * only
     */
    public void resetForGC();

    /**
     * remove a node
     * 
     * @param node
     */
    public void remove(MessageReference node);

    /**
     * free up any internal buffers
     */
    public void gc();

    /**
     * Set the UsageManager
     * 
     * @param usageManager
     * @see org.apache.activemq.memory.UsageManager
     */
    public void setUsageManager(UsageManager usageManager);

    /**
     * @return the usageManager
     */
    public UsageManager getUsageManager();

    /**
     * @return the memoryUsageHighWaterMark
     */
    public int getMemoryUsageHighWaterMark();

    /**
     * @param memoryUsageHighWaterMark the memoryUsageHighWaterMark to set
     */
    public void setMemoryUsageHighWaterMark(int memoryUsageHighWaterMark);

    /**
     * @return true if the cursor is full
     */
    public boolean isFull();

    /**
     * @return true if the cursor has buffered messages ready to deliver
     */
    public boolean hasMessagesBufferedToDeliver();

    /**
     * destroy the cursor
     * 
     * @throws Exception
     */
    public void destroy() throws Exception;

    /**
     * Page in a restricted number of messages
     * 
     * @param maxItems
     * @return a list of paged in messages
     */
    public LinkedList pageInList(int maxItems);

}
