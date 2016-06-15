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
package org.apache.activemq.broker.region.cursors;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.usage.SystemUsage;

/**
 * Interface to pending message (messages awaiting disptach to a consumer)
 * cursor
 *
 *
 */
public interface PendingMessageCursor extends Service {

    static final long INFINITE_WAIT = 0;

    /**
     * Add a destination
     *
     * @param context
     * @param destination
     * @throws Exception
     */
    void add(ConnectionContext context, Destination destination) throws Exception;

    /**
     * remove a destination
     *
     * @param context
     * @param destination
     * @throws Exception
     */
    List<MessageReference> remove(ConnectionContext context, Destination destination) throws Exception;

    /**
     * @return true if there are no pending messages
     */
    boolean isEmpty();

    /**
     * check if a Destination is Empty for this cursor
     *
     * @param destination
     * @return true id the Destination is empty
     */
    boolean isEmpty(Destination destination);

    /**
     * reset the cursor
     */
    void reset();

    /**
     * hint to the cursor to release any locks it might have grabbed after a
     * reset
     */
    void release();

    /**
     * add message to await dispatch
     *
     * @param node
     * @return boolean true if successful, false if cursor traps a duplicate
     * @throws IOException
     * @throws Exception
     */
    boolean addMessageLast(MessageReference node) throws Exception;

    /**
     * add message to await dispatch - if it can
     *
     * @param node
     * @param maxWaitTime
     * @return true if successful
     * @throws IOException
     * @throws Exception
     */
    boolean tryAddMessageLast(MessageReference node, long maxWaitTime) throws Exception;

    /**
     * add message to await dispatch
     *
     * @param node
     * @throws Exception
     */
    void addMessageFirst(MessageReference node) throws Exception;

    /**
     * Add a message recovered from a retroactive policy
     *
     * @param node
     * @throws Exception
     */
    void addRecoveredMessage(MessageReference node) throws Exception;

    /**
     * @return true if there pending messages to dispatch
     */
    boolean hasNext();

    /**
     * @return the next pending message with its reference count increment
     */
    MessageReference next();

    /**
     * remove the message at the cursor position
     */
    void remove();

    /**
     * @return the number of pending messages
     */
    int size();

    long messageSize();

    /**
     * clear all pending messages
     */
    void clear();

    /**
     * Informs the Broker if the subscription needs to intervention to recover
     * it's state e.g. DurableTopicSubscriber may do
     *
     * @return true if recovery required
     */
    boolean isRecoveryRequired();

    /**
     * @return the maximum batch size
     */
    int getMaxBatchSize();

    /**
     * Set the max batch size
     *
     * @param maxBatchSize
     */
    void setMaxBatchSize(int maxBatchSize);

    /**
     * Give the cursor a hint that we are about to remove messages from memory
     * only
     */
    void resetForGC();

    /**
     * remove a node
     *
     * @param node
     */
    void remove(MessageReference node);

    /**
     * free up any internal buffers
     */
    void gc();

    /**
     * Set the UsageManager
     *
     * @param systemUsage
     * @see org.apache.activemq.usage.SystemUsage
     */
    void setSystemUsage(SystemUsage systemUsage);

    /**
     * @return the usageManager
     */
    SystemUsage getSystemUsage();

    /**
     * @return the memoryUsageHighWaterMark
     */
    int getMemoryUsageHighWaterMark();

    /**
     * @param memoryUsageHighWaterMark the memoryUsageHighWaterMark to set
     */
    void setMemoryUsageHighWaterMark(int memoryUsageHighWaterMark);

    /**
     * @return true if the cursor is full
     */
    boolean isFull();

    /**
     * @return true if the cursor has space to page messages into
     */
    public boolean hasSpace();

    /**
     * @return true if the cursor has buffered messages ready to deliver
     */
    boolean hasMessagesBufferedToDeliver();

    /**
     * destroy the cursor
     *
     * @throws Exception
     */
    void destroy() throws Exception;

    /**
     * Page in a restricted number of messages and increment the reference count
     *
     * @param maxItems
     * @return a list of paged in messages
     */
    LinkedList<MessageReference> pageInList(int maxItems);

    /**
     * set the maximum number of producers to track at one time
     * @param value
     */
    void setMaxProducersToAudit(int value);

    /**
     * @return the maximum number of producers to audit
     */
    int getMaxProducersToAudit();

    /**
     * Set the maximum depth of message ids to track
     * @param depth
     */
    void setMaxAuditDepth(int depth);

    /**
     * @return the audit depth
     */
    int getMaxAuditDepth();

    /**
     * @return the enableAudit
     */
    public boolean isEnableAudit();
    /**
     * @param enableAudit the enableAudit to set
     */
    public void setEnableAudit(boolean enableAudit);

    /**
     * @return true if the underlying state of this cursor
     * disappears when the broker shuts down
     */
    public boolean isTransient();


    /**
     * set the audit
     * @param audit
     */
    public void setMessageAudit(ActiveMQMessageAudit audit);


    /**
     * @return the audit - could be null
     */
    public ActiveMQMessageAudit getMessageAudit();

    /**
     * use a cache to improve performance
     * @param useCache
     */
    public void setUseCache(boolean useCache);

    /**
     * @return true if a cache may be used
     */
    public boolean isUseCache();

    /**
     * remove from auditing the message id
     * @param id
     */
    public void rollback(MessageId id);

    /**
     * @return true if cache is being used
     */
    public boolean isCacheEnabled();

    public void rebase();

}
