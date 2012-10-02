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
package org.apache.activemq.broker.region.policy;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.StoreDurableSubscriberCursor;
import org.apache.activemq.kaha.Store;

/**
 * Creates a PendingMessageCursor that access the persistent store to retrieve
 * messages
 * 
 * @org.apache.xbean.XBean element="storeDurableSubscriberCursor"
 *                         description="Pending messages for a durable
 *                         subscriber are referenced from the Store"
 * 
 */
public class StorePendingDurableSubscriberMessageStoragePolicy implements PendingDurableSubscriberMessageStoragePolicy {
    boolean immediatePriorityDispatch = true;
    boolean useCache = true;

    public boolean isImmediatePriorityDispatch() {
        return immediatePriorityDispatch;
    }

    /**
     * Ensure that new higher priority messages will get an immediate dispatch
     * rather than wait for the end of the current cursor batch.
     * Useful when there is a large message backlog and intermittent high priority messages.
     *
     * @param immediatePriorityDispatch
     */
    public void setImmediatePriorityDispatch(boolean immediatePriorityDispatch) {
        this.immediatePriorityDispatch = immediatePriorityDispatch;
    }

    public boolean isUseCache() {
        return useCache;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    /**
     * Retrieve the configured pending message storage cursor;
     * @param broker 
     * 
     * @param clientId
     * @param name
     * @param maxBatchSize
     * @param sub 
     * @return the Pending Message cursor
     */
    public PendingMessageCursor getSubscriberPendingMessageCursor(Broker broker,String clientId, String name, int maxBatchSize, DurableTopicSubscription sub) {
        StoreDurableSubscriberCursor cursor = new StoreDurableSubscriberCursor(broker,clientId, name, maxBatchSize, sub);
        cursor.setUseCache(isUseCache());
        cursor.setImmediatePriorityDispatch(isImmediatePriorityDispatch());
        return cursor;
    }
}
