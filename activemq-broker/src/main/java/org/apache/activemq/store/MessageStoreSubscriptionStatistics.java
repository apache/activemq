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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.management.CountStatisticImpl;
import org.apache.activemq.management.SizeStatisticImpl;

public class MessageStoreSubscriptionStatistics extends AbstractMessageStoreStatistics {

    private final ConcurrentMap<String, SubscriptionStatistics> subStatistics
        = new ConcurrentHashMap<>();

    /**
     * @param enabled
     */
    public MessageStoreSubscriptionStatistics(boolean enabled) {
        super(enabled, "The number of messages or this subscription in the message store",
                "Size of messages contained by this subscription in the message store");
    }

    /**
     * Total count for all subscriptions
     */
    @Override
    public CountStatisticImpl getMessageCount() {
        return this.messageCount;
    }

    /**
     * Total size for all subscriptions
     */
    @Override
    public SizeStatisticImpl getMessageSize() {
        return this.messageSize;
    }

    public CountStatisticImpl getMessageCount(String subKey) {
        return getOrInitStatistics(subKey).getMessageCount();
    }

    public SizeStatisticImpl getMessageSize(String subKey) {
        return getOrInitStatistics(subKey).getMessageSize();
    }

    public void removeSubscription(String subKey) {
        SubscriptionStatistics subStats = subStatistics.remove(subKey);
        //Subtract from the parent
        if (subStats != null) {
           getMessageCount().subtract(subStats.getMessageCount().getCount());
           getMessageSize().addSize(-subStats.getMessageSize().getTotalSize());
        }
    }

    @Override
    public void reset() {
        super.reset();
        subStatistics.clear();
    }

    private SubscriptionStatistics getOrInitStatistics(String subKey) {
        SubscriptionStatistics subStats = subStatistics.get(subKey);

        if (subStats == null) {
            final SubscriptionStatistics stats = new SubscriptionStatistics();
            subStats = subStatistics.putIfAbsent(subKey, stats);
            if (subStats == null) {
                subStats = stats;
            }
        }

        return subStats;
    }

    private class SubscriptionStatistics extends AbstractMessageStoreStatistics {

        public SubscriptionStatistics() {
            this(MessageStoreSubscriptionStatistics.this.enabled);
        }

        /**
         * @param enabled
         */
        public SubscriptionStatistics(boolean enabled) {
            super(enabled, "The number of messages or this subscription in the message store",
                    "Size of messages contained by this subscription in the message store");
            this.setParent(MessageStoreSubscriptionStatistics.this);
        }
    }

}
