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

/**
 * A useful base class for implementation inheritence.
 * 
 * 
 */
public abstract class MessageEvictionStrategySupport implements MessageEvictionStrategy {

    private int evictExpiredMessagesHighWatermark = 1000;
    private boolean expiryCheckEnabled = true;

    public int getEvictExpiredMessagesHighWatermark() {
        return evictExpiredMessagesHighWatermark;
    }

    /**
     * Sets the high water mark on which we will eagerly evict expired messages from RAM
     */
    public void setEvictExpiredMessagesHighWatermark(int evictExpiredMessagesHighWaterMark) {
        this.evictExpiredMessagesHighWatermark = evictExpiredMessagesHighWaterMark;
    }

    @Override
    public boolean isExpiryCheckEnabled() {
        return expiryCheckEnabled;
    }

    /**
     * Controls whether the broker performs an eager expired-message scan when a
     * non-durable topic subscription's pending slow-consumer buffer exceeds
     * {@link #getEvictExpiredMessagesHighWatermark()}.
     * <p>
     * Set to {@code false} when messages carry no TTL, or when the O(n) scan cost
     * outweighs the benefit of eagerly evicting expired messages from slow-consumer
     * buffers. When messages have no TTL, every scan iterates the full buffer without
     * removing anything, adding latency to every enqueue once the buffer exceeds the
     * high-water mark.
     *
     * @param expiryCheckEnabled {@code false} to skip the scan; {@code true} to enable it (default)
     */
    public void setExpiryCheckEnabled(boolean expiryCheckEnabled) {
        this.expiryCheckEnabled = expiryCheckEnabled;
    }

}
