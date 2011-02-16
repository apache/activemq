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
package org.apache.activemq;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the prefetch message policies for different types of consumers
 * 
 * @org.apache.xbean.XBean element="prefetchPolicy"
 * 
 */
public class ActiveMQPrefetchPolicy extends Object implements Serializable {
    public static final int MAX_PREFETCH_SIZE = Short.MAX_VALUE - 1;
    public static final int DEFAULT_QUEUE_PREFETCH = 1000;
    public static final int DEFAULT_QUEUE_BROWSER_PREFETCH = 500;
    public static final int DEFAULT_DURABLE_TOPIC_PREFETCH = 100;
    public static final int DEFAULT_OPTIMIZE_DURABLE_TOPIC_PREFETCH=1000;
    public static final int DEFAULT_INPUT_STREAM_PREFETCH=100;
    public static final int DEFAULT_TOPIC_PREFETCH = MAX_PREFETCH_SIZE;
    
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQPrefetchPolicy.class);
    
    private int queuePrefetch;
    private int queueBrowserPrefetch;
    private int topicPrefetch;
    private int durableTopicPrefetch;
    private int optimizeDurableTopicPrefetch;
    private int inputStreamPrefetch;
    private int maximumPendingMessageLimit;

    /**
     * Initialize default prefetch policies
     */
    public ActiveMQPrefetchPolicy() {
        this.queuePrefetch = DEFAULT_QUEUE_PREFETCH;
        this.queueBrowserPrefetch = DEFAULT_QUEUE_BROWSER_PREFETCH;
        this.topicPrefetch = DEFAULT_TOPIC_PREFETCH;
        this.durableTopicPrefetch = DEFAULT_DURABLE_TOPIC_PREFETCH;
        this.optimizeDurableTopicPrefetch = DEFAULT_OPTIMIZE_DURABLE_TOPIC_PREFETCH;
        this.inputStreamPrefetch = DEFAULT_INPUT_STREAM_PREFETCH;
    }

    /**
     * @return Returns the durableTopicPrefetch.
     */
    public int getDurableTopicPrefetch() {
        return durableTopicPrefetch;
    }

    /**
     * @param durableTopicPrefetch The durableTopicPrefetch to set.
     */
    public void setDurableTopicPrefetch(int durableTopicPrefetch) {
        this.durableTopicPrefetch = getMaxPrefetchLimit(durableTopicPrefetch);
    }

    /**
     * @return Returns the queuePrefetch.
     */
    public int getQueuePrefetch() {
        return queuePrefetch;
    }

    /**
     * @param queuePrefetch The queuePrefetch to set.
     */
    public void setQueuePrefetch(int queuePrefetch) {
        this.queuePrefetch = getMaxPrefetchLimit(queuePrefetch);
    }

    /**
     * @return Returns the queueBrowserPrefetch.
     */
    public int getQueueBrowserPrefetch() {
        return queueBrowserPrefetch;
    }

    /**
     * @param queueBrowserPrefetch The queueBrowserPrefetch to set.
     */
    public void setQueueBrowserPrefetch(int queueBrowserPrefetch) {
        this.queueBrowserPrefetch = getMaxPrefetchLimit(queueBrowserPrefetch);
    }

    /**
     * @return Returns the topicPrefetch.
     */
    public int getTopicPrefetch() {
        return topicPrefetch;
    }

    /**
     * @param topicPrefetch The topicPrefetch to set.
     */
    public void setTopicPrefetch(int topicPrefetch) {
        this.topicPrefetch = getMaxPrefetchLimit(topicPrefetch);
    }

    /**
     * @return Returns the optimizeDurableTopicPrefetch.
     */
    public int getOptimizeDurableTopicPrefetch() {
        return optimizeDurableTopicPrefetch;
    }

    /**
     * @param optimizeAcknowledgePrefetch The optimizeDurableTopicPrefetch to
     *                set.
     */
    public void setOptimizeDurableTopicPrefetch(int optimizeAcknowledgePrefetch) {
        this.optimizeDurableTopicPrefetch = optimizeAcknowledgePrefetch;
    }

    public int getMaximumPendingMessageLimit() {
        return maximumPendingMessageLimit;
    }

    /**
     * Sets how many messages a broker will keep around, above the prefetch
     * limit, for non-durable topics before starting to discard older messages.
     */
    public void setMaximumPendingMessageLimit(int maximumPendingMessageLimit) {
        this.maximumPendingMessageLimit = maximumPendingMessageLimit;
    }

    private int getMaxPrefetchLimit(int value) {
        int result = Math.min(value, MAX_PREFETCH_SIZE);
        if (result < value) {
            LOG.warn("maximum prefetch limit has been reset from " + value + " to " + MAX_PREFETCH_SIZE);
        }
        return result;
    }

    public void setAll(int i) {
        this.durableTopicPrefetch = i;
        this.queueBrowserPrefetch = i;
        this.queuePrefetch = i;
        this.topicPrefetch = i;
        this.inputStreamPrefetch = 1;
        this.optimizeDurableTopicPrefetch = i;
    }

    public int getInputStreamPrefetch() {
        return inputStreamPrefetch;
    }

    public void setInputStreamPrefetch(int inputStreamPrefetch) {
        this.inputStreamPrefetch = getMaxPrefetchLimit(inputStreamPrefetch);
    }
    
    public boolean equals(Object object){
        if (object instanceof ActiveMQPrefetchPolicy){
            ActiveMQPrefetchPolicy other = (ActiveMQPrefetchPolicy) object;
            return this.queuePrefetch == other.queuePrefetch &&
            this.queueBrowserPrefetch == other.queueBrowserPrefetch &&
            this.topicPrefetch == other.topicPrefetch &&
            this.durableTopicPrefetch == other.durableTopicPrefetch &&
            this.optimizeDurableTopicPrefetch == other.optimizeDurableTopicPrefetch &&
            this.inputStreamPrefetch == other.inputStreamPrefetch;
        }
        return false;
    }

}
