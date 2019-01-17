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
package org.apache.activemq.broker.view;

import org.apache.activemq.broker.region.Destination;

public class BrokerDestinationView {
    private final Destination destination;


     BrokerDestinationView(Destination destination) {
        this.destination = destination;
    }


    /**
     * @return the name of the DestinationView
     */
    public String getName() {
        return destination.getName();
    }

    /**
     * @return the number of messages enqueued by this destination
     */

    public long getEnqueueCount() {
        return destination.getDestinationStatistics().getEnqueues().getCount();
    }

    /**
     * @return the number of messages dequeued (dispatched and removed) by this destination
     */
    public long getDequeueCount() {
        return destination.getDestinationStatistics().getDequeues().getCount();
    }

    /**
     * @return the number of messages dispatched by this destination
     */
    public long getDispatchCount() {
        return destination.getDestinationStatistics().getDispatched().getCount();
    }

    /**
     * @return the number of messages inflight (dispatched by not acknowledged) by this destination
     */
    public long getInFlightCount() {
        return destination.getDestinationStatistics().getInflight().getCount();
    }

    /**
     * @return the number of messages expired by this destination
     */
    public long getExpiredCount() {
        return destination.getDestinationStatistics().getExpired().getCount();
    }

    /**
     * @return the number of active consumers on this destination
     */
    public int getConsumerCount() {
        return (int)destination.getDestinationStatistics().getConsumers().getCount();
    }

    /**
     * @return the number of active consumers on this destination
     */
    public int getProducerCount() {
        return (int)destination.getDestinationStatistics().getProducers().getCount();
    }

    /**
     * @return the depth of the Destination
     */
    public long getQueueSize() {
        return destination.getDestinationStatistics().getMessages().getCount();
    }

    /**
     * @return the number of messages cached in memory by this destination
     */
    public long getMessagesCached() {
        return destination.getDestinationStatistics().getMessagesCached().getCount();
    }

    /**
     * @return the memory usage as a percentage for this Destination
     */
    public int getMemoryPercentUsage() {
        return destination.getMemoryUsage().getPercentUsage();
    }

    /**
     * @return the memory used by this destination in bytes
     */
    public long getMemoryUsageByteCount() {
        return destination.getMemoryUsage().getUsage();
    }


    /**
     * @return  the memory limit for this destination in bytes
     */
    public long getMemoryLimit() {
        return destination.getMemoryUsage().getLimit();
    }

    /**
     * Gets the temp usage as a percentage for this Destination.
     *
     * @return Gets the temp usage as a percentage for this Destination.
     */
    public int getTempPercentUsage() {
        return destination.getTempUsage().getPercentUsage();
    }

    /**
     * Gets the temp usage limit in bytes.
     *
     * @return the temp usage limit in bytes.
     */
    public long getTempUsageLimit() {
        return destination.getTempUsage().getLimit();
    }

    /**
     * @return the average time it takes to store a message on this destination (ms)
     */
    public double getAverageEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getAverageTime();
    }

    /**
     * @return the maximum time it takes to store a message on this destination (ms)
     */
    public long getMaxEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getMaxTime();
    }

    /**
     * @return the minimum time it takes to store a message on this destination (ms)
     */

    public long getMinEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getMinTime();
    }

    /**
     * @return the average size of a message (bytes)
     */
    public double getAverageMessageSize() {
        return destination.getDestinationStatistics().getMessageSize().getAverageSize();
    }

    /**
      * @return the max size of a message (bytes)
    */
    public long getMaxMessageSize() {
        return destination.getDestinationStatistics().getMessageSize().getMaxSize();
    }

    /**
     * @return the min size of a message (bytes)
     */
    public long getMinMessageSize() {
        return destination.getDestinationStatistics().getMessageSize().getMinSize();
    }


    /**
     * @return true if the destination is a Dead Letter Queue
     */
    public boolean isDLQ() {
        return destination.getActiveMQDestination().isDLQ();
    }


    /**
     * @return the number of messages blocked waiting for dispatch (indication of slow consumption if greater than zero)
     */
    public long getBlockedSends() {
        return destination.getDestinationStatistics().getBlockedSends().getCount();
    }

    /**
     * @return the average time(ms) messages are  blocked waiting for dispatch (indication of slow consumption if greater than zero)
     */

    public double getAverageBlockedTime() {
        return destination.getDestinationStatistics().getBlockedTime().getAverageTime();
    }

    /**
     * @return the total time(ms) messages are  blocked waiting for dispatch (indication of slow consumption if greater than zero)
     */

    public long getTotalBlockedTime() {
        return destination.getDestinationStatistics().getBlockedTime().getTotalTime();
    }
}
