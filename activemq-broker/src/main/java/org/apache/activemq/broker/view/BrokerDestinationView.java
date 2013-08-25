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


    public BrokerDestinationView(Destination destination) {
        this.destination = destination;
    }



    public String getName() {
        return destination.getName();
    }


    public long getEnqueueCount() {
        return destination.getDestinationStatistics().getEnqueues().getCount();
    }

    public long getDequeueCount() {
        return destination.getDestinationStatistics().getDequeues().getCount();
    }


    public long getDispatchCount() {
        return destination.getDestinationStatistics().getDispatched().getCount();
    }


    public long getInFlightCount() {
        return destination.getDestinationStatistics().getInflight().getCount();
    }


    public long getExpiredCount() {
        return destination.getDestinationStatistics().getExpired().getCount();
    }


    public long getConsumerCount() {
        return destination.getDestinationStatistics().getConsumers().getCount();
    }


    public long getQueueSize() {
        return destination.getDestinationStatistics().getMessages().getCount();
    }

    public long getMessagesCached() {
        return destination.getDestinationStatistics().getMessagesCached().getCount();
    }


    public int getMemoryPercentUsage() {
        return destination.getMemoryUsage().getPercentUsage();
    }


    public long getMemoryUsageByteCount() {
        return destination.getMemoryUsage().getUsage();
    }


    public long getMemoryLimit() {
        return destination.getMemoryUsage().getLimit();
    }


    public void setMemoryLimit(long limit) {
        destination.getMemoryUsage().setLimit(limit);
    }


    public double getAverageEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getAverageTime();
    }


    public long getMaxEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getMaxTime();
    }


    public long getMinEnqueueTime() {
        return destination.getDestinationStatistics().getProcessTime().getMinTime();
    }


    public float getMemoryUsagePortion() {
        return destination.getMemoryUsage().getUsagePortion();
    }

    public long getProducerCount() {
        return destination.getDestinationStatistics().getProducers().getCount();
    }


    public boolean isDLQ() {
        return destination.isDLQ();
    }


    public long getBlockedSends() {
        return destination.getDestinationStatistics().getBlockedSends().getCount();
    }


    public double getAverageBlockedTime() {
        return destination.getDestinationStatistics().getBlockedTime().getAverageTime();
    }


    public long getTotalBlockedTime() {
        return destination.getDestinationStatistics().getBlockedTime().getTotalTime();
    }
}
