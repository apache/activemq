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
package org.apache.activemq.management;

import java.util.Set;

public class MessageFlowStatsImpl extends UnsampledStatsImpl implements MessageFlowStats, Statistic, Resettable {

    private final UnsampledStatisticImpl<Long> enqueuedMessageBrokerInTime;
    private final UnsampledStatisticImpl<String> enqueuedMessageClientID;
    private final UnsampledStatisticImpl<String> enqueuedMessageID;
    private final UnsampledStatisticImpl<Long> enqueuedMessageTimestamp;
    private final UnsampledStatisticImpl<Long> dequeuedMessageBrokerInTime;
    private final UnsampledStatisticImpl<Long> dequeuedMessageBrokerOutTime;
    private final UnsampledStatisticImpl<String> dequeuedMessageClientID;
    private final UnsampledStatisticImpl<String> dequeuedMessageID;
    private final UnsampledStatisticImpl<Long> dequeuedMessageTimestamp;

    public MessageFlowStatsImpl() {
        super();

        enqueuedMessageBrokerInTime = new UnsampledStatisticImpl<>("enqueuedMessageBrokerInTime", "ms", "Broker in time (ms) of last enqueued message to the destination", Long.valueOf(0l));
        enqueuedMessageClientID = new UnsampledStatisticImpl<>("enqueuedMessageClientID", "id", "ClientID of last enqueued message to the destination", null);
        enqueuedMessageID = new UnsampledStatisticImpl<>("enqueuedMessageID", "id", "MessageID of last enqueued message to the destination", null);
        enqueuedMessageTimestamp = new UnsampledStatisticImpl<>("enqueuedMessageTimestamp", "ms", "Message timestamp of last enqueued message to the destination", Long.valueOf(0l));

        dequeuedMessageBrokerInTime = new UnsampledStatisticImpl<>("dequeuedMessageBrokerInTime", "ms", "Broker in time (ms) of last dequeued message to the destination", Long.valueOf(0l));
        dequeuedMessageBrokerOutTime = new UnsampledStatisticImpl<>("dequeuedMessageBrokerOutTime", "ms", "Broker out time (ms) of last dequeued message to the destination", Long.valueOf(0l));
        dequeuedMessageClientID = new UnsampledStatisticImpl<>("dequeuedMessageClientID", "id", "ClientID of last dequeued message to the destination", null);
        dequeuedMessageID = new UnsampledStatisticImpl<>("dequeuedMessageID", "id", "MessageID of last dequeued message to the destination", null);
        dequeuedMessageTimestamp = new UnsampledStatisticImpl<>("dequeuedMessageTimestamp", "ms", "Message timestamp of last dequeued message to the destination", Long.valueOf(0l));

        addStatistics(Set.of(enqueuedMessageBrokerInTime, enqueuedMessageClientID, enqueuedMessageID, enqueuedMessageTimestamp,
                dequeuedMessageBrokerInTime, dequeuedMessageBrokerOutTime, dequeuedMessageClientID, dequeuedMessageID, dequeuedMessageTimestamp));
    }

    @Override
    public UnsampledStatistic<Long> getEnqueuedMessageBrokerInTime() {
        return enqueuedMessageBrokerInTime;
    }

    @Override
    public UnsampledStatistic<String> getEnqueuedMessageClientID() {
        return enqueuedMessageClientID;
    }

    @Override
    public UnsampledStatistic<String> getEnqueuedMessageID() {
        return enqueuedMessageID;
    }

    @Override
    public UnsampledStatistic<Long> getEnqueuedMessageTimestamp() {
        return enqueuedMessageTimestamp;
    }

    @Override
    public UnsampledStatistic<Long> getDequeuedMessageBrokerInTime() {
        return dequeuedMessageBrokerInTime;
    }

    @Override
    public UnsampledStatistic<Long> getDequeuedMessageBrokerOutTime() {
        return dequeuedMessageBrokerOutTime;
    }

    @Override
    public UnsampledStatistic<String> getDequeuedMessageClientID() {
        return dequeuedMessageClientID;
    }

    @Override
    public UnsampledStatistic<String> getDequeuedMessageID() {
        return dequeuedMessageID;
    }

    @Override
    public UnsampledStatistic<Long> getDequeuedMessageTimestamp() {
        return dequeuedMessageTimestamp;
    }

    @Override
    public synchronized void enqueueStats(String clientID, String messageID, long messageTimestamp, long messageBrokerInTime) {
        enqueuedMessageClientID.setValue(clientID);
        enqueuedMessageID.setValue(messageID);
        enqueuedMessageTimestamp.setValue(messageTimestamp);
        enqueuedMessageBrokerInTime.setValue(messageBrokerInTime);
    }

    @Override
    public synchronized void dequeueStats(String clientID, String messageID) {
        dequeuedMessageClientID.setValue(clientID);
        dequeuedMessageID.setValue(messageID);
    }

    @Override
    public synchronized void dequeueStats(String clientID, String messageID, long messageTimestamp, long messageBrokerInTime, long messageBrokerOutTime) {
        dequeuedMessageClientID.setValue(clientID);
        dequeuedMessageID.setValue(messageID);
        dequeuedMessageTimestamp.setValue(messageTimestamp);
        dequeuedMessageBrokerInTime.setValue(messageBrokerInTime);
        dequeuedMessageBrokerOutTime.setValue(messageBrokerOutTime);
    }
}
