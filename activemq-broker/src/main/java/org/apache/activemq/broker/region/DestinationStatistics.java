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
package org.apache.activemq.broker.region;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.activemq.management.CountStatisticImpl;
import org.apache.activemq.management.PollCountStatisticImpl;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.management.*;

/**
 * The Statistics for a Destination.
 */
public class DestinationStatistics extends StatsImpl {

    private final CountStatisticImpl enqueues;
    private final CountStatisticImpl dequeues;
    private final CountStatisticImpl forwards;
    private final CountStatisticImpl consumers;
    private final CountStatisticImpl producers;
    private final CountStatisticImpl messages;
    private final PollCountStatisticImpl messagesCached;
    private final CountStatisticImpl dispatched;
    private final CountStatisticImpl duplicateFromStore;
    private final CountStatisticImpl inflight;
    private final CountStatisticImpl expired;
    private final TimeStatisticImpl processTime;
    private final CountStatisticImpl blockedSends;
    private final TimeStatisticImpl blockedTime;
    private final SizeStatisticImpl messageSize;
    private final CountStatisticImpl maxUncommittedExceededCount;

    // [AMQ-9437] Advanced Network Statistics
    private final CountStatisticImpl networkEnqueues;
    private final CountStatisticImpl networkDequeues;

    // [AMQ-8463] Advanced Message Statistics are disabled by default
    private final AtomicReference<MessageFlowStatsImpl> messageFlowStats = new AtomicReference<>();

    public DestinationStatistics() {

        enqueues = new CountStatisticImpl("enqueues", "The number of messages that have been sent to the destination");
        dispatched = new CountStatisticImpl("dispatched", "The number of messages that have been dispatched from the destination");
        dequeues = new CountStatisticImpl("dequeues", "The number of messages that have been acknowledged from the destination");
        duplicateFromStore = new CountStatisticImpl("duplicateFromStore", "The number of duplicate messages that have been paged-in from the store for this destination");
        forwards = new CountStatisticImpl("forwards", "The number of messages that have been forwarded to a networked broker from the destination");
        inflight = new CountStatisticImpl("inflight", "The number of messages dispatched but awaiting acknowledgement");
        expired = new CountStatisticImpl("expired", "The number of messages that have expired");

        consumers = new CountStatisticImpl("consumers", "The number of consumers that that are subscribing to messages from the destination");
        consumers.setDoReset(false);
        producers = new CountStatisticImpl("producers", "The number of producers that that are publishing messages to the destination");
        producers.setDoReset(false);
        messages = new CountStatisticImpl("messages", "The number of messages that that are being held by the destination");
        messages.setDoReset(false);
        messagesCached = new PollCountStatisticImpl("messagesCached", "The number of messages that are held in the destination's memory cache");
        processTime = new TimeStatisticImpl("processTime", "information around length of time messages are held by a destination");
        blockedSends = new CountStatisticImpl("blockedSends", "number of messages that have to wait for flow control");
        blockedTime = new TimeStatisticImpl("blockedTime","amount of time messages are blocked for flow control");
        messageSize = new SizeStatisticImpl("messageSize","Size of messages passing through the destination");
        maxUncommittedExceededCount = new CountStatisticImpl("maxUncommittedExceededCount", "number of times maxUncommittedCount has been exceeded");

        networkEnqueues = new CountStatisticImpl("networkEnqueues", "The number of messages that have been sent to the destination via network connection");
        networkDequeues = new CountStatisticImpl("networkDequeues", "The number of messages that have been acknowledged from the destination via network connection");
    }

    public CountStatisticImpl getEnqueues() {
        return enqueues;
    }

    public CountStatisticImpl getDequeues() {
        return dequeues;
    }

    public CountStatisticImpl getForwards() {
        return forwards;
    }

    public CountStatisticImpl getInflight() {
        return inflight;
    }

    public CountStatisticImpl getExpired() {
        return expired;
    }

    public CountStatisticImpl getConsumers() {
        return consumers;
    }

    public CountStatisticImpl getProducers() {
        return producers;
    }

    public PollCountStatisticImpl getMessagesCached() {
        return messagesCached;
    }

    public CountStatisticImpl getMessages() {
        return messages;
    }

    public CountStatisticImpl getDispatched() {
        return dispatched;
    }

    public CountStatisticImpl getDuplicateFromStore() {
        return duplicateFromStore;
    }

    public TimeStatisticImpl getProcessTime() {
        return this.processTime;
    }

    public CountStatisticImpl getBlockedSends(){
        return this.blockedSends;
    }
    public TimeStatisticImpl getBlockedTime(){
        return this.blockedTime;
    }
    public SizeStatisticImpl getMessageSize(){
        return this.messageSize;
    }

    public CountStatisticImpl getMaxUncommittedExceededCount(){
        return this.maxUncommittedExceededCount;
    }

    public CountStatisticImpl getNetworkEnqueues() {
        return networkEnqueues;
    }

    public CountStatisticImpl getNetworkDequeues() {
        return networkDequeues;
    }

    public MessageFlowStats getMessageFlowStats() {
        return messageFlowStats.get();
    }

    public void reset() {
        if (this.isDoReset()) {
            super.reset();
            enqueues.reset();
            dequeues.reset();
            forwards.reset();
            dispatched.reset();
            duplicateFromStore.reset();
            inflight.reset();
            expired.reset();
            blockedSends.reset();
            blockedTime.reset();
            messageSize.reset();
            maxUncommittedExceededCount.reset();
            networkEnqueues.reset();
            networkDequeues.reset();
            Optional.ofNullable(messageFlowStats.get())
                .ifPresent(MessageFlowStatsImpl::reset);
        }
    }

    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        enqueues.setEnabled(enabled);
        dispatched.setEnabled(enabled);
        dequeues.setEnabled(enabled);
        duplicateFromStore.setEnabled(enabled);
        forwards.setEnabled(enabled);
        inflight.setEnabled(enabled);
        expired.setEnabled(true);
        consumers.setEnabled(enabled);
        producers.setEnabled(enabled);
        messages.setEnabled(enabled);
        messagesCached.setEnabled(enabled);
        processTime.setEnabled(enabled);
        blockedSends.setEnabled(enabled);
        blockedTime.setEnabled(enabled);
        messageSize.setEnabled(enabled);
        maxUncommittedExceededCount.setEnabled(enabled);

        // [AMQ-9437] Advanced Network Statistics
        networkEnqueues.setEnabled(enabled);
        networkDequeues.setEnabled(enabled);

        // [AMQ-9437] Advanced Message Statistics
        Optional.ofNullable(messageFlowStats.get())
            .ifPresent(stats -> stats.setEnabled(enabled));
    }

    public void setParent(DestinationStatistics parent) {
        if (parent != null) {
            enqueues.setParent(parent.enqueues);
            dispatched.setParent(parent.dispatched);
            dequeues.setParent(parent.dequeues);
            duplicateFromStore.setParent(parent.duplicateFromStore);
            forwards.setParent(parent.forwards);
            inflight.setParent(parent.inflight);
            expired.setParent(parent.expired);
            consumers.setParent(parent.consumers);
            producers.setParent(parent.producers);
            messagesCached.setParent(parent.messagesCached);
            messages.setParent(parent.messages);
            processTime.setParent(parent.processTime);
            blockedSends.setParent(parent.blockedSends);
            blockedTime.setParent(parent.blockedTime);
            messageSize.setParent(parent.messageSize);
            maxUncommittedExceededCount.setParent(parent.maxUncommittedExceededCount);
            networkEnqueues.setParent(parent.networkEnqueues);
            networkDequeues.setParent(parent.networkDequeues);
            // [AMQ-9437] Advanced Message Statistics does not have a parent.
        } else {
            enqueues.setParent(null);
            dispatched.setParent(null);
            dequeues.setParent(null);
            duplicateFromStore.setParent(null);
            forwards.setParent(null);
            inflight.setParent(null);
            expired.setParent(null);
            consumers.setParent(null);
            producers.setParent(null);
            messagesCached.setParent(null);
            messages.setParent(null);
            processTime.setParent(null);
            blockedSends.setParent(null);
            blockedTime.setParent(null);
            messageSize.setParent(null);
            maxUncommittedExceededCount.setParent(null);
            networkEnqueues.setParent(null);
            networkDequeues.setParent(null);
            // [AMQ-9437] Advanced Message Statistics does not have a parent.
        }
    }

    // This is the only method that can mutate the messageFlowStats state
    public synchronized void setAdvancedMessageStatisticsEnabled(boolean enabled) {
        if(!enabled) {
            this.messageFlowStats.set(null);
            return;
        }

        if(this.messageFlowStats.get() == null) {
            MessageFlowStatsImpl tmpMessageFlowStatsImpl = new MessageFlowStatsImpl();
            tmpMessageFlowStatsImpl.setEnabled(true);
            this.messageFlowStats.set(tmpMessageFlowStatsImpl);
        }
    }

    public boolean isAdvancedMessageStatisticsEnabled() {
        return this.messageFlowStats.get() != null;
    }
}
