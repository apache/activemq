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

import org.apache.activemq.management.CountStatisticImpl;
import org.apache.activemq.management.PollCountStatisticImpl;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.management.*;

/**
 * The J2EE Statistics for the a Destination.
 *
 *
 */
public class DestinationStatistics extends StatsImpl {

    protected CountStatisticImpl enqueues;
    protected CountStatisticImpl dequeues;
    protected CountStatisticImpl forwards;
    protected CountStatisticImpl consumers;
    protected CountStatisticImpl producers;
    protected CountStatisticImpl messages;
    protected PollCountStatisticImpl messagesCached;
    protected CountStatisticImpl dispatched;
    protected CountStatisticImpl inflight;
    protected CountStatisticImpl expired;
    protected TimeStatisticImpl processTime;
    protected CountStatisticImpl blockedSends;
    protected TimeStatisticImpl blockedTime;
    protected SizeStatisticImpl messageSize;


    public DestinationStatistics() {

        enqueues = new CountStatisticImpl("enqueues", "The number of messages that have been sent to the destination");
        dispatched = new CountStatisticImpl("dispatched", "The number of messages that have been dispatched from the destination");
        dequeues = new CountStatisticImpl("dequeues", "The number of messages that have been acknowledged from the destination");
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
        addStatistic("enqueues", enqueues);
        addStatistic("dispatched", dispatched);
        addStatistic("dequeues", dequeues);
        addStatistic("inflight", inflight);
        addStatistic("expired", expired);
        addStatistic("consumers", consumers);
        addStatistic("producers", producers);
        addStatistic("messages", messages);
        addStatistic("messagesCached", messagesCached);
        addStatistic("processTime", processTime);
        addStatistic("blockedSends",blockedSends);
        addStatistic("blockedTime",blockedTime);
        addStatistic("messageSize",messageSize);
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

    public void setMessagesCached(PollCountStatisticImpl messagesCached) {
        this.messagesCached = messagesCached;
    }

    public CountStatisticImpl getDispatched() {
        return dispatched;
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

    public void reset() {
        if (this.isDoReset()) {
            super.reset();
            enqueues.reset();
            dequeues.reset();
            forwards.reset();
            dispatched.reset();
            inflight.reset();
            expired.reset();
            blockedSends.reset();
            blockedTime.reset();
            messageSize.reset();
        }
    }

    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        enqueues.setEnabled(enabled);
        dispatched.setEnabled(enabled);
        dequeues.setEnabled(enabled);
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

    }

    public void setParent(DestinationStatistics parent) {
        if (parent != null) {
            enqueues.setParent(parent.enqueues);
            dispatched.setParent(parent.dispatched);
            dequeues.setParent(parent.dequeues);
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
        } else {
            enqueues.setParent(null);
            dispatched.setParent(null);
            dequeues.setParent(null);
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
        }
    }

}
