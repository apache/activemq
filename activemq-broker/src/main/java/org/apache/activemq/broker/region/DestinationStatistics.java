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
//IC see: https://issues.apache.org/jira/browse/AMQ-1023
        dispatched = new CountStatisticImpl("dispatched", "The number of messages that have been dispatched from the destination");
        dequeues = new CountStatisticImpl("dequeues", "The number of messages that have been acknowledged from the destination");
//IC see: https://issues.apache.org/jira/browse/AMQ-5289
        forwards = new CountStatisticImpl("forwards", "The number of messages that have been forwarded to a networked broker from the destination");
//IC see: https://issues.apache.org/jira/browse/AMQ-1560
        inflight = new CountStatisticImpl("inflight", "The number of messages dispatched but awaiting acknowledgement");
        expired = new CountStatisticImpl("expired", "The number of messages that have expired");
//IC see: https://issues.apache.org/jira/browse/AMQ-1112

        consumers = new CountStatisticImpl("consumers", "The number of consumers that that are subscribing to messages from the destination");
//IC see: https://issues.apache.org/jira/browse/AMQ-1946
        consumers.setDoReset(false);
        producers = new CountStatisticImpl("producers", "The number of producers that that are publishing messages to the destination");
        producers.setDoReset(false);
        messages = new CountStatisticImpl("messages", "The number of messages that that are being held by the destination");
//IC see: https://issues.apache.org/jira/browse/AMQ-2971
        messages.setDoReset(false);
        messagesCached = new PollCountStatisticImpl("messagesCached", "The number of messages that are held in the destination's memory cache");
        processTime = new TimeStatisticImpl("processTime", "information around length of time messages are held by a destination");
//IC see: https://issues.apache.org/jira/browse/AMQ-4635
        blockedSends = new CountStatisticImpl("blockedSends", "number of messages that have to wait for flow control");
        blockedTime = new TimeStatisticImpl("blockedTime","amount of time messages are blocked for flow control");
//IC see: https://issues.apache.org/jira/browse/AMQ-4697
        messageSize = new SizeStatisticImpl("messageSize","Size of messages passing through the destination");
        addStatistic("enqueues", enqueues);
        addStatistic("dispatched", dispatched);
        addStatistic("dequeues", dequeues);
//IC see: https://issues.apache.org/jira/browse/AMQ-1560
        addStatistic("inflight", inflight);
//IC see: https://issues.apache.org/jira/browse/AMQ-1112
        addStatistic("expired", expired);
        addStatistic("consumers", consumers);
//IC see: https://issues.apache.org/jira/browse/AMQ-1946
        addStatistic("producers", producers);
        addStatistic("messages", messages);
        addStatistic("messagesCached", messagesCached);
        addStatistic("processTime", processTime);
//IC see: https://issues.apache.org/jira/browse/AMQ-4635
        addStatistic("blockedSends",blockedSends);
        addStatistic("blockedTime",blockedTime);
//IC see: https://issues.apache.org/jira/browse/AMQ-4697
        addStatistic("messageSize",messageSize);
    }

    public CountStatisticImpl getEnqueues() {
        return enqueues;
    }

    public CountStatisticImpl getDequeues() {
        return dequeues;
    }

    public CountStatisticImpl getForwards() {
//IC see: https://issues.apache.org/jira/browse/AMQ-5289
        return forwards;
    }

    public CountStatisticImpl getInflight() {
//IC see: https://issues.apache.org/jira/browse/AMQ-1560
        return inflight;
    }

    public CountStatisticImpl getExpired() {
//IC see: https://issues.apache.org/jira/browse/AMQ-1112
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
//IC see: https://issues.apache.org/jira/browse/AMQ-567
        this.messagesCached = messagesCached;
    }

    public CountStatisticImpl getDispatched() {
//IC see: https://issues.apache.org/jira/browse/AMQ-1023
        return dispatched;
    }

    public TimeStatisticImpl getProcessTime() {
        return this.processTime;
    }

    public CountStatisticImpl getBlockedSends(){
//IC see: https://issues.apache.org/jira/browse/AMQ-4635
        return this.blockedSends;
    }
    public TimeStatisticImpl getBlockedTime(){
        return this.blockedTime;
    }
    public SizeStatisticImpl getMessageSize(){
//IC see: https://issues.apache.org/jira/browse/AMQ-4697
        return this.messageSize;
    }

    public void reset() {
//IC see: https://issues.apache.org/jira/browse/AMQ-1946
        if (this.isDoReset()) {
            super.reset();
            enqueues.reset();
            dequeues.reset();
//IC see: https://issues.apache.org/jira/browse/AMQ-5289
            forwards.reset();
//IC see: https://issues.apache.org/jira/browse/AMQ-1023
            dispatched.reset();
//IC see: https://issues.apache.org/jira/browse/AMQ-1560
            inflight.reset();
//IC see: https://issues.apache.org/jira/browse/AMQ-1112
            expired.reset();
//IC see: https://issues.apache.org/jira/browse/AMQ-4635
            blockedSends.reset();
            blockedTime.reset();
//IC see: https://issues.apache.org/jira/browse/AMQ-4697
            messageSize.reset();
        }
    }

    public void setEnabled(boolean enabled) {
//IC see: https://issues.apache.org/jira/browse/AMQ-894
        super.setEnabled(enabled);
        enqueues.setEnabled(enabled);
        dispatched.setEnabled(enabled);
        dequeues.setEnabled(enabled);
//IC see: https://issues.apache.org/jira/browse/AMQ-5289
        forwards.setEnabled(enabled);
//IC see: https://issues.apache.org/jira/browse/AMQ-1560
        inflight.setEnabled(enabled);
//IC see: https://issues.apache.org/jira/browse/AMQ-1112
        expired.setEnabled(true);
        consumers.setEnabled(enabled);
        producers.setEnabled(enabled);
        messages.setEnabled(enabled);
        messagesCached.setEnabled(enabled);
//IC see: https://issues.apache.org/jira/browse/AMQ-567
        processTime.setEnabled(enabled);
//IC see: https://issues.apache.org/jira/browse/AMQ-4635
        blockedSends.setEnabled(enabled);
        blockedTime.setEnabled(enabled);
        messageSize.setEnabled(enabled);
//IC see: https://issues.apache.org/jira/browse/AMQ-4697

    }

    public void setParent(DestinationStatistics parent) {
        if (parent != null) {
            enqueues.setParent(parent.enqueues);
            dispatched.setParent(parent.dispatched);
            dequeues.setParent(parent.dequeues);
//IC see: https://issues.apache.org/jira/browse/AMQ-5289
            forwards.setParent(parent.forwards);
//IC see: https://issues.apache.org/jira/browse/AMQ-1560
            inflight.setParent(parent.inflight);
//IC see: https://issues.apache.org/jira/browse/AMQ-1112
            expired.setParent(parent.expired);
            consumers.setParent(parent.consumers);
            producers.setParent(parent.producers);
            messagesCached.setParent(parent.messagesCached);
            messages.setParent(parent.messages);
//IC see: https://issues.apache.org/jira/browse/AMQ-567
            processTime.setParent(parent.processTime);
//IC see: https://issues.apache.org/jira/browse/AMQ-4635
            blockedSends.setParent(parent.blockedSends);
            blockedTime.setParent(parent.blockedTime);
//IC see: https://issues.apache.org/jira/browse/AMQ-4697
            messageSize.setParent(parent.messageSize);
        } else {
            enqueues.setParent(null);
//IC see: https://issues.apache.org/jira/browse/AMQ-1023
            dispatched.setParent(null);
            dequeues.setParent(null);
//IC see: https://issues.apache.org/jira/browse/AMQ-5289
            forwards.setParent(null);
//IC see: https://issues.apache.org/jira/browse/AMQ-1560
            inflight.setParent(null);
//IC see: https://issues.apache.org/jira/browse/AMQ-1112
            expired.setParent(null);
            consumers.setParent(null);
            producers.setParent(null);
            messagesCached.setParent(null);
            messages.setParent(null);
//IC see: https://issues.apache.org/jira/browse/AMQ-567
            processTime.setParent(null);
//IC see: https://issues.apache.org/jira/browse/AMQ-4635
            blockedSends.setParent(null);
            blockedTime.setParent(null);
//IC see: https://issues.apache.org/jira/browse/AMQ-4697
            messageSize.setParent(null);
        }
    }

}
