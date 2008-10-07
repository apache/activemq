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
import org.apache.activemq.management.TimeStatisticImpl;
import org.apache.tools.ant.taskdefs.condition.IsReference;

/**
 * The J2EE Statistics for the a Destination.
 * 
 * @version $Revision$
 */
public class DestinationStatistics extends StatsImpl {

    protected CountStatisticImpl enqueues;
    protected CountStatisticImpl dequeues;
    protected CountStatisticImpl consumers;
    protected CountStatisticImpl producers;
    protected CountStatisticImpl messages;
    protected PollCountStatisticImpl messagesCached;
    protected CountStatisticImpl dispatched;
    protected CountStatisticImpl inflight;
    protected TimeStatisticImpl processTime;

    public DestinationStatistics() {

        enqueues = new CountStatisticImpl("enqueues", "The number of messages that have been sent to the destination");
        dispatched = new CountStatisticImpl("dispatched", "The number of messages that have been dispatched from the destination");
        dequeues = new CountStatisticImpl("dequeues", "The number of messages that have been acknowledged from the destination");
        inflight = new CountStatisticImpl("inflight", "The number of messages dispatched but awaiting acknowledgement");
        consumers = new CountStatisticImpl("consumers", "The number of consumers that that are subscribing to messages from the destination");
        consumers.setDoReset(false);
        producers = new CountStatisticImpl("producers", "The number of producers that that are publishing messages to the destination");
        producers.setDoReset(false);
        messages = new CountStatisticImpl("messages", "The number of messages that that are being held by the destination");
        messagesCached = new PollCountStatisticImpl("messagesCached", "The number of messages that are held in the destination's memory cache");
        processTime = new TimeStatisticImpl("processTime", "information around length of time messages are held by a destination");
        addStatistic("enqueues", enqueues);
        addStatistic("dispatched", dispatched);
        addStatistic("dequeues", dequeues);
        addStatistic("inflight", inflight);
        addStatistic("consumers", consumers);
        addStatistic("producers", producers);
        addStatistic("messages", messages);
        addStatistic("messagesCached", messagesCached);
        addStatistic("processTime", processTime);
    }

    public CountStatisticImpl getEnqueues() {
        return enqueues;
    }

    public CountStatisticImpl getDequeues() {
        return dequeues;
    }
    
    public CountStatisticImpl getInflight() {
        return inflight;
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

    public void reset() {
        if (this.isDoReset()) {
            super.reset();
            enqueues.reset();
            dequeues.reset();
            dispatched.reset();
            inflight.reset();
        }
    }

    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        enqueues.setEnabled(enabled);
        dispatched.setEnabled(enabled);
        dequeues.setEnabled(enabled);
        inflight.setEnabled(enabled);
        consumers.setEnabled(enabled);
        producers.setEnabled(enabled);
        messages.setEnabled(enabled);
        messagesCached.setEnabled(enabled);
        processTime.setEnabled(enabled);

    }

    public void setParent(DestinationStatistics parent) {
        if (parent != null) {
            enqueues.setParent(parent.enqueues);
            dispatched.setParent(parent.dispatched);
            dequeues.setParent(parent.dequeues);
            inflight.setParent(parent.inflight);
            consumers.setParent(parent.consumers);
            producers.setParent(parent.producers);
            messagesCached.setParent(parent.messagesCached);
            messages.setParent(parent.messages);
            processTime.setParent(parent.processTime);
        } else {
            enqueues.setParent(null);
            dispatched.setParent(null);
            dequeues.setParent(null);
            inflight.setParent(null);
            consumers.setParent(null);
            producers.setParent(null);
            messagesCached.setParent(null);
            messages.setParent(null);
            processTime.setParent(null);
        }
    }

}
