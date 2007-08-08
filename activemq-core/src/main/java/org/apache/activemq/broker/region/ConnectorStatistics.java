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

/**
 * The J2EE Statistics for the a Destination.
 * 
 * @version $Revision$
 */
public class ConnectorStatistics extends StatsImpl {

    protected CountStatisticImpl enqueues;
    protected CountStatisticImpl dequeues;
    protected CountStatisticImpl consumers;
    protected CountStatisticImpl messages;
    protected PollCountStatisticImpl messagesCached;

    public ConnectorStatistics() {

        enqueues = new CountStatisticImpl("enqueues", "The number of messages that have been sent to the destination");
        dequeues = new CountStatisticImpl("dequeues", "The number of messages that have been dispatched from the destination");
        consumers = new CountStatisticImpl("consumers", "The number of consumers that that are subscribing to messages from the destination");
        messages = new CountStatisticImpl("messages", "The number of messages that that are being held by the destination");
        messagesCached = new PollCountStatisticImpl("messagesCached", "The number of messages that are held in the destination's memory cache");

        addStatistic("enqueues", enqueues);
        addStatistic("dequeues", dequeues);
        addStatistic("consumers", consumers);
        addStatistic("messages", messages);
        addStatistic("messagesCached", messagesCached);
    }

    public CountStatisticImpl getEnqueues() {
        return enqueues;
    }

    public CountStatisticImpl getDequeues() {
        return dequeues;
    }

    public CountStatisticImpl getConsumers() {
        return consumers;
    }

    public PollCountStatisticImpl getMessagesCached() {
        return messagesCached;
    }

    public CountStatisticImpl getMessages() {
        return messages;
    }

    public void reset() {
        super.reset();
        enqueues.reset();
        dequeues.reset();
    }

    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        enqueues.setEnabled(enabled);
        dequeues.setEnabled(enabled);
        consumers.setEnabled(enabled);
        messages.setEnabled(enabled);
        messagesCached.setEnabled(enabled);
    }

    public void setParent(ConnectorStatistics parent) {
        if (parent != null) {
            enqueues.setParent(parent.enqueues);
            dequeues.setParent(parent.dequeues);
            consumers.setParent(parent.consumers);
            messagesCached.setParent(parent.messagesCached);
            messages.setParent(parent.messages);
        } else {
            enqueues.setParent(null);
            dequeues.setParent(null);
            consumers.setParent(null);
            messagesCached.setParent(null);
            messages.setParent(null);
        }
    }

    public void setMessagesCached(PollCountStatisticImpl messagesCached) {
        this.messagesCached = messagesCached;
    }
}
