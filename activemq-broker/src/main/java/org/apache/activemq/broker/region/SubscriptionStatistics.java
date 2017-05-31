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
import org.apache.activemq.management.SizeStatisticImpl;
import org.apache.activemq.management.StatsImpl;

/**
 * The J2EE Statistics for a Subsription.
 */
public class SubscriptionStatistics extends StatsImpl {

    protected CountStatisticImpl consumedCount;
    protected CountStatisticImpl enqueues;
    protected CountStatisticImpl dequeues;
    protected CountStatisticImpl dispatched;
    protected SizeStatisticImpl inflightMessageSize;


    public SubscriptionStatistics() {
        this(true);
    }

    public SubscriptionStatistics(boolean enabled) {

        consumedCount = new CountStatisticImpl("consumedCount", "The number of messages that have been consumed by the subscription");
        enqueues = new CountStatisticImpl("enqueues", "The number of messages that have been sent to the subscription");
        dispatched = new CountStatisticImpl("dispatched", "The number of messages that have been dispatched from the subscription");
        dequeues = new CountStatisticImpl("dequeues", "The number of messages that have been acknowledged from the subscription");
        inflightMessageSize = new SizeStatisticImpl("inflightMessageSize", "The size in bytes of messages dispatched but awaiting acknowledgement");

        addStatistic("consumedCount", consumedCount);
        addStatistic("enqueues", enqueues);
        addStatistic("dispatched", dispatched);
        addStatistic("dequeues", dequeues);
        addStatistic("inflightMessageSize", inflightMessageSize);

        this.setEnabled(enabled);
    }

    public CountStatisticImpl getConsumedCount() {
        return consumedCount;
    }

    public CountStatisticImpl getEnqueues() {
        return enqueues;
    }

    public CountStatisticImpl getDequeues() {
        return dequeues;
    }

    public CountStatisticImpl getDispatched() {
        return dispatched;
    }

    public SizeStatisticImpl getInflightMessageSize() {
        return inflightMessageSize;
    }

    public void reset() {
        if (this.isDoReset()) {
            super.reset();
            consumedCount.reset();
            enqueues.reset();
            dequeues.reset();
            dispatched.reset();
            inflightMessageSize.reset();
        }
    }

    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        consumedCount.setEnabled(enabled);
        enqueues.setEnabled(enabled);
        dispatched.setEnabled(enabled);
        dequeues.setEnabled(enabled);
        inflightMessageSize.setEnabled(enabled);
    }

    public void setParent(SubscriptionStatistics parent) {
        if (parent != null) {
            consumedCount.setParent(parent.consumedCount);
            enqueues.setParent(parent.enqueues);
            dispatched.setParent(parent.dispatched);
            dequeues.setParent(parent.dequeues);
            inflightMessageSize.setParent(parent.inflightMessageSize);
        } else {
            consumedCount.setParent(null);
            enqueues.setParent(null);
            dispatched.setParent(null);
            dequeues.setParent(null);
            inflightMessageSize.setParent(null);
        }
    }

}
