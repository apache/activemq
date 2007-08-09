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

import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.util.IndentPrinter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Statistics for a JMS endpoint, typically a MessageProducer or MessageConsumer
 * but this class can also be used to represent statistics on a
 * {@link Destination} as well.
 * 
 * @version $Revision: 1.3 $
 */
public class JMSEndpointStatsImpl extends StatsImpl {
    private static final Log LOG = LogFactory.getLog(JMSEndpointStatsImpl.class);

    protected CountStatisticImpl messageCount;
    protected CountStatisticImpl pendingMessageCount;
    protected CountStatisticImpl expiredMessageCount;
    protected TimeStatisticImpl messageWaitTime;
    protected TimeStatisticImpl messageRateTime;

    /**
     * This constructor is used to create statistics for a
     * {@link MessageProducer} or {@link MessageConsumer} as it passes in a
     * {@link Session} parent statistic.
     * 
     * @param sessionStats
     */
    public JMSEndpointStatsImpl(JMSSessionStatsImpl sessionStats) {
        this();
        setParent(messageCount, sessionStats.getMessageCount());
        setParent(pendingMessageCount, sessionStats.getPendingMessageCount());
        setParent(expiredMessageCount, sessionStats.getExpiredMessageCount());
        setParent(messageWaitTime, sessionStats.getMessageWaitTime());
        setParent(messageRateTime, sessionStats.getMessageRateTime());
    }

    /**
     * This constructor is typically used to create a statistics object for a
     * {@link Destination}
     */
    public JMSEndpointStatsImpl() {
        this(new CountStatisticImpl("messageCount", "Number of messages processed"), new CountStatisticImpl("pendingMessageCount", "Number of pending messages"),
             new CountStatisticImpl("expiredMessageCount", "Number of expired messages"),
             new TimeStatisticImpl("messageWaitTime", "Time spent by a message before being delivered"), new TimeStatisticImpl("messageRateTime",
                                                                                                                               "Time taken to process a message (thoughtput rate)"));
    }

    public JMSEndpointStatsImpl(CountStatisticImpl messageCount, CountStatisticImpl pendingMessageCount, CountStatisticImpl expiredMessageCount, TimeStatisticImpl messageWaitTime,
                                TimeStatisticImpl messageRateTime) {
        this.messageCount = messageCount;
        this.pendingMessageCount = pendingMessageCount;
        this.expiredMessageCount = expiredMessageCount;
        this.messageWaitTime = messageWaitTime;
        this.messageRateTime = messageRateTime;

        // lets add named stats
        addStatistic("messageCount", messageCount);
        addStatistic("pendingMessageCount", pendingMessageCount);
        addStatistic("expiredMessageCount", expiredMessageCount);
        addStatistic("messageWaitTime", messageWaitTime);
        addStatistic("messageRateTime", messageRateTime);
    }

    public synchronized void reset() {
        super.reset();
        messageCount.reset();
        messageRateTime.reset();
        pendingMessageCount.reset();
        expiredMessageCount.reset();
        messageWaitTime.reset();
    }

    public CountStatisticImpl getMessageCount() {
        return messageCount;
    }

    public CountStatisticImpl getPendingMessageCount() {
        return pendingMessageCount;
    }

    public CountStatisticImpl getExpiredMessageCount() {
        return expiredMessageCount;
    }

    public TimeStatisticImpl getMessageRateTime() {
        return messageRateTime;
    }

    public TimeStatisticImpl getMessageWaitTime() {
        return messageWaitTime;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append(messageCount);
        buffer.append(" ");
        buffer.append(messageRateTime);
        buffer.append(" ");
        buffer.append(pendingMessageCount);
        buffer.append(" ");
        buffer.append(expiredMessageCount);
        buffer.append(" ");
        buffer.append(messageWaitTime);
        return buffer.toString();
    }

    public void onMessage() {
        if (enabled) {
            long start = messageCount.getLastSampleTime();
            messageCount.increment();
            long end = messageCount.getLastSampleTime();
            messageRateTime.addTime(end - start);
        }
    }

    public void dump(IndentPrinter out) {
        out.printIndent();
        out.println(messageCount);
        out.printIndent();
        out.println(messageRateTime);
        out.printIndent();
        out.println(pendingMessageCount);
        out.printIndent();
        out.println(messageRateTime);
        out.printIndent();
        out.println(expiredMessageCount);
        out.printIndent();
        out.println(messageWaitTime);
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void setParent(CountStatisticImpl child, CountStatisticImpl parent) {
        if (child instanceof CountStatisticImpl && parent instanceof CountStatisticImpl) {
            CountStatisticImpl c = (CountStatisticImpl)child;
            c.setParent((CountStatisticImpl)parent);
        } else {
            LOG.warn("Cannot associate endpoint counters with session level counters as they are not both CountStatisticImpl clases. Endpoint: " + child + " session: " + parent);
        }
    }

    protected void setParent(TimeStatisticImpl child, TimeStatisticImpl parent) {
        if (child instanceof TimeStatisticImpl && parent instanceof TimeStatisticImpl) {
            TimeStatisticImpl c = (TimeStatisticImpl)child;
            c.setParent((TimeStatisticImpl)parent);
        } else {
            LOG.warn("Cannot associate endpoint counters with session level counters as they are not both TimeStatisticImpl clases. Endpoint: " + child + " session: " + parent);
        }
    }
}
