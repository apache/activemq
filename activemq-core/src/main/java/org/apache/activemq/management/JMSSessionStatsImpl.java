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

import java.util.List;

import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.util.IndentPrinter;

/**
 * Statistics for a JMS session
 * 
 * @version $Revision: 1.2 $
 */
public class JMSSessionStatsImpl extends StatsImpl {
    private List producers;
    private List consumers;
    private CountStatisticImpl messageCount;
    private CountStatisticImpl pendingMessageCount;
    private CountStatisticImpl expiredMessageCount;
    private TimeStatisticImpl messageWaitTime;
    private CountStatisticImpl durableSubscriptionCount;

    private TimeStatisticImpl messageRateTime;

    public JMSSessionStatsImpl(List producers, List consumers) {
        this.producers = producers;
        this.consumers = consumers;
        this.messageCount = new CountStatisticImpl("messageCount", "Number of messages exchanged");
        this.pendingMessageCount = new CountStatisticImpl("pendingMessageCount", "Number of pending messages");
        this.expiredMessageCount = new CountStatisticImpl("expiredMessageCount", "Number of expired messages");
        this.messageWaitTime = new TimeStatisticImpl("messageWaitTime", "Time spent by a message before being delivered");
        this.durableSubscriptionCount = new CountStatisticImpl("durableSubscriptionCount", "The number of durable subscriptions");
        this.messageWaitTime = new TimeStatisticImpl("messageWaitTime", "Time spent by a message before being delivered");
        this.messageRateTime = new TimeStatisticImpl("messageRateTime", "Time taken to process a message (thoughtput rate)");

        // lets add named stats
        addStatistic("messageCount", messageCount);
        addStatistic("pendingMessageCount", pendingMessageCount);
        addStatistic("expiredMessageCount", expiredMessageCount);
        addStatistic("messageWaitTime", messageWaitTime);
        addStatistic("durableSubscriptionCount", durableSubscriptionCount);
        addStatistic("messageRateTime", messageRateTime);
    }

    public JMSProducerStatsImpl[] getProducers() {
        // lets make a snapshot before we process them
        Object[] producerArray = producers.toArray();
        int size = producerArray.length;
        JMSProducerStatsImpl[] answer = new JMSProducerStatsImpl[size];
        for (int i = 0; i < size; i++) {
            ActiveMQMessageProducer producer = (ActiveMQMessageProducer)producerArray[i];
            answer[i] = producer.getProducerStats();
        }
        return answer;
    }

    public JMSConsumerStatsImpl[] getConsumers() {
        // lets make a snapshot before we process them
        Object[] consumerArray = consumers.toArray();
        int size = consumerArray.length;
        JMSConsumerStatsImpl[] answer = new JMSConsumerStatsImpl[size];
        for (int i = 0; i < size; i++) {
            ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer)consumerArray[i];
            answer[i] = consumer.getConsumerStats();
        }
        return answer;
    }

    public void reset() {
        super.reset();
        JMSConsumerStatsImpl[] cstats = getConsumers();
        for (int i = 0, size = cstats.length; i < size; i++) {
            cstats[i].reset();
        }
        JMSProducerStatsImpl[] pstats = getProducers();
        for (int i = 0, size = pstats.length; i < size; i++) {
            pstats[i].reset();
        }
    }

    /**
     * @param enabled the enabled to set
     */
    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        JMSConsumerStatsImpl[] cstats = getConsumers();
        for (int i = 0, size = cstats.length; i < size; i++) {
            cstats[i].setEnabled(enabled);
        }
        JMSProducerStatsImpl[] pstats = getProducers();
        for (int i = 0, size = pstats.length; i < size; i++) {
            pstats[i].setEnabled(enabled);
        }

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

    public TimeStatisticImpl getMessageWaitTime() {
        return messageWaitTime;
    }

    public CountStatisticImpl getDurableSubscriptionCount() {
        return durableSubscriptionCount;
    }

    public TimeStatisticImpl getMessageRateTime() {
        return messageRateTime;
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer(" ");
        buffer.append(messageCount);
        buffer.append(" ");
        buffer.append(messageRateTime);
        buffer.append(" ");
        buffer.append(pendingMessageCount);
        buffer.append(" ");
        buffer.append(expiredMessageCount);
        buffer.append(" ");
        buffer.append(messageWaitTime);
        buffer.append(" ");
        buffer.append(durableSubscriptionCount);

        buffer.append(" producers{ ");
        JMSProducerStatsImpl[] producerArray = getProducers();
        for (int i = 0; i < producerArray.length; i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append(Integer.toString(i));
            buffer.append(" = ");
            buffer.append(producerArray[i]);
        }
        buffer.append(" } consumers{ ");
        JMSConsumerStatsImpl[] consumerArray = getConsumers();
        for (int i = 0; i < consumerArray.length; i++) {
            if (i > 0) {
                buffer.append(", ");
            }
            buffer.append(Integer.toString(i));
            buffer.append(" = ");
            buffer.append(consumerArray[i]);
        }
        buffer.append(" }");
        return buffer.toString();
    }

    public void dump(IndentPrinter out) {
        out.printIndent();
        out.println(messageCount);
        out.printIndent();
        out.println(messageRateTime);
        out.printIndent();
        out.println(pendingMessageCount);
        out.printIndent();
        out.println(expiredMessageCount);
        out.printIndent();
        out.println(messageWaitTime);
        out.printIndent();
        out.println(durableSubscriptionCount);
        out.println();

        out.printIndent();
        out.println("producers {");
        out.incrementIndent();
        JMSProducerStatsImpl[] producerArray = getProducers();
        for (int i = 0; i < producerArray.length; i++) {
            JMSProducerStatsImpl producer = (JMSProducerStatsImpl)producerArray[i];
            producer.dump(out);
        }
        out.decrementIndent();
        out.printIndent();
        out.println("}");

        out.printIndent();
        out.println("consumers {");
        out.incrementIndent();
        JMSConsumerStatsImpl[] consumerArray = getConsumers();
        for (int i = 0; i < consumerArray.length; i++) {
            JMSConsumerStatsImpl consumer = (JMSConsumerStatsImpl)consumerArray[i];
            consumer.dump(out);
        }
        out.decrementIndent();
        out.printIndent();
        out.println("}");
    }

    public void onCreateDurableSubscriber() {
        durableSubscriptionCount.increment();
    }

    public void onRemoveDurableSubscriber() {
        durableSubscriptionCount.decrement();
    }
}
