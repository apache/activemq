/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
**/
package org.activemq.management;

import javax.jms.Message;

/**
 * Statistics for a {@link javax.jms.Queue}
 *
 * @version $Revision: 1.2 $
 */
public class JMSQueueStatsImpl extends JMSEndpointStatsImpl implements JMSDestinationStats {
    protected TimeStatisticImpl sendMessageRateTime;

    public JMSQueueStatsImpl() {
        this.sendMessageRateTime = new TimeStatisticImpl("sendMessageRateTime", "Time taken to send a message (publish thoughtput rate)");
        addStatistic("sendMessageRateTime", sendMessageRateTime);
    }

    public JMSQueueStatsImpl(CountStatisticImpl messageCount, CountStatisticImpl pendingMessageCount, CountStatisticImpl expiredMessageCount, TimeStatisticImpl messageWaitTime, TimeStatisticImpl messageRateTime, TimeStatisticImpl sendMessageRateTime) {
        super(messageCount, pendingMessageCount, expiredMessageCount, messageWaitTime, messageRateTime);
        this.sendMessageRateTime = sendMessageRateTime;
        addStatistic("sendMessageRateTime", sendMessageRateTime);
    }

    public void setPendingMessageCountOnStartup(long count) {
        CountStatisticImpl messageCount = (CountStatisticImpl) getPendingMessageCount();
        messageCount.setCount(count);
    }

    public void onMessageSend(Message message) {
        long start = pendingMessageCount.getLastSampleTime();
        pendingMessageCount.increment();
        long end = pendingMessageCount.getLastSampleTime();
        sendMessageRateTime.addTime(end - start);
    }

    public void onMessageAck() {
        long start = messageCount.getLastSampleTime();
        messageCount.increment();
        long end = messageCount.getLastSampleTime();
        messageRateTime.addTime(end - start);
    }
}
