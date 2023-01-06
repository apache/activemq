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
package org.apache.activemq.broker.scheduler;

import org.apache.activemq.ScheduledMessage;

import org.apache.activemq.command.Message;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * Job Scheduler Activity listener that forwards scheduled and dispatched messages to a specified destination
 */
public class JobSchedulerForwardingActivityListener implements JobSchedulerActivityListener {

    private ActiveMQDestination destination = ActiveMQDestination.createDestination(ScheduledMessage.AMQ_SCHEDULER_ACTIVITY_DESTINATION, ActiveMQDestination.TOPIC_TYPE);

    private JobSchedulerForwardingActivityListenerMessageFormat format = new JobSchedulerForwardingActivityListenerAdvisoryMessageFormat();

    @Override
    public void scheduled(SchedulerBroker schedulerBroker, final Message messageSend) throws Exception {
        Message msg = format.format(messageSend);
        schedulerBroker.forwardMessage(schedulerBroker.getConnectionContext(), msg, destination);
    }

    @Override
    public void dispatched(SchedulerBroker schedulerBroker, final Message messageSend) throws Exception {
        Message msg = format.format(messageSend);
        schedulerBroker.forwardMessage(schedulerBroker.getConnectionContext(), msg, destination);
    }

    /**
    * @param destination the destination to set
    */
    public void setSchedulerActivityDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    /**
     * @return the destination
     */
    public ActiveMQDestination getSchedulerActivityDestination() {
        return destination;
    }

    /**
    * @param format the format to set
    */
    public void setFormat(JobSchedulerForwardingActivityListenerMessageFormat format) {
        this.format = format;
    }

    /**
     * @return the format
     */
    public JobSchedulerForwardingActivityListenerMessageFormat getFormat() {
        return format;
    }

	public String toString() {
		return getClass().getName()+" "+destination+" "+format;
	}
}
