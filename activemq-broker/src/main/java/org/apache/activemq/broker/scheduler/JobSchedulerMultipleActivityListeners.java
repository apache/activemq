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

import java.util.List;

import org.apache.activemq.ScheduledMessage;

import org.apache.activemq.command.Message;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * Job Scheduler Activity listener that forwards scheduled and dispatched messages to other listeners
 */
public class JobSchedulerMultipleActivityListeners implements JobSchedulerActivityListener {

    private List <JobSchedulerActivityListener> listeners = null;

    @Override
    public void scheduled(SchedulerBroker schedulerBroker, final Message messageSend) throws Exception {
		for(JobSchedulerActivityListener listener : listeners) {
			listener.scheduled(schedulerBroker, messageSend);
		}
    }

    @Override
    public void dispatched(SchedulerBroker schedulerBroker, final Message messageSend) throws Exception {
		for(JobSchedulerActivityListener listener : listeners) {
			listener.dispatched(schedulerBroker, messageSend);
		}
    }

    /**
    * @param listeners the listeners to set
    */
    public void setListeners(List<JobSchedulerActivityListener> listeners) {
        this.listeners = listeners;
    }

    /**
     * @return the listeners
     */
    public List<JobSchedulerActivityListener> getListeners() {
        return listeners;
    }

	public String toString() {
		return getClass().getName()+" "+listeners;
	}
}
