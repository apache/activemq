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
package org.apache.activemq;

public interface ScheduledMessage {
    /**
     * The time in milliseconds that a message will wait before being scheduled to be
     * delivered by the broker
     */
    public static final String AMQ_SCHEDULED_DELAY = "AMQ_SCHEDULED_DELAY";
    /**
     * The time in milliseconds to wait after the start time to wait before scheduling the message again
     */
    public static final String AMQ_SCHEDULED_PERIOD = "AMQ_SCHEDULED_PERIOD";
    /**
     * The number of times to repeat scheduling a message for delivery
     */
    public static final String AMQ_SCHEDULED_REPEAT = "AMQ_SCHEDULED_REPEAT";
    /**
     * Use a Cron tab entry to set the schedule
     */
    public static final String AMQ_SCHEDULED_CRON = "AMQ_SCHEDULED_CRON";
    /**
     * An Id that is assigned to a Scheduled Message, this value is only available once the
     * Message is scheduled, Messages sent to the Browse Destination or delivered to the
     * assigned Destination will have this value set.
     */
    public static final String AMQ_SCHEDULED_ID = "scheduledJobId";

    /**
     * Special destination to send Message's to with an assigned "action" that the Scheduler
     * should perform such as removing a message.
     */
    public static final String AMQ_SCHEDULER_MANAGEMENT_DESTINATION = "ActiveMQ.Scheduler.Management";
    /**
     * Used to specify that a some operation should be performed on the Scheduled Message,
     * the Message must have an assigned Id for this action to be taken.
     */
    public static final String AMQ_SCHEDULER_ACTION = "AMQ_SCHEDULER_ACTION";

    /**
     * Indicates that a browse of the Scheduled Messages is being requested.
     */
    public static final String AMQ_SCHEDULER_ACTION_BROWSE = "BROWSE";
    /**
     * Indicates that a Scheduled Message is to be remove from the Scheduler, the Id of
     * the scheduled message must be set as a property in order for this action to have
     * any effect.
     */
    public static final String AMQ_SCHEDULER_ACTION_REMOVE = "REMOVE";
    /**
     * Indicates that all scheduled Messages should be removed.
     */
    public static final String AMQ_SCHEDULER_ACTION_REMOVEALL = "REMOVEALL";

    /**
     * A property that holds the beginning of the time interval that the specified action should
     * be applied within.  Maps to a long value that specified time in milliseconds since UTC.
     */
    public static final String AMQ_SCHEDULER_ACTION_START_TIME = "ACTION_START_TIME";
    /**
     * A property that holds the end of the time interval that the specified action should be
     * applied within.  Maps to a long value that specified time in milliseconds since UTC.
     */
    public static final String AMQ_SCHEDULER_ACTION_END_TIME = "ACTION_END_TIME";

}
