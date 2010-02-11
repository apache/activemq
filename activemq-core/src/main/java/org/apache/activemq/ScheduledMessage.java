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
    

}
