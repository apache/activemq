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

import java.util.Arrays;
import java.util.List;

import org.apache.activemq.ScheduledMessage;

/**
 * Job Scheduler Forwarding Activity Listener Message Format that strips the body of the forwarded message 
 */
public class JobSchedulerForwardingActivityListenerAdvisoryMessageFormat extends JobSchedulerForwardingActivityListenerMetadataOnlyMessageFormat {

    private static final List<String> ADVISORY_PROPERTIES = Arrays.asList(ScheduledMessage.AMQ_SCHEDULED_ID, ScheduledMessage.AMQ_SCHEDULER_ACTIVITY);

    public JobSchedulerForwardingActivityListenerAdvisoryMessageFormat() {
        setIncludes(ADVISORY_PROPERTIES);
    }

    public String toString() {
        return getClass().getName()+" "+ADVISORY_PROPERTIES;
    }
}
