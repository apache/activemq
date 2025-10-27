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
package org.apache.activemq.web.controller;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.activemq.broker.jmx.JobSchedulerViewMBean;
import org.apache.activemq.web.BrokerFacade;
import org.apache.activemq.web.DestinationFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

@Component
@RequestScope
public class DeleteJob extends DestinationFacade implements Controller {
    private String jobId;
    private static final Logger LOG = LoggerFactory.getLogger(DeleteJob.class);

    public DeleteJob(final BrokerFacade brokerFacade) {
        super(brokerFacade);
    }

    public void handleRequest(final HttpServletRequest request, final HttpServletResponse response) throws Exception {
        if (jobId != null) {
            JobSchedulerViewMBean jobScheduler = getBrokerFacade().getJobScheduler();
            if (jobScheduler != null) {
                jobScheduler.removeJob(jobId);
                LOG.info("Removed scheduled Job {}", jobId);
            } else {
            	LOG.warn("Scheduler not configured");
            }
        }
        response.sendRedirect("scheduled.jsp");
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(final String id) {
        this.jobId = id;
    }
}
