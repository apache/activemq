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
package org.apache.activemq.broker.jmx;

import java.io.IOException;
import java.util.List;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import org.apache.activemq.broker.jmx.OpenTypeSupport.OpenTypeFactory;
import org.apache.activemq.broker.scheduler.Job;
import org.apache.activemq.broker.scheduler.JobImpl;
import org.apache.activemq.broker.scheduler.JobScheduler;

public class JobSchedulerView implements JobSchedulerViewMBean {

    private final JobScheduler jobScheduler;

    public JobSchedulerView(JobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
    }

    public TabularData getAllJobs() throws Exception {
        OpenTypeFactory factory = OpenTypeSupport.getFactory(Job.class);
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("Scheduled Jobs", "Scheduled Jobs", ct, new String[] { "jobId" });
        TabularDataSupport rc = new TabularDataSupport(tt);
        List<Job> jobs = this.jobScheduler.getAllJobs();
        for (Job job : jobs) {
            rc.put(new CompositeDataSupport(ct, factory.getFields(job)));
        }
        return rc;
    }

    public TabularData getAllJobs(String startTime, String finishTime) throws Exception {
        OpenTypeFactory factory = OpenTypeSupport.getFactory(Job.class);
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("Scheduled Jobs", "Scheduled Jobs", ct, new String[] { "jobId" });
        TabularDataSupport rc = new TabularDataSupport(tt);
        long start = JobImpl.getDataTime(startTime);
        long finish = JobImpl.getDataTime(finishTime);
        List<Job> jobs = this.jobScheduler.getAllJobs(start, finish);
        for (Job job : jobs) {
            rc.put(new CompositeDataSupport(ct, factory.getFields(job)));
        }
        return rc;
    }

    public TabularData getNextScheduleJobs() throws Exception {
        OpenTypeFactory factory = OpenTypeSupport.getFactory(Job.class);
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("Scheduled Jobs", "Scheduled Jobs", ct, new String[] { "jobId" });
        TabularDataSupport rc = new TabularDataSupport(tt);
        List<Job> jobs = this.jobScheduler.getNextScheduleJobs();
        for (Job job : jobs) {
            rc.put(new CompositeDataSupport(ct, factory.getFields(job)));
        }
        return rc;
    }

    public String getNextScheduleTime() throws Exception {
        long time = this.jobScheduler.getNextScheduleTime();
        return JobImpl.getDateTime(time);
    }

    public void removeAllJobs() throws Exception {
        this.jobScheduler.removeAllJobs();

    }

    public void removeAllJobs(String startTime, String finishTime) throws Exception {
        long start = JobImpl.getDataTime(startTime);
        long finish = JobImpl.getDataTime(finishTime);
        this.jobScheduler.removeAllJobs(start, finish);

    }

    public void removeJob(String jobId) throws Exception {
        this.jobScheduler.remove(jobId);

    }

    public void removeJobAtScheduledTime(String time) throws IOException {
        // TODO Auto-generated method stub

    }

}
