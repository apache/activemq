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

import java.util.List;

import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.activemq.Message;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.jmx.OpenTypeSupport.OpenTypeFactory;
import org.apache.activemq.broker.scheduler.Job;
import org.apache.activemq.broker.scheduler.JobScheduler;
import org.apache.activemq.broker.scheduler.JobSupport;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;

/**
 * MBean object that can be used to manage a single instance of a JobScheduler.  The object
 * provides methods for querying for jobs and removing some or all of the jobs that are
 * scheduled in the managed store.
 */
public class JobSchedulerView implements JobSchedulerViewMBean {

    private final JobScheduler jobScheduler;

    /**
     * Creates a new instance of the JobScheduler management MBean.
     *
     * @param jobScheduler
     *        The scheduler instance to manage.
     */
    public JobSchedulerView(JobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
    }

    @Override
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

    @Override
    public TabularData getAllJobs(String startTime, String finishTime) throws Exception {
        OpenTypeFactory factory = OpenTypeSupport.getFactory(Job.class);
        CompositeType ct = factory.getCompositeType();
        TabularType tt = new TabularType("Scheduled Jobs", "Scheduled Jobs", ct, new String[] { "jobId" });
        TabularDataSupport rc = new TabularDataSupport(tt);
        long start = JobSupport.getDataTime(startTime);
        long finish = JobSupport.getDataTime(finishTime);
        List<Job> jobs = this.jobScheduler.getAllJobs(start, finish);
        for (Job job : jobs) {
            rc.put(new CompositeDataSupport(ct, factory.getFields(job)));
        }
        return rc;
    }

    @Override
    public int getDelayedMessageCount() throws Exception {
        int counter = 0;
        OpenWireFormat wireFormat = new OpenWireFormat();
        for (Job job : jobScheduler.getAllJobs()) {
            Message msg = (Message) wireFormat.unmarshal(new ByteSequence(job.getPayload()));
            if (msg.getLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY) > 0) {
                counter++;
            }
        }
        return counter;
    }

    @Override
    public int getScheduledMessageCount() throws Exception {
        return this.jobScheduler.getAllJobs().size();
    }

    @Override
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

    @Override
    public String getNextScheduleTime() throws Exception {
        long time = this.jobScheduler.getNextScheduleTime();
        return JobSupport.getDateTime(time);
    }

    @Override
    public void removeAllJobs() throws Exception {
        this.jobScheduler.removeAllJobs();
    }

    @Override
    public void removeAllJobs(String startTime, String finishTime) throws Exception {
        long start = JobSupport.getDataTime(startTime);
        long finish = JobSupport.getDataTime(finishTime);
        this.jobScheduler.removeAllJobs(start, finish);
    }

    @Override
    public void removeAllJobsAtScheduledTime(String time) throws Exception {
        long removeAtTime = JobSupport.getDataTime(time);
        this.jobScheduler.remove(removeAtTime);
    }

    @Override
    public void removeJob(String jobId) throws Exception {
        this.jobScheduler.remove(jobId);
    }

    @Override
    public int getExecutionCount(String jobId) throws Exception {
        int result = 0;

        List<Job> jobs = this.jobScheduler.getAllJobs();
        for (Job job : jobs) {
            if (job.getJobId().equals(jobId)) {
                result = job.getExecutionCount();
            }
        }

        return result;
    }
}
