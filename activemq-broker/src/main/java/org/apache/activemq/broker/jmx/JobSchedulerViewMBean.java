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

import javax.management.openmbean.TabularData;

public interface JobSchedulerViewMBean {

    /**
     * Remove all jobs scheduled to run at this time.  If there are no jobs scheduled
     * at the given time this methods returns without making any modifications to the
     * scheduler store.
     *
     * @param time
     *        the string formated time that should be used to remove jobs.
     *
     * @throws Exception if an error occurs while performing the remove.
     */
    @MBeanInfo("remove jobs with matching execution time")
    public abstract void removeAllJobsAtScheduledTime(@MBeanInfo("time: yyyy-MM-dd hh:mm:ss")String time) throws Exception;

    /**
     * Remove a job with the matching jobId.  If the method does not find a matching job
     * then it returns without throwing an error or making any modifications to the job
     * scheduler store.
     *
     * @param jobId
     *        the Job Id to remove from the scheduler store.
     *
     * @throws Exception if an error occurs while attempting to remove the Job.
     */
    @MBeanInfo("remove jobs with matching jobId")
    public abstract void removeJob(@MBeanInfo("jobId")String jobId) throws Exception;

    /**
     * Remove all the Jobs from the scheduler,
     *
     * @throws Exception if an error occurs while purging the store.
     */
    @MBeanInfo("remove all scheduled jobs")
    public abstract void removeAllJobs() throws Exception;

    /**
     * Remove all the Jobs from the scheduler that are due between the start and finish times.
     *
     * @param start
     *        the starting time to remove jobs from.
     * @param finish
     *        the finish time for the remove operation.
     *
     * @throws Exception if an error occurs while attempting to remove the jobs.
     */
    @MBeanInfo("remove all scheduled jobs between time ranges ")
    public abstract void removeAllJobs(@MBeanInfo("start: yyyy-MM-dd hh:mm:ss")String start,@MBeanInfo("finish: yyyy-MM-dd hh:mm:ss")String finish) throws Exception;

    /**
     * Get the next time jobs will be fired from this scheduler store.
     *
     * @return the time in milliseconds of the next job to execute.
     *
     * @throws Exception if an error occurs while accessing the store.
     */
    @MBeanInfo("get the next time a job is due to be scheduled ")
    public abstract String getNextScheduleTime() throws Exception;

    /**
     * Gets the number of times a scheduled Job has been executed.
     *
     * @return the total number of time a scheduled job has executed.
     *
     * @throws Exception if an error occurs while querying for the Job.
     */
    @MBeanInfo("get the next time a job is due to be scheduled ")
    public abstract int getExecutionCount(@MBeanInfo("jobId")String jobId) throws Exception;

    /**
     * Get all the jobs scheduled to run next.
     *
     * @return a list of jobs that will be scheduled next
     *
     * @throws Exception if an error occurs while reading the scheduler store.
     */
    @MBeanInfo("get the next job(s) to be scheduled. Not HTML friendly ")
    public abstract TabularData getNextScheduleJobs() throws Exception;

    /**
     * Get all the outstanding Jobs that are scheduled in this scheduler store.
     *
     * @return a table of all jobs in this scheduler store.
     *
     * @throws Exception if an error occurs while reading the store.
     */
    @MBeanInfo("get the scheduled Jobs in the Store. Not HTML friendly ")
    public abstract TabularData getAllJobs() throws Exception;

    /**
     * Get all outstanding jobs due to run between start and finish time range.
     *
     * @param start
     *        the starting time range to query the store for jobs.
     * @param finish
     *        the ending time of this query for scheduled jobs.
     *
     * @return a table of jobs in the range given.
     *
     * @throws Exception if an error occurs while querying the scheduler store.
     */
    @MBeanInfo("get the scheduled Jobs in the Store within the time range. Not HTML friendly ")
    public abstract TabularData getAllJobs(@MBeanInfo("start: yyyy-MM-dd hh:mm:ss")String start,@MBeanInfo("finish: yyyy-MM-dd hh:mm:ss")String finish)throws Exception;

    /**
     * Get the number of messages in the scheduler.
     *
     * @return the number of messages in the scheduler.
     *
     * @throws Exception if an error occurs while querying the scheduler store.
     */
    @MBeanInfo("get the number of scheduled message (basically message in the scheduler")
    public abstract int getScheduledMessageCount() throws Exception;

    /**
     * Get the number of delayed messages.
     *
     * @return the number of delayed messages.
     *
     * @throws Exception if an error occurs while querying the scheduler store.
     */
    @MBeanInfo("get the number of delayed message")
    public abstract int getDelayedMessageCount() throws Exception;

}
