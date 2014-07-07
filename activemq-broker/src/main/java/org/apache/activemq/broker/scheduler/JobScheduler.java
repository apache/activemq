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

import org.apache.activemq.util.ByteSequence;

public interface JobScheduler {

    /**
     * @return the name of the scheduler
     * @throws Exception
     */
    String getName() throws Exception;

    /**
     * Starts dispatch of scheduled Jobs to registered listeners.
     *
     * Any listener added after the start dispatch method can miss jobs so its
     * important to register critical listeners before the start of job dispatching.
     *
     * @throws Exception
     */
    void startDispatching() throws Exception;

    /**
     * Stops dispatching of scheduled Jobs to registered listeners.
     *
     * @throws Exception
     */
    void stopDispatching() throws Exception;

    /**
     * Add a Job listener which will receive events related to scheduled jobs.
     *
     * @param listener
     *      The job listener to add.
     *
     * @throws Exception
     */
    void addListener(JobListener listener) throws Exception;

    /**
     * remove a JobListener that was previously registered.  If the given listener is not in
     * the registry this method has no effect.
     *
     * @param listener
     *      The listener that should be removed from the listener registry.
     *
     * @throws Exception
     */
    void removeListener(JobListener listener) throws Exception;

    /**
     * Add a job to be scheduled
     *
     * @param jobId
     *            a unique identifier for the job
     * @param payload
     *            the message to be sent when the job is scheduled
     * @param delay
     *            the time in milliseconds before the job will be run
     *
     * @throws Exception if an error occurs while scheduling the Job.
     */
    void schedule(String jobId, ByteSequence payload, long delay) throws Exception;

    /**
     * Add a job to be scheduled
     *
     * @param jobId
     *            a unique identifier for the job
     * @param payload
     *            the message to be sent when the job is scheduled
     * @param cronEntry
     *            The cron entry to use to schedule this job.
     *
     * @throws Exception if an error occurs while scheduling the Job.
     */
    void schedule(String jobId, ByteSequence payload, String cronEntry) throws Exception;

    /**
     * Add a job to be scheduled
     *
     * @param jobId
     *            a unique identifier for the job
     * @param payload
     *            the message to be sent when the job is scheduled
     * @param cronEntry
     *            cron entry
     * @param delay
     *            time in ms to wait before scheduling
     * @param period
     *            the time in milliseconds between successive executions of the Job
     * @param repeat
     *            the number of times to execute the job - less than 0 will be repeated forever
     * @throws Exception
     */
    void schedule(String jobId, ByteSequence payload, String cronEntry, long delay, long period, int repeat) throws Exception;

    /**
     * remove all jobs scheduled to run at this time
     *
     * @param time
     *      The UTC time to use to remove a batch of scheduled Jobs.
     *
     * @throws Exception
     */
    void remove(long time) throws Exception;

    /**
     * remove a job with the matching jobId
     *
     * @param jobId
     *      The unique Job Id to search for and remove from the scheduled set of jobs.
     *
     * @throws Exception if an error occurs while removing the Job.
     */
    void remove(String jobId) throws Exception;

    /**
     * remove all the Jobs from the scheduler
     *
     * @throws Exception
     */
    void removeAllJobs() throws Exception;

    /**
     * remove all the Jobs from the scheduler that are due between the start and finish times
     *
     * @param start
     *            time in milliseconds
     * @param finish
     *            time in milliseconds
     * @throws Exception
     */
    void removeAllJobs(long start, long finish) throws Exception;

    /**
     * Get the next time jobs will be fired
     *
     * @return the time in milliseconds
     * @throws Exception
     */
    long getNextScheduleTime() throws Exception;

    /**
     * Get all the jobs scheduled to run next
     *
     * @return a list of jobs that will be scheduled next
     * @throws Exception
     */
    List<Job> getNextScheduleJobs() throws Exception;

    /**
     * Get all the outstanding Jobs
     *
     * @return a list of all jobs
     * @throws Exception
     */
    List<Job> getAllJobs() throws Exception;

    /**
     * Get all outstanding jobs due to run between start and finish
     *
     * @param start
     * @param finish
     * @return a list of jobs
     * @throws Exception
     */
    List<Job> getAllJobs(long start, long finish) throws Exception;

}