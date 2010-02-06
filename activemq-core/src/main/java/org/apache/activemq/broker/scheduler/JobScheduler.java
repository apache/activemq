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
import org.apache.kahadb.util.ByteSequence;

public interface JobScheduler {

    /**
     * @return the name of the scheduler
     * @throws Exception 
     */
    public abstract String getName() throws Exception;
/**
 * Add a Job listener
 * @param l
 * @throws Exception 
 */
    public abstract void addListener(JobListener l) throws Exception;
/**
 * remove a JobListener
 * @param l
 * @throws Exception 
 */
    public abstract void removeListener(JobListener l) throws Exception;

    /**
     * Add a job to be scheduled
     * @param jobId a unique identifier for the job
     * @param payload the message to be sent when the job is scheduled
     * @param delay the time in milliseconds before the job will be run
     * @throws Exception
     */
    public abstract void schedule(String jobId, ByteSequence payload,long delay) throws Exception;

    
    /**
     * Add a job to be scheduled
     * @param jobId a unique identifier for the job
     * @param payload the message to be sent when the job is scheduled
     * @param start 
     * @param period the time in milliseconds between successive executions of the Job
     * @param repeat the number of times to execute the job - less than 0 will be repeated forever
     * @throws Exception
     */
    public abstract void schedule(String jobId, ByteSequence payload,long start, long period, int repeat) throws Exception;

    /**
     * remove all jobs scheduled to run at this time
     * @param time
     * @throws Exception 
     */
    public abstract void remove(long time) throws  Exception;

    /**
     * remove a job with the matching jobId
     * @param jobId
     * @throws Exception 
     */
    public abstract void remove(String jobId) throws  Exception;
    
    /**
     * remove all the Jobs from the scheduler
     * @throws Exception
     */
    public abstract void removeAllJobs() throws Exception;
    
    /**
     * remove all the Jobs from the scheduler that are due between the start and finish times
     * @param start time in milliseconds
     * @param finish time in milliseconds
     * @throws Exception
     */
    public abstract void removeAllJobs(long start,long finish) throws Exception;
    

    
    /**
     * Get the next time jobs will be fired
     * @return the time in milliseconds
     * @throws Exception 
     */
    public abstract long getNextScheduleTime() throws Exception;
    
    /**
     * Get all the jobs scheduled to run next
     * @return a list of jobs that will be scheduled next
     * @throws Exception
     */
    public abstract List<Job> getNextScheduleJobs() throws Exception;
    
    /**
     * Get all the outstanding Jobs
     * @return a  list of all jobs
     * @throws Exception 
     */
    public abstract List<Job> getAllJobs() throws Exception;
    
    /**
     * Get all outstanding jobs due to run between start and finish
     * @param start
     * @param finish
     * @return a list of jobs
     * @throws Exception
     */
    public abstract List<Job> getAllJobs(long start,long finish)throws Exception;

}