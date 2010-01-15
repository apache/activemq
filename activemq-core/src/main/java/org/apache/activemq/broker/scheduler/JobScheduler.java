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

import java.io.IOException;
import java.util.List;
import org.apache.kahadb.util.ByteSequence;

interface JobScheduler {

    /**
     * @return the name of the scheduler
     */
    public abstract String getName();
/**
 * Add a Job listener
 * @param l
 */
    public abstract void addListener(JobListener l);
/**
 * remove a JobListener
 * @param l
 */
    public abstract void removeListener(JobListener l);

    /**
     * Add a job to be scheduled
     * @param jobId a unique identifier for the job
     * @param payload the message to be sent when the job is scheduled
     * @param delay the time in milliseconds before the job will be run
     * @throws IOException
     */
    public abstract void schedule(String jobId, ByteSequence payload,long delay) throws IOException;

    
    /**
     * Add a job to be scheduled
     * @param jobId a unique identifier for the job
     * @param payload the message to be sent when the job is scheduled
     * @param start 
     * @param period the time in milliseconds between successive executions of the Job
     * @param repeat the number of times to execute the job - less than 0 will be repeated forever
     * @throws IOException
     */
    public abstract void schedule(String jobId, ByteSequence payload,long start, long period, int repeat) throws IOException;

    /**
     * remove all jobs scheduled to run at this time
     * @param time
     * @throws IOException
     */
    public abstract void remove(long time) throws IOException;

    /**
     * remove a job with the matching jobId
     * @param jobId
     * @throws IOException
     */
    public abstract void remove(String jobId) throws IOException;

    /**
     * Get all the jobs scheduled to run next
     * @return a list of messages that will be scheduled next
     * @throws IOException
     */
    public abstract List<ByteSequence> getNextScheduleJobs() throws IOException;
    
    /**
     * Get the next time jobs will be fired
     * @return the time in milliseconds
     * @throws IOException 
     */
    public abstract long getNextScheduleTime() throws IOException;

}