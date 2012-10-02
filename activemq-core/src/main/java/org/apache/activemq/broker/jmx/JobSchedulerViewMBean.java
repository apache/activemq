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
     * remove all jobs scheduled to run at this time
     * @param time
     * @throws Exception
     */
    @MBeanInfo("remove jobs with matching execution time")
    public abstract void removeJobAtScheduledTime(@MBeanInfo("time: yyyy-MM-dd hh:mm:ss")String time) throws Exception;

    /**
     * remove a job with the matching jobId
     * @param jobId
     * @throws Exception
     */
    @MBeanInfo("remove jobs with matching jobId")
    public abstract void removeJob(@MBeanInfo("jobId")String jobId) throws Exception;
    
    /**
     * remove all the Jobs from the scheduler
     * @throws Exception
     */
    @MBeanInfo("remove all scheduled jobs")
    public abstract void removeAllJobs() throws Exception;
    
    /**
     * remove all the Jobs from the scheduler that are due between the start and finish times
     * @param start time 
     * @param finish time
     * @throws Exception
     */
    @MBeanInfo("remove all scheduled jobs between time ranges ")
    public abstract void removeAllJobs(@MBeanInfo("start: yyyy-MM-dd hh:mm:ss")String start,@MBeanInfo("finish: yyyy-MM-dd hh:mm:ss")String finish) throws Exception;
    

    
    /**
     * Get the next time jobs will be fired
     * @return the time in milliseconds
     * @throws Exception 
     */
    @MBeanInfo("get the next time a job is due to be scheduled ")
    public abstract String getNextScheduleTime() throws Exception;
    
    /**
     * Get all the jobs scheduled to run next
     * @return a list of jobs that will be scheduled next
     * @throws Exception
     */
    @MBeanInfo("get the next job(s) to be scheduled. Not HTML friendly ")
    public abstract TabularData getNextScheduleJobs() throws Exception;
    
    /**
     * Get all the outstanding Jobs
     * @return a  table of all jobs
     * @throws Exception

     */
    @MBeanInfo("get the scheduled Jobs in the Store. Not HTML friendly ")
    public abstract TabularData getAllJobs() throws Exception;
    
    /**
     * Get all outstanding jobs due to run between start and finish
     * @param start
     * @param finish
     * @return a table of jobs in the range
     * @throws Exception

     */
    @MBeanInfo("get the scheduled Jobs in the Store within the time range. Not HTML friendly ")
    public abstract TabularData getAllJobs(@MBeanInfo("start: yyyy-MM-dd hh:mm:ss")String start,@MBeanInfo("finish: yyyy-MM-dd hh:mm:ss")String finish)throws Exception;
}
