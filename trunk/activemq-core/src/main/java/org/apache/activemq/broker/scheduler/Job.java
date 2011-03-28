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


public interface Job {

    /**
     * @return the jobId
     */
    public abstract String getJobId();

    /**
     * @return the repeat
     */
    public abstract int getRepeat();

    /**
     * @return the start
     */
    public abstract long getStart();

    /**
     * @return the Delay
     */
    public abstract long getDelay();
    /**
     * @return the period
     */
    public abstract long getPeriod();
    
    /**
     * @return the cron entry
     */
    public abstract String getCronEntry();

    /**
     * @return the payload
     */
    public abstract byte[] getPayload();
    
    /**
     * Get the start time as a Date time string
     * @return the date time
     */
    public String getStartTime();
    
    /**
     * Get the time the job is next due to execute 
     * @return the date time
     */
    public String getNextExecutionTime();

}