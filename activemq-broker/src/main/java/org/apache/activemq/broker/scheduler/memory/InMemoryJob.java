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
package org.apache.activemq.broker.scheduler.memory;

import org.apache.activemq.broker.scheduler.Job;
import org.apache.activemq.broker.scheduler.JobSupport;

/**
 * A simple in memory Job POJO.
 */
public class InMemoryJob implements Job {

    private final String jobId;

    private int repeat;
    private long start;
    private long nextTime;
    private long delay;
    private long period;
    private String cronEntry;
    private int executionCount;

    private byte[] payload;

    public InMemoryJob(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public int getRepeat() {
        return repeat;
    }

    public void setRepeat(int repeat) {
        this.repeat = repeat;
    }

    @Override
    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getNextTime() {
        return nextTime;
    }

    public void setNextTime(long nextTime) {
        this.nextTime = nextTime;
    }

    @Override
    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    @Override
    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    @Override
    public String getCronEntry() {
        return cronEntry;
    }

    public void setCronEntry(String cronEntry) {
        this.cronEntry = cronEntry;
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    @Override
    public String getStartTime() {
        return JobSupport.getDateTime(getStart());
    }

    @Override
    public String getNextExecutionTime() {
        return JobSupport.getDateTime(getNextTime());
    }

    @Override
    public int getExecutionCount() {
        return executionCount;
    }

    public void incrementExecutionCount() {
        this.executionCount++;
    }

    public void decrementRepeatCount() {
        if (this.repeat > 0) {
            this.repeat--;
        }
    }

    /**
     * @return true if this Job represents a Cron entry.
     */
    public boolean isCron() {
        return getCronEntry() != null && getCronEntry().length() > 0;
    }

    @Override
    public int hashCode() {
        return jobId.hashCode();
    }

    @Override
    public String toString() {
        return "Job: " + getJobId();
    }
}
