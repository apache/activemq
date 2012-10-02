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

import java.util.Collections;
import java.util.List;
import org.apache.kahadb.util.ByteSequence;

public class JobSchedulerFacade implements JobScheduler {

    private final SchedulerBroker broker;
    
    JobSchedulerFacade(SchedulerBroker broker){
        this.broker=broker;
    }
    public void addListener(JobListener l) throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            js.addListener(l);
        }
    }

    public List<Job> getAllJobs() throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            return js.getAllJobs();
        }
        return Collections.emptyList();
    }

    public List<Job> getAllJobs(long start, long finish) throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            return js.getAllJobs(start,finish);
        }
        return Collections.emptyList();
    }

    public String getName() throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            return js.getName();
        }
        return "";
    }

    public List<Job> getNextScheduleJobs() throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            return js.getNextScheduleJobs();
        }
        return Collections.emptyList();
    }

    public long getNextScheduleTime() throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            return js.getNextScheduleTime();
        }
        return 0;
    }

    public void remove(long time) throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            js.remove(time);
        }
    }

    public void remove(String jobId) throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            js.remove(jobId);
        }

    }

    public void removeAllJobs() throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            js.removeAllJobs();
        }
    }

    public void removeAllJobs(long start, long finish) throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            js.removeAllJobs(start,finish);
        }

    }

    public void removeListener(JobListener l) throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            js.removeListener(l);
        }

    }

    public void schedule(String jobId, ByteSequence payload, long delay) throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            js.schedule(jobId, payload, delay);
        }
    }

    public void schedule(String jobId, ByteSequence payload,String cronEntry, long start, long period, int repeat) throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            js.schedule(jobId, payload, cronEntry,start,period,repeat);
        }
    }
    public void schedule(String jobId, ByteSequence payload, String cronEntry) throws Exception {
        JobScheduler js = this.broker.getInternalScheduler();
        if (js !=null) {
            js.schedule(jobId, payload, cronEntry);
        }
        
    }
}
