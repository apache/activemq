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

import org.apache.activemq.util.ByteSequence;

/**
 * Job event listener interface. Provides event points for Job related events
 * such as job ready events.
 */
public interface JobListener {
    public void willScheduleJob(String id, ByteSequence job) throws Exception;
    public void scheduleJob(String id, ByteSequence job) throws Exception;
    public void didScheduleJob(String id, ByteSequence job) throws Exception;

    public void willDispatchJob(String id, ByteSequence job) throws Exception;

    /**
     * A Job that has been scheduled is now ready to be fired.  The Job is passed
     * in its raw byte form and must be un-marshaled before being delivered.
     *
     * @param id
     *        The unique Job Id of the Job that is ready to fire.
     * @param job
     *        The job that is now ready, delivered in byte form.
     */
    public void dispatchJob(String id, ByteSequence job) throws Exception;
    public void didDispatchJob(String id, ByteSequence job) throws Exception;

    public void willRemoveJob(String id) throws Exception;
    public void removeJob(String id) throws Exception;
    public void didRemoveJob(String id) throws Exception;

    public void willRemoveRange(long start, long end) throws Exception;
    public void removeRange(long start, long end) throws Exception;
    public void didRemoveRange(long start, long end) throws Exception;


}
