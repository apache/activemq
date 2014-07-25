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

import java.io.File;

import org.apache.activemq.Service;

/**
 * A Job Scheduler Store interface use to manage delay processing of Messaging
 * related jobs.
 */
public interface JobSchedulerStore extends Service {

    /**
     * Gets the location where the Job Scheduler will write the persistent data used
     * to preserve and recover scheduled Jobs.
     *
     * If the scheduler implementation does not utilize a file system based store this
     * method returns null.
     *
     * @return the directory where persistent store data is written.
     */
    File getDirectory();

    /**
     * Sets the directory where persistent store data will be written.  This method
     * must be called before the scheduler store is started to have any effect.
     *
     * @param directory
     *      The directory where the job scheduler store is to be located.
     */
    void setDirectory(File directory);

    /**
     * The size of the current store on disk if the store utilizes a disk based store
     * mechanism.
     *
     * @return the current store size on disk.
     */
    long size();

    /**
     * Returns the JobScheduler instance identified by the given name.
     *
     * @param name
     *        the name of the JobScheduler instance to lookup.
     *
     * @return the named JobScheduler or null if none exists with the given name.
     *
     * @throws Exception if an error occurs while loading the named scheduler.
     */
    JobScheduler getJobScheduler(String name) throws Exception;

    /**
     * Removes the named JobScheduler if it exists, purging all scheduled messages
     * assigned to it.
     *
     * @param name
     *        the name of the scheduler instance to remove.
     *
     * @return true if there was a scheduler with the given name to remove.
     *
     * @throws Exception if an error occurs while removing the scheduler.
     */
    boolean removeJobScheduler(String name) throws Exception;
}
