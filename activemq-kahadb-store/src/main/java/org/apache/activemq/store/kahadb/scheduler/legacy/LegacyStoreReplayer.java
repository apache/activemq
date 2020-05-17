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
package org.apache.activemq.store.kahadb.scheduler.legacy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.activemq.store.kahadb.data.KahaAddScheduledJobCommand;
import org.apache.activemq.store.kahadb.scheduler.JobSchedulerStoreImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to upgrade a Legacy Job Scheduler store to the latest version this class
 * loads a found legacy scheduler store and generates new add commands for all
 * jobs currently in the store.
 */
public class LegacyStoreReplayer {

    static final Logger LOG = LoggerFactory.getLogger(LegacyStoreReplayer.class);

    private LegacyJobSchedulerStoreImpl store;
    private final File legacyStoreDirectory;

    /**
     * Creates a new Legacy Store Replayer with the given target store
     * @param directory
     */
    public LegacyStoreReplayer(File directory) {
        this.legacyStoreDirectory = directory;
    }

    /**
     * Loads the legacy store and prepares it for replay into a newer Store instance.
     *
     * @throws IOException if an error occurs while reading in the legacy store.
     */
    public void load() throws IOException {

        store = new LegacyJobSchedulerStoreImpl();
        store.setDirectory(legacyStoreDirectory);
        store.setFailIfDatabaseIsLocked(true);

        try {
            store.start();
        } catch (IOException ioe) {
            LOG.warn("Legacy store load failed: ", ioe);
            throw ioe;
        } catch (Exception e) {
            LOG.warn("Legacy store load failed: ", e);
            throw new IOException(e);
        }
    }

    /**
     * Unloads a previously loaded legacy store to release any resources associated with it.
     *
     * Once a store is unloaded it cannot be replayed again until it has been reloaded.
     * @throws IOException
     */
    public void unload() throws IOException {

        if (store != null) {
            try {
                store.stop();
            } catch (Exception e) {
                LOG.warn("Legacy store unload failed: ", e);
                throw new IOException(e);
            } finally {
                store = null;
            }
        }
    }

    /**
     * Performs a replay of scheduled jobs into the target JobSchedulerStore.
     *
     * @param targetStore
     *      The JobSchedulerStore that will receive the replay events from the legacy store.
     *
     * @throws IOException if an error occurs during replay of the legacy store.
     */
    public void startReplay(JobSchedulerStoreImpl targetStore) throws IOException {
        checkLoaded();

        if (targetStore == null) {
            throw new IOException("Cannot replay to a null store");
        }

        try {
            Set<String> schedulers = store.getJobSchedulerNames();
            if (!schedulers.isEmpty()) {

                for (String name : schedulers) {
                    LegacyJobSchedulerImpl scheduler = store.getJobScheduler(name);
                    LOG.info("Replay of legacy store {} starting.", name);
                    replayScheduler(scheduler, targetStore);
                }
            }

            LOG.info("Replay of legacy store complate.");
        } catch (IOException ioe) {
            LOG.warn("Failed during replay of legacy store: ", ioe);
            throw ioe;
        } catch (Exception e) {
            LOG.warn("Failed during replay of legacy store: ", e);
            throw new IOException(e);
        }
    }

    private final void replayScheduler(LegacyJobSchedulerImpl legacy, JobSchedulerStoreImpl target) throws Exception {
        List<LegacyJobImpl> jobs = legacy.getAllJobs();

        String schedulerName = legacy.getName();

        for (LegacyJobImpl job : jobs) {
            LOG.trace("Storing job from legacy store to new store: {}", job);
            KahaAddScheduledJobCommand newJob = new KahaAddScheduledJobCommand();
            newJob.setScheduler(schedulerName);
            newJob.setJobId(job.getJobId());
            newJob.setStartTime(job.getStartTime());
            newJob.setCronEntry(job.getCronEntry());
            newJob.setDelay(job.getDelay());
            newJob.setPeriod(job.getPeriod());
            newJob.setRepeat(job.getRepeat());
            newJob.setNextExecutionTime(job.getNextExecutionTime());
            newJob.setPayload(job.getPayload());

            target.store(newJob);
        }
    }

    private final void checkLoaded() throws IOException {
        if (this.store == null) {
            throw new IOException("Cannot replay until legacy store is loaded.");
        }
    }
}
