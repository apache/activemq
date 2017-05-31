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
package org.apache.activemq.store.kahadb.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.MessageFormatException;

import org.apache.activemq.broker.scheduler.CronParser;
import org.apache.activemq.broker.scheduler.Job;
import org.apache.activemq.broker.scheduler.JobListener;
import org.apache.activemq.broker.scheduler.JobScheduler;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.kahadb.data.KahaAddScheduledJobCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveScheduledJobCommand;
import org.apache.activemq.store.kahadb.data.KahaRemoveScheduledJobsCommand;
import org.apache.activemq.store.kahadb.data.KahaRescheduleJobCommand;
import org.apache.activemq.store.kahadb.disk.index.BTreeIndex;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.LongMarshaller;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobSchedulerImpl extends ServiceSupport implements Runnable, JobScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(JobSchedulerImpl.class);
    private final JobSchedulerStoreImpl store;
    private final AtomicBoolean running = new AtomicBoolean();
    private String name;
    private BTreeIndex<Long, List<JobLocation>> index;
    private Thread thread;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final List<JobListener> jobListeners = new CopyOnWriteArrayList<>();
    private static final IdGenerator ID_GENERATOR = new IdGenerator();
    private final ScheduleTime scheduleTime = new ScheduleTime();

    JobSchedulerImpl(JobSchedulerStoreImpl store) {
        this.store = store;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void addListener(JobListener l) {
        this.jobListeners.add(l);
    }

    @Override
    public void removeListener(JobListener l) {
        this.jobListeners.remove(l);
    }

    @Override
    public void schedule(final String jobId, final ByteSequence payload, final long delay) throws IOException {
        doSchedule(jobId, payload, "", 0, delay, 0);
    }

    @Override
    public void schedule(final String jobId, final ByteSequence payload, final String cronEntry) throws Exception {
        doSchedule(jobId, payload, cronEntry, 0, 0, 0);
    }

    @Override
    public void schedule(final String jobId, final ByteSequence payload, final String cronEntry, final long delay, final long period, final int repeat) throws IOException {
        doSchedule(jobId, payload, cronEntry, delay, period, repeat);
    }

    @Override
    public void remove(final long time) throws IOException {
        doRemoveRange(time, time);
    }

    @Override
    public void remove(final String jobId) throws IOException {
        doRemove(-1, jobId);
    }

    @Override
    public void removeAllJobs() throws IOException {
        doRemoveRange(0, Long.MAX_VALUE);
    }

    @Override
    public void removeAllJobs(final long start, final long finish) throws IOException {
        doRemoveRange(start, finish);
    }

    @Override
    public long getNextScheduleTime() throws IOException {
        this.store.readLockIndex();
        try {
            Map.Entry<Long, List<JobLocation>> first = this.index.getFirst(this.store.getPageFile().tx());
            return first != null ? first.getKey() : -1l;
        } finally {
            this.store.readUnlockIndex();
        }
    }

    @Override
    public List<Job> getNextScheduleJobs() throws IOException {
        final List<Job> result = new ArrayList<>();
        this.store.readLockIndex();
        try {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    Map.Entry<Long, List<JobLocation>> first = index.getFirst(tx);
                    if (first != null) {
                        for (JobLocation jl : first.getValue()) {
                            ByteSequence bs = getPayload(jl.getLocation());
                            Job job = new JobImpl(jl, bs);
                            result.add(job);
                        }
                    }
                }
            });
        } finally {
            this.store.readUnlockIndex();
        }
        return result;
    }

    private Map.Entry<Long, List<JobLocation>> getNextToSchedule() throws IOException {
        this.store.readLockIndex();
        try {
            if (!this.store.isStopped() && !this.store.isStopping()) {
                Map.Entry<Long, List<JobLocation>> first = this.index.getFirst(this.store.getPageFile().tx());
                return first;
            }
        } finally {
            this.store.readUnlockIndex();
        }
        return null;
    }

    @Override
    public List<Job> getAllJobs() throws IOException {
        final List<Job> result = new ArrayList<>();
        this.store.readLockIndex();
        try {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    Iterator<Map.Entry<Long, List<JobLocation>>> iter = index.iterator(store.getPageFile().tx());
                    while (iter.hasNext()) {
                        Map.Entry<Long, List<JobLocation>> next = iter.next();
                        if (next != null) {
                            for (JobLocation jl : next.getValue()) {
                                ByteSequence bs = getPayload(jl.getLocation());
                                Job job = new JobImpl(jl, bs);
                                result.add(job);
                            }
                        } else {
                            break;
                        }
                    }
                }
            });
        } finally {
            this.store.readUnlockIndex();
        }
        return result;
    }

    @Override
    public List<Job> getAllJobs(final long start, final long finish) throws IOException {
        final List<Job> result = new ArrayList<>();
        this.store.readLockIndex();
        try {
            this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
                @Override
                public void execute(Transaction tx) throws IOException {
                    Iterator<Map.Entry<Long, List<JobLocation>>> iter = index.iterator(tx, start);
                    while (iter.hasNext()) {
                        Map.Entry<Long, List<JobLocation>> next = iter.next();
                        if (next != null && next.getKey().longValue() <= finish) {
                            for (JobLocation jl : next.getValue()) {
                                ByteSequence bs = getPayload(jl.getLocation());
                                Job job = new JobImpl(jl, bs);
                                result.add(job);
                            }
                        } else {
                            break;
                        }
                    }
                }
            });
        } finally {
            this.store.readUnlockIndex();
        }
        return result;
    }

    private void doSchedule(final String jobId, final ByteSequence payload, final String cronEntry, long delay, long period, int repeat) throws IOException {
        long startTime = System.currentTimeMillis();
        // round startTime - so we can schedule more jobs at the same time
        startTime = ((startTime + 500) / 500) * 500;

        long time = 0;
        if (cronEntry != null && cronEntry.length() > 0) {
            try {
                time = CronParser.getNextScheduledTime(cronEntry, startTime);
            } catch (MessageFormatException e) {
                throw new IOException(e.getMessage());
            }
        }

        if (time == 0) {
            // start time not set by CRON - so it it to the current time
            time = startTime;
        }

        if (delay > 0) {
            time += delay;
        } else {
            time += period;
        }

        KahaAddScheduledJobCommand newJob = new KahaAddScheduledJobCommand();
        newJob.setScheduler(name);
        newJob.setJobId(jobId);
        newJob.setStartTime(startTime);
        newJob.setCronEntry(cronEntry);
        newJob.setDelay(delay);
        newJob.setPeriod(period);
        newJob.setRepeat(repeat);
        newJob.setNextExecutionTime(time);
        newJob.setPayload(new Buffer(payload.getData(), payload.getOffset(), payload.getLength()));

        this.store.store(newJob);
    }

    private void doReschedule(final String jobId, long executionTime, long nextExecutionTime, int rescheduledCount) throws IOException {
        KahaRescheduleJobCommand update = new KahaRescheduleJobCommand();
        update.setScheduler(name);
        update.setJobId(jobId);
        update.setExecutionTime(executionTime);
        update.setNextExecutionTime(nextExecutionTime);
        update.setRescheduledCount(rescheduledCount);
        this.store.store(update);
    }

    private void doRemove(final long executionTime, final List<JobLocation> jobs) throws IOException {
        for (JobLocation job : jobs) {
            doRemove(executionTime, job.getJobId());
        }
    }

    private void doRemove(long executionTime, final String jobId) throws IOException {
        KahaRemoveScheduledJobCommand remove = new KahaRemoveScheduledJobCommand();
        remove.setScheduler(name);
        remove.setJobId(jobId);
        remove.setNextExecutionTime(executionTime);
        this.store.store(remove);
    }

    private void doRemoveRange(long start, long end) throws IOException {
        KahaRemoveScheduledJobsCommand destroy = new KahaRemoveScheduledJobsCommand();
        destroy.setScheduler(name);
        destroy.setStartTime(start);
        destroy.setEndTime(end);
        this.store.store(destroy);
    }

    /**
     * Adds a new Scheduled job to the index.  Must be called under index lock.
     *
     * This method must ensure that a duplicate add is not processed into the scheduler.  On index
     * recover some adds may be replayed and we don't allow more than one instance of a JobId to
     * exist at any given scheduled time, so filter these out to ensure idempotence.
     *
     * @param tx
     *      Transaction in which the update is performed.
     * @param command
     *      The new scheduled job command to process.
     * @param location
     *      The location where the add command is stored in the journal.
     *
     * @throws IOException if an error occurs updating the index.
     */
    protected void process(final Transaction tx, final KahaAddScheduledJobCommand command, Location location) throws IOException {
        JobLocation jobLocation = new JobLocation(location);
        jobLocation.setJobId(command.getJobId());
        jobLocation.setStartTime(command.getStartTime());
        jobLocation.setCronEntry(command.getCronEntry());
        jobLocation.setDelay(command.getDelay());
        jobLocation.setPeriod(command.getPeriod());
        jobLocation.setRepeat(command.getRepeat());

        long nextExecutionTime = command.getNextExecutionTime();

        List<JobLocation> values = null;
        jobLocation.setNextTime(nextExecutionTime);
        if (this.index.containsKey(tx, nextExecutionTime)) {
            values = this.index.remove(tx, nextExecutionTime);
        }
        if (values == null) {
            values = new ArrayList<>();
        }

        // There can never be more than one instance of the same JobId scheduled at any
        // given time, when it happens its probably the result of index recovery and this
        // method must be idempotent so check for it first.
        if (!values.contains(jobLocation)) {
            values.add(jobLocation);

            // Reference the log file where the add command is stored to prevent GC.
            this.store.incrementJournalCount(tx, location);
            this.index.put(tx, nextExecutionTime, values);
            this.scheduleTime.newJob();
        } else {
            this.index.put(tx, nextExecutionTime, values);
            LOG.trace("Job {} already in scheduler at this time {}",
                      jobLocation.getJobId(), jobLocation.getNextTime());
        }
    }

    /**
     * Reschedules a Job after it has be fired.
     *
     * For jobs that are repeating this method updates the job in the index by adding it to the
     * jobs list for the new execution time.  If the job is not a cron type job then this method
     * will reduce the repeat counter if the job has a fixed number of repeats set.  The Job will
     * be removed from the jobs list it just executed on.
     *
     * This method must also update the value of the last update location in the JobLocation
     * instance so that the checkpoint worker doesn't drop the log file in which that command lives.
     *
     * This method must ensure that an reschedule command that references a job that doesn't exist
     * does not cause an error since it's possible that on recover the original add might be gone
     * and so the job should not reappear in the scheduler.
     *
     * @param tx
     *      The TX under which the index is updated.
     * @param command
     *      The reschedule command to process.
     * @param location
     *      The location in the index where the reschedule command was stored.
     *
     * @throws IOException if an error occurs during the reschedule.
     */
    protected void process(final Transaction tx, final KahaRescheduleJobCommand command, Location location) throws IOException {
        JobLocation result = null;
        final List<JobLocation> current = this.index.remove(tx, command.getExecutionTime());
        if (current != null) {
            for (int i = 0; i < current.size(); i++) {
                JobLocation jl = current.get(i);
                if (jl.getJobId().equals(command.getJobId())) {
                    current.remove(i);
                    if (!current.isEmpty()) {
                        this.index.put(tx, command.getExecutionTime(), current);
                    }
                    result = jl;
                    break;
                }
            }
        } else {
            LOG.debug("Process reschedule command for job {} non-existent executime time {}.",
                      command.getJobId(), command.getExecutionTime());
        }

        if (result != null) {
            Location previousUpdate = result.getLastUpdate();

            List<JobLocation> target = null;
            result.setNextTime(command.getNextExecutionTime());
            result.setLastUpdate(location);
            result.setRescheduledCount(command.getRescheduledCount());
            if (!result.isCron() && result.getRepeat() > 0) {
                result.setRepeat(result.getRepeat() - 1);
            }
            if (this.index.containsKey(tx, command.getNextExecutionTime())) {
                target = this.index.remove(tx, command.getNextExecutionTime());
            }
            if (target == null) {
                target = new ArrayList<>();
            }
            target.add(result);

            // Track the location of the last reschedule command and release the log file
            // reference for the previous one if there was one.
            this.store.incrementJournalCount(tx, location);
            if (previousUpdate != null) {
                this.store.decrementJournalCount(tx, previousUpdate);
            }

            this.index.put(tx, command.getNextExecutionTime(), target);
            this.scheduleTime.newJob();
        } else {
            LOG.debug("Process reschedule command for non-scheduled job {} at executime time {}.",
                      command.getJobId(), command.getExecutionTime());
        }
    }

    /**
     * Removes a scheduled job from the scheduler.
     *
     * The remove operation can be of two forms.  The first is that there is a job Id but no set time
     * (-1) in which case the jobs index is searched until the target job Id is located.  The alternate
     * form is that a job Id and execution time are both set in which case the given time is checked
     * for a job matching that Id.  In either case once an execution time is identified the job is
     * removed and the index updated.
     *
     * This method should ensure that if the matching job is not found that no error results as it
     * is possible that on a recover the initial add command could be lost so the job may not be
     * rescheduled.
     *
     * @param tx
     *      The transaction under which the index is updated.
     * @param command
     *      The remove command to process.
     * @param location
     *      The location of the remove command in the Journal.
     *
     * @throws IOException if an error occurs while updating the scheduler index.
     */
    void process(final Transaction tx, final KahaRemoveScheduledJobCommand command, Location location) throws IOException {

        // Case 1: JobId and no time value means find the job and remove it.
        // Case 2: JobId and a time value means find exactly this scheduled job.

        Long executionTime = command.getNextExecutionTime();

        List<JobLocation> values = null;

        if (executionTime == -1) {
            for (Iterator<Map.Entry<Long, List<JobLocation>>> i = this.index.iterator(tx); i.hasNext();) {
                Map.Entry<Long, List<JobLocation>> entry = i.next();
                List<JobLocation> candidates = entry.getValue();
                if (candidates != null) {
                    for (JobLocation jl : candidates) {
                        if (jl.getJobId().equals(command.getJobId())) {
                            LOG.trace("Entry {} contains the remove target: {}", entry.getKey(), command.getJobId());
                            executionTime = entry.getKey();
                            values = this.index.remove(tx, executionTime);
                            break;
                        }
                    }
                }
            }
        } else {
            values = this.index.remove(tx, executionTime);
        }

        JobLocation removed = null;

        // Remove the job and update the index if there are any other jobs scheduled at this time.
        if (values != null) {
            for (JobLocation job : values) {
                if (job.getJobId().equals(command.getJobId())) {
                    removed = job;
                    values.remove(removed);
                    break;
                }
            }

            if (!values.isEmpty()) {
                this.index.put(tx, executionTime, values);
            }
        }

        if (removed != null) {
            LOG.trace("{} removed from scheduler {}", removed, this);

            // Remove the references for add and reschedule commands for this job
            // so that those logs can be GC'd when free.
            this.store.decrementJournalCount(tx, removed.getLocation());
            if (removed.getLastUpdate() != null) {
                this.store.decrementJournalCount(tx, removed.getLastUpdate());
            }

            // now that the job is removed from the index we can store the remove info and
            // then dereference the log files that hold the initial add command and the most
            // recent update command.  If the remove and the add that created the job are in
            // the same file we don't need to track it and just let a normal GC of the logs
            // remove it when the log is unreferenced.
            if (removed.getLocation().getDataFileId() != location.getDataFileId()) {
                this.store.referenceRemovedLocation(tx, location, removed);
            }
        }
    }

    /**
     * Removes all scheduled jobs within a given time range.
     *
     * The method can be used to clear the entire scheduler index by specifying a range that
     * encompasses all time [0...Long.MAX_VALUE] or a single execution time can be removed by
     * setting start and end time to the same value.
     *
     * @param tx
     *      The transaction under which the index is updated.
     * @param command
     *      The remove command to process.
     * @param location
     *      The location of the remove command in the Journal.
     *
     * @throws IOException if an error occurs while updating the scheduler index.
     */
    protected void process(final Transaction tx, final KahaRemoveScheduledJobsCommand command, Location location) throws IOException {
        removeInRange(tx, command.getStartTime(), command.getEndTime(), location);
    }

    /**
     * Removes all jobs from the schedulers index.  Must be called with the index locked.
     *
     * @param tx
     *      The transaction under which the index entries for this scheduler are removed.
     *
     * @throws IOException if an error occurs removing the jobs from the scheduler index.
     */
    protected void removeAll(Transaction tx) throws IOException {
        this.removeInRange(tx, 0, Long.MAX_VALUE, null);
    }

    /**
     * Removes all scheduled jobs within the target range.
     *
     * This method can be used to remove all the stored jobs by passing a range of [0...Long.MAX_VALUE]
     * or it can be used to remove all jobs at a given scheduled time by passing the same time value
     * for both start and end.  If the optional location parameter is set then this method will update
     * the store's remove location tracker with the location value and the Jobs that are being removed.
     *
     * This method must be called with the store index locked for writes.
     *
     * @param tx
     *      The transaction under which the index is to be updated.
     * @param start
     *      The start time for the remove operation.
     * @param finish
     *      The end time for the remove operation.
     * @param location (optional)
     *      The location of the remove command that triggered this remove.
     *
     * @throws IOException if an error occurs during the remove operation.
     */
    protected void removeInRange(Transaction tx, long start, long finish, Location location) throws IOException {
        List<Long> keys = new ArrayList<>();
        for (Iterator<Map.Entry<Long, List<JobLocation>>> i = this.index.iterator(tx, start); i.hasNext();) {
            Map.Entry<Long, List<JobLocation>> entry = i.next();
            if (entry.getKey().longValue() <= finish) {
                keys.add(entry.getKey());
            } else {
                break;
            }
        }

        for (Long executionTime : keys) {
            List<JobLocation> values = this.index.remove(tx, executionTime);
            if (location != null) {
                for (JobLocation job : values) {
                    LOG.trace("Removing {} scheduled at: {}", job, executionTime);

                    // Remove the references for add and reschedule commands for this job
                    // so that those logs can be GC'd when free.
                    this.store.decrementJournalCount(tx, job.getLocation());
                    if (job.getLastUpdate() != null) {
                        this.store.decrementJournalCount(tx, job.getLastUpdate());
                    }

                    // now that the job is removed from the index we can store the remove info and
                    // then dereference the log files that hold the initial add command and the most
                    // recent update command.  If the remove and the add that created the job are in
                    // the same file we don't need to track it and just let a normal GC of the logs
                    // remove it when the log is unreferenced.
                    if (job.getLocation().getDataFileId() != location.getDataFileId()) {
                        this.store.referenceRemovedLocation(tx, location, job);
                    }
                }
            }
        }
    }

    /**
     * Removes a Job from the index using it's Id value and the time it is currently set to
     * be executed.  This method will only remove the Job if it is found at the given execution
     * time.
     *
     * This method must be called under index lock.
     *
     * @param tx
     *        the transaction under which this method is being executed.
     * @param jobId
     *        the target Job Id to remove.
     * @param executionTime
     *        the scheduled time that for the Job Id that is being removed.
     *
     * @returns true if the Job was removed or false if not found at the given time.
     *
     * @throws IOException if an error occurs while removing the Job.
     */
    protected boolean removeJobAtTime(Transaction tx, String jobId, long executionTime) throws IOException {
        boolean result = false;

        List<JobLocation> jobs = this.index.remove(tx, executionTime);
        Iterator<JobLocation> jobsIter = jobs.iterator();
        while (jobsIter.hasNext()) {
            JobLocation job = jobsIter.next();
            if (job.getJobId().equals(jobId)) {
                jobsIter.remove();
                // Remove the references for add and reschedule commands for this job
                // so that those logs can be GC'd when free.
                this.store.decrementJournalCount(tx, job.getLocation());
                if (job.getLastUpdate() != null) {
                    this.store.decrementJournalCount(tx, job.getLastUpdate());
                }
                result = true;
                break;
            }
        }

        // Return the list to the index modified or unmodified.
        this.index.put(tx, executionTime, jobs);

        return result;
    }

    /**
     * Walks the Scheduled Job Tree and collects the add location and last update location
     * for all scheduled jobs.
     *
     * This method must be called with the index locked.
     *
     * @param tx
     *        the transaction under which this operation was invoked.
     *
     * @return a list of all referenced Location values for this JobSchedulerImpl
     *
     * @throws IOException if an error occurs walking the scheduler tree.
     */
    protected List<JobLocation> getAllScheduledJobs(Transaction tx) throws IOException {
        List<JobLocation> references = new ArrayList<>();

        for (Iterator<Map.Entry<Long, List<JobLocation>>> i = this.index.iterator(tx); i.hasNext();) {
            Map.Entry<Long, List<JobLocation>> entry = i.next();
            List<JobLocation> scheduled = entry.getValue();
            for (JobLocation job : scheduled) {
                references.add(job);
            }
        }

        return references;
    }

    @Override
    public void run() {
        try {
            mainLoop();
        } catch (Throwable e) {
            if (this.running.get() && isStarted()) {
                LOG.error("{} Caught exception in mainloop", this, e);
            }
        } finally {
            if (running.get()) {
                try {
                    stop();
                } catch (Exception e) {
                    LOG.error("Failed to stop {}", this);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "JobScheduler: " + this.name;
    }

    protected void mainLoop() {
        while (this.running.get()) {
            this.scheduleTime.clearNewJob();
            try {
                long currentTime = System.currentTimeMillis();

                // Read the list of scheduled events and fire the jobs, reschedule repeating jobs as
                // needed before firing the job event.
                Map.Entry<Long, List<JobLocation>> first = getNextToSchedule();
                if (first != null) {
                    List<JobLocation> list = new ArrayList<>(first.getValue());
                    List<JobLocation> toRemove = new ArrayList<>(list.size());
                    final long executionTime = first.getKey();
                    long nextExecutionTime = 0;
                    if (executionTime <= currentTime) {
                        for (final JobLocation job : list) {

                            if (!running.get()) {
                                break;
                            }

                            int repeat = job.getRepeat();
                            nextExecutionTime = calculateNextExecutionTime(job, currentTime, repeat);
                            long waitTime = nextExecutionTime - currentTime;
                            this.scheduleTime.setWaitTime(waitTime);
                            if (!job.isCron()) {
                                fireJob(job);
                                if (repeat != 0) {
                                    // Reschedule for the next time, the scheduler will take care of
                                    // updating the repeat counter on the update.
                                    doReschedule(job.getJobId(), executionTime, nextExecutionTime, job.getRescheduledCount() + 1);
                                } else {
                                    toRemove.add(job);
                                }
                            } else {
                                if (repeat == 0) {
                                    // This is a non-repeating Cron entry so we can fire and forget it.
                                    fireJob(job);
                                }

                                if (nextExecutionTime > currentTime) {
                                    // Reschedule the cron job as a new event, if the cron entry signals
                                    // a repeat then it will be stored separately and fired as a normal
                                    // event with decrementing repeat.
                                    doReschedule(job.getJobId(), executionTime, nextExecutionTime, job.getRescheduledCount() + 1);

                                    if (repeat != 0) {
                                        // we have a separate schedule to run at this time
                                        // so the cron job is used to set of a separate schedule
                                        // hence we won't fire the original cron job to the
                                        // listeners but we do need to start a separate schedule
                                        String jobId = ID_GENERATOR.generateId();
                                        ByteSequence payload = getPayload(job.getLocation());
                                        schedule(jobId, payload, "", job.getDelay(), job.getPeriod(), job.getRepeat());
                                        waitTime = job.getDelay() != 0 ? job.getDelay() : job.getPeriod();
                                        this.scheduleTime.setWaitTime(waitTime);
                                    }
                                } else {
                                    toRemove.add(job);
                                }
                            }
                        }

                        // now remove all jobs that have not been rescheduled from this execution
                        // time, if there are no more entries in that time it will be removed.
                        doRemove(executionTime, toRemove);

                        // If there is a job that should fire before the currently set wait time
                        // we need to reset wait time otherwise we'll miss it.
                        Map.Entry<Long, List<JobLocation>> nextUp = getNextToSchedule();
                        if (nextUp != null) {
                            final long timeUntilNextScheduled = nextUp.getKey() - currentTime;
                            if (timeUntilNextScheduled < this.scheduleTime.getWaitTime()) {
                                this.scheduleTime.setWaitTime(timeUntilNextScheduled);
                            }
                        }
                    } else {
                        this.scheduleTime.setWaitTime(executionTime - currentTime);
                    }
                }

                this.scheduleTime.pause();
            } catch (Exception ioe) {
                LOG.error("{} Failed to schedule job", this.name, ioe);
                try {
                    this.store.stop();
                } catch (Exception e) {
                    LOG.error("{} Failed to shutdown JobSchedulerStore", this.name, e);
                }
            }
        }
    }

    void fireJob(JobLocation job) throws IllegalStateException, IOException {
        LOG.debug("Firing: {}", job);
        ByteSequence bs = this.store.getPayload(job.getLocation());
        for (JobListener l : jobListeners) {
            l.scheduledJob(job.getJobId(), bs);
        }
    }

    @Override
    public void startDispatching() throws Exception {
        if (!this.running.get()) {
            return;
        }

        if (started.compareAndSet(false, true)) {
            this.thread = new Thread(this, "JobScheduler:" + this.name);
            this.thread.setDaemon(true);
            this.thread.start();
        }
    }

    @Override
    public void stopDispatching() throws Exception {
        if (started.compareAndSet(true, false)) {
            this.scheduleTime.wakeup();
            Thread t = this.thread;
            this.thread = null;
            if (t != null) {
                t.join(3000);
            }
        }
    }

    @Override
    protected void doStart() throws Exception {
        this.running.set(true);
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        this.running.set(false);
        stopDispatching();
    }

    private ByteSequence getPayload(Location location) throws IllegalStateException, IOException {
        return this.store.getPayload(location);
    }

    long calculateNextExecutionTime(final JobLocation job, long currentTime, int repeat) throws MessageFormatException {
        long result = currentTime;
        String cron = job.getCronEntry();
        if (cron != null && cron.length() > 0) {
            result = CronParser.getNextScheduledTime(cron, result);
        } else if (job.getRepeat() != 0) {
            result += job.getPeriod();
        }
        return result;
    }

    void createIndexes(Transaction tx) throws IOException {
        this.index = new BTreeIndex<>(this.store.getPageFile(), tx.allocate().getPageId());
    }

    void load(Transaction tx) throws IOException {
        this.index.setKeyMarshaller(LongMarshaller.INSTANCE);
        this.index.setValueMarshaller(JobLocationsMarshaller.INSTANCE);
        this.index.load(tx);
    }

    void read(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.index = new BTreeIndex<>(this.store.getPageFile(), in.readLong());
        this.index.setKeyMarshaller(LongMarshaller.INSTANCE);
        this.index.setValueMarshaller(JobLocationsMarshaller.INSTANCE);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(this.index.getPageId());
    }

    static class ScheduleTime {
        private final int DEFAULT_WAIT = 500;
        private final int DEFAULT_NEW_JOB_WAIT = 100;
        private boolean newJob;
        private long waitTime = DEFAULT_WAIT;
        private final Object mutex = new Object();

        /**
         * @return the waitTime
         */
        long getWaitTime() {
            return this.waitTime;
        }

        /**
         * @param waitTime
         *            the waitTime to set
         */
        void setWaitTime(long waitTime) {
            if (!this.newJob) {
                this.waitTime = waitTime > 0 ? waitTime : DEFAULT_WAIT;
            }
        }

        void pause() {
            synchronized (mutex) {
                try {
                    mutex.wait(this.waitTime);
                } catch (InterruptedException e) {
                }
            }
        }

        void newJob() {
            this.newJob = true;
            this.waitTime = DEFAULT_NEW_JOB_WAIT;
            wakeup();
        }

        void clearNewJob() {
            this.newJob = false;
        }

        void wakeup() {
            synchronized (this.mutex) {
                mutex.notifyAll();
            }
        }
    }
}
