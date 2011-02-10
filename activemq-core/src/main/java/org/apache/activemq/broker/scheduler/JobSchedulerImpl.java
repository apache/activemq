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

import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.VariableMarshaller;

class JobSchedulerImpl extends ServiceSupport implements Runnable, JobScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(JobSchedulerImpl.class);
    final JobSchedulerStore store;
    private final AtomicBoolean running = new AtomicBoolean();
    private String name;
    BTreeIndex<Long, List<JobLocation>> index;
    private Thread thread;
    private final List<JobListener> jobListeners = new CopyOnWriteArrayList<JobListener>();
    private static final IdGenerator ID_GENERATOR = new IdGenerator();
    private final ScheduleTime scheduleTime = new ScheduleTime();

    JobSchedulerImpl(JobSchedulerStore store) {

        this.store = store;
    }

    public void setName(String name) {
        this.name = name;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.activemq.beanstalk.JobScheduler#getName()
     */
    public String getName() {
        return this.name;
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.activemq.beanstalk.JobScheduler#addListener(org.apache.activemq
     * .beanstalk.JobListener)
     */
    public void addListener(JobListener l) {
        this.jobListeners.add(l);
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.activemq.beanstalk.JobScheduler#removeListener(org.apache.
     * activemq.beanstalk.JobListener)
     */
    public void removeListener(JobListener l) {
        this.jobListeners.remove(l);
    }

    public synchronized void schedule(final String jobId, final ByteSequence payload, final long delay) throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                schedule(tx, jobId, payload, "", 0, delay, 0);
            }
        });
    }

    public synchronized void schedule(final String jobId, final ByteSequence payload, final String cronEntry) throws Exception {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                schedule(tx, jobId, payload, cronEntry, 0, 0, 0);
            }
        });

    }

    public synchronized void schedule(final String jobId, final ByteSequence payload, final String cronEntry, final long delay,
            final long period, final int repeat) throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                schedule(tx, jobId, payload, cronEntry, delay, period, repeat);
            }
        });

    }

    /*
     * (non-Javadoc)
     * @see org.apache.activemq.beanstalk.JobScheduler#remove(long)
     */
    public synchronized void remove(final long time) throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                remove(tx, time);
            }
        });
    }

    synchronized void removeFromIndex(final long time, final String jobId) throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                removeFromIndex(tx, time, jobId);
            }
        });
    }

    /*
     * (non-Javadoc)
     * @see org.apache.activemq.beanstalk.JobScheduler#remove(long,
     * java.lang.String)
     */
    public synchronized void remove(final long time, final String jobId) throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                remove(tx, time, jobId);
            }
        });
    }

    /*
     * (non-Javadoc)
     * @see org.apache.activemq.beanstalk.JobScheduler#remove(java.lang.String)
     */
    public synchronized void remove(final String jobId) throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                remove(tx, jobId);
            }
        });
    }

    public synchronized long getNextScheduleTime() throws IOException {
        Map.Entry<Long, List<JobLocation>> first = this.index.getFirst(this.store.getPageFile().tx());
        return first != null ? first.getKey() : -1l;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.activemq.beanstalk.JobScheduler#getNextScheduleJobs()
     */
    public synchronized List<Job> getNextScheduleJobs() throws IOException {
        final List<Job> result = new ArrayList<Job>();

        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                Map.Entry<Long, List<JobLocation>> first = index.getFirst(store.getPageFile().tx());
                if (first != null) {
                    for (JobLocation jl : first.getValue()) {
                        ByteSequence bs = getPayload(jl.getLocation());
                        Job job = new JobImpl(jl, bs);
                        result.add(job);
                    }
                }
            }
        });
        return result;
    }

    public synchronized List<Job> getAllJobs() throws IOException {
        final List<Job> result = new ArrayList<Job>();
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
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
        return result;
    }

    public synchronized List<Job> getAllJobs(final long start, final long finish) throws IOException {
        final List<Job> result = new ArrayList<Job>();
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                Iterator<Map.Entry<Long, List<JobLocation>>> iter = index.iterator(store.getPageFile().tx(), start);
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
        return result;
    }

    public synchronized void removeAllJobs() throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                destroy(tx);
            }
        });
    }

    public synchronized void removeAllJobs(final long start, final long finish) throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                destroy(tx, start, finish);
            }
        });

    }

    ByteSequence getPayload(Location location) throws IllegalStateException, IOException {
        return this.store.getPayload(location);
    }

    void schedule(Transaction tx, String jobId, ByteSequence payload, String cronEntry, long delay, long period,
            int repeat) throws IOException {
        long startTime = System.currentTimeMillis();
        // round startTime - so we can schedule more jobs
        // at the same time
        startTime = (startTime / 1000) * 1000;
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

        Location location = this.store.write(payload, false);
        JobLocation jobLocation = new JobLocation(location);
        this.store.incrementJournalCount(tx, location);
        jobLocation.setJobId(jobId);
        jobLocation.setStartTime(startTime);
        jobLocation.setCronEntry(cronEntry);
        jobLocation.setDelay(delay);
        jobLocation.setPeriod(period);
        jobLocation.setRepeat(repeat);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Scheduling " + jobLocation);
        }
        storeJob(tx, jobLocation, time);
        this.scheduleTime.newJob();
    }

    synchronized void storeJob(final JobLocation jobLocation, final long nextExecutionTime) throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                storeJob(tx, jobLocation, nextExecutionTime);
            }
        });
    }

    void storeJob(final Transaction tx, final JobLocation jobLocation, final long nextExecutionTime) throws IOException {
        List<JobLocation> values = null;
        jobLocation.setNextTime(nextExecutionTime);
        if (this.index.containsKey(tx, nextExecutionTime)) {
            values = this.index.remove(tx, nextExecutionTime);
        }
        if (values == null) {
            values = new ArrayList<JobLocation>();
        }
        values.add(jobLocation);
        this.index.put(tx, nextExecutionTime, values);

    }

    void remove(Transaction tx, long time, String jobId) throws IOException {
        JobLocation result = removeFromIndex(tx, time, jobId);
        if (result != null) {
            this.store.decrementJournalCount(tx, result.getLocation());
        }
    }

    JobLocation removeFromIndex(Transaction tx, long time, String jobId) throws IOException {
        JobLocation result = null;
        List<JobLocation> values = this.index.remove(tx, time);
        if (values != null) {
            for (int i = 0; i < values.size(); i++) {
                JobLocation jl = values.get(i);
                if (jl.getJobId().equals(jobId)) {
                    values.remove(i);
                    if (!values.isEmpty()) {
                        this.index.put(tx, time, values);
                    }
                    result = jl;
                    break;
                }
            }
        }
        return result;
    }

    void remove(Transaction tx, long time) throws IOException {
        List<JobLocation> values = this.index.remove(tx, time);
        if (values != null) {
            for (JobLocation jl : values) {
                this.store.decrementJournalCount(tx, jl.getLocation());
            }
        }
    }

    void remove(Transaction tx, String id) throws IOException {
        for (Iterator<Map.Entry<Long, List<JobLocation>>> i = this.index.iterator(tx); i.hasNext();) {
            Map.Entry<Long, List<JobLocation>> entry = i.next();
            List<JobLocation> values = entry.getValue();
            if (values != null) {
                for (JobLocation jl : values) {
                    if (jl.getJobId().equals(id)) {
                        remove(tx, entry.getKey(), id);
                        return;
                    }
                }
            }
        }
    }

    synchronized void destroy(Transaction tx) throws IOException {
        List<Long> keys = new ArrayList<Long>();
        for (Iterator<Map.Entry<Long, List<JobLocation>>> i = this.index.iterator(tx); i.hasNext();) {
            Map.Entry<Long, List<JobLocation>> entry = i.next();
            keys.add(entry.getKey());
            List<JobLocation> values = entry.getValue();
            if (values != null) {
                for (JobLocation jl : values) {
                    this.store.decrementJournalCount(tx, jl.getLocation());
                }
            }
        }
        for (Long l : keys) {
            this.index.remove(tx, l);
        }
    }

    synchronized void destroy(Transaction tx, long start, long finish) throws IOException {
        List<Long> keys = new ArrayList<Long>();
        for (Iterator<Map.Entry<Long, List<JobLocation>>> i = this.index.iterator(tx, start); i.hasNext();) {
            Map.Entry<Long, List<JobLocation>> entry = i.next();
            if (entry.getKey().longValue() <= finish) {
                keys.add(entry.getKey());
                List<JobLocation> values = entry.getValue();
                if (values != null) {
                    for (JobLocation jl : values) {
                        this.store.decrementJournalCount(tx, jl.getLocation());
                    }
                }
            } else {
                break;
            }
        }
        for (Long l : keys) {
            this.index.remove(tx, l);
        }
    }

    private synchronized Map.Entry<Long, List<JobLocation>> getNextToSchedule() throws IOException {
        if (!this.store.isStopped() && !this.store.isStopping()) {
            Map.Entry<Long, List<JobLocation>> first = this.index.getFirst(this.store.getPageFile().tx());
            return first;
        }
        return null;

    }

    void fireJob(JobLocation job) throws IllegalStateException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Firing " + job);
        }
        ByteSequence bs = this.store.getPayload(job.getLocation());
        for (JobListener l : jobListeners) {
            l.scheduledJob(job.getJobId(), bs);
        }
    }

    public void run() {
        try {
            mainLoop();
        } catch (Throwable e) {
            if (this.running.get() && isStarted()) {
                LOG.error(this + " Caught exception in mainloop", e);
            }
        } finally {
            if (running.get()) {
                try {
                    stop();
                } catch (Exception e) {
                    LOG.error("Failed to stop " + this);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "JobScheduler:" + this.name;
    }

    protected void mainLoop() {
        while (this.running.get()) {
            this.scheduleTime.clearNewJob();
            try {
                // peek the next job
                long currentTime = System.currentTimeMillis();

                // Reads the list of the next entries and removes them from the store in one atomic step.
                // Prevents race conditions on short delays, when storeJob() tries to append new items to the 
                // existing list during this read operation (see AMQ-3141).
                synchronized (this) {
                    Map.Entry<Long, List<JobLocation>> first = getNextToSchedule();
                    if (first != null) {
                        List<JobLocation> list = new ArrayList<JobLocation>(first.getValue());
                        final long executionTime = first.getKey();
                        long nextExecutionTime = 0;
                        if (executionTime <= currentTime) {
    
                            for (final JobLocation job : list) {
                                int repeat = job.getRepeat();
                                nextExecutionTime = calculateNextExecutionTime(job, currentTime, repeat);
                                long waitTime = nextExecutionTime - currentTime;
                                this.scheduleTime.setWaitTime(waitTime);
                                if (job.isCron() == false) {
                                    fireJob(job);
                                    if (repeat != 0) {
                                        repeat--;
                                        job.setRepeat(repeat);
                                        // remove this job from the index - so it
                                        // doesn't get destroyed
                                        removeFromIndex(executionTime, job.getJobId());
                                        // and re-store it
                                        storeJob(job, nextExecutionTime);
                                    }
                                } else {
                                    // cron job
                                    if (repeat == 0) {
                                        // we haven't got a separate scheduler to
                                        // execute at
                                        // this time - just a cron job - so fire it
                                        fireJob(job);
                                    }
                                    if (nextExecutionTime > currentTime) {
                                        // we will run again ...
                                        // remove this job from the index - so it
                                        // doesn't get destroyed
                                        removeFromIndex(executionTime, job.getJobId());
                                        // and re-store it
                                        storeJob(job, nextExecutionTime);
                                        if (repeat != 0) {
                                            // we have a separate schedule to run at
                                            // this time
                                            // so the cron job is used to set of a
                                            // seperate scheule
                                            // hence we won't fire the original cron
                                            // job to the listeners
                                            // but we do need to start a separate
                                            // schedule
                                            String jobId = ID_GENERATOR.generateId();
                                            ByteSequence payload = getPayload(job.getLocation());
                                            schedule(jobId, payload, "", job.getDelay(), job.getPeriod(), job.getRepeat());
                                            waitTime = job.getDelay() != 0 ? job.getDelay() : job.getPeriod();
                                            this.scheduleTime.setWaitTime(waitTime);
                                        }
                                    }
                                }
                            }
                            // now remove all jobs that have not been
                            // rescheduled from this execution time
                            remove(executionTime);
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Not yet time to execute the job, waiting " + (executionTime - currentTime) + " ms");
                            }
                            this.scheduleTime.setWaitTime(executionTime - currentTime);
                        }
                    }
                }
                this.scheduleTime.pause();

            } catch (Exception ioe) {
                LOG.error(this.name + " Failed to schedule job", ioe);
                try {
                    this.store.stop();
                } catch (Exception e) {
                    LOG.error(this.name + " Failed to shutdown JobSchedulerStore", e);
                }
            }
        }
    }

    @Override
    protected void doStart() throws Exception {
        this.running.set(true);
        this.thread = new Thread(this, "JobScheduler:" + this.name);
        this.thread.setDaemon(true);
        this.thread.start();

    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        this.running.set(false);
        this.scheduleTime.wakeup();
        Thread t = this.thread;
        if (t != null) {
            t.join(1000);
        }

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
        this.index = new BTreeIndex<Long, List<JobLocation>>(this.store.getPageFile(), tx.allocate().getPageId());
    }

    void load(Transaction tx) throws IOException {
        this.index.setKeyMarshaller(LongMarshaller.INSTANCE);
        this.index.setValueMarshaller(ValueMarshaller.INSTANCE);
        this.index.load(tx);
    }

    void read(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.index = new BTreeIndex<Long, List<JobLocation>>(this.store.getPageFile(), in.readLong());
        this.index.setKeyMarshaller(LongMarshaller.INSTANCE);
        this.index.setValueMarshaller(ValueMarshaller.INSTANCE);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(this.index.getPageId());
    }

    static class ValueMarshaller extends VariableMarshaller<List<JobLocation>> {
        static ValueMarshaller INSTANCE = new ValueMarshaller();
        public List<JobLocation> readPayload(DataInput dataIn) throws IOException {
            List<JobLocation> result = new ArrayList<JobLocation>();
            int size = dataIn.readInt();
            for (int i = 0; i < size; i++) {
                JobLocation jobLocation = new JobLocation();
                jobLocation.readExternal(dataIn);
                result.add(jobLocation);
            }
            return result;
        }

        public void writePayload(List<JobLocation> value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.size());
            for (JobLocation jobLocation : value) {
                jobLocation.writeExternal(dataOut);
            }
        }
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
