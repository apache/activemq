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
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.ByteSequence;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.VariableMarshaller;

class JobSchedulerImpl extends ServiceSupport implements Runnable, JobScheduler {
    private static final Log LOG = LogFactory.getLog(JobSchedulerImpl.class);
    final JobSchedulerStore store;
    private final AtomicBoolean running = new AtomicBoolean();
    private String name;
    BTreeIndex<Long, List<JobLocation>> index;
    private Thread thread;
    private final List<JobListener> jobListeners = new CopyOnWriteArrayList<JobListener>();

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

   
    public void schedule(final String jobId, final ByteSequence payload, final long delay) throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                schedule(tx, jobId, payload, 0, delay, 0);
            }
        });
    }

   
    public void schedule(final String jobId, final ByteSequence payload, final long start, final long period, final int repeat) throws IOException {
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                schedule(tx, jobId, payload, start, period, repeat);
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
    public synchronized List<ByteSequence> getNextScheduleJobs() throws IOException {
        final List<ByteSequence> result = new ArrayList<ByteSequence>();

        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            public void execute(Transaction tx) throws IOException {
                Map.Entry<Long, List<JobLocation>> first = index.getFirst(store.getPageFile().tx());
                if (first != null) {
                    for (JobLocation jl : first.getValue()) {
                        ByteSequence bs = getJob(jl.getLocation());
                        result.add(bs);
                    }
                }
            }
        });
        return result;
    }

    ByteSequence getJob(Location location) throws IllegalStateException, IOException {
        return this.store.getJob(location);
    }

    void schedule(Transaction tx,  String jobId, ByteSequence payload,long start, long period, int repeat)
            throws IOException {
        List<JobLocation> values = null;
        long startTime;
        long time;
        if (start > 0) {
            time = startTime = start;
        }else {
            startTime = System.currentTimeMillis();
            time = startTime + period;
        }
        if (this.index.containsKey(tx, time)) {
            values = this.index.remove(tx, time);
        }
        if (values == null) {
            values = new ArrayList<JobLocation>();
        }

        Location location = this.store.write(payload, false);
        JobLocation jobLocation = new JobLocation(location);
        jobLocation.setJobId(jobId);
        jobLocation.setPeriod(period);
        jobLocation.setRepeat(repeat);
        values.add(jobLocation);
        this.index.put(tx, time, values);
        this.store.incrementJournalCount(tx, location);
        poke();
    }

    void remove(Transaction tx, long time, String jobId) throws IOException {
        List<JobLocation> values = this.index.remove(tx, time);
        if (values != null) {
            for (int i = 0; i < values.size(); i++) {
                JobLocation jl = values.get(i);
                if (jl.getJobId().equals(jobId)) {
                    values.remove(i);
                    if (!values.isEmpty()) {
                        this.index.put(tx, time, values);
                    }
                    this.store.decrementJournalCount(tx, jl.getLocation());
                    break;
                }
            }
        }
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
        for (Iterator<Map.Entry<Long, List<JobLocation>>> i = this.index.iterator(tx); i.hasNext();) {
            Map.Entry<Long, List<JobLocation>> entry = i.next();
            List<JobLocation> values = entry.getValue();
            if (values != null) {
                for (JobLocation jl : values) {
                    this.store.decrementJournalCount(tx, jl.getLocation());
                }
            }

        }
    }

    synchronized Map.Entry<Long, List<JobLocation>> getNextToSchedule() throws IOException {
        if (!this.store.isStopped() && !this.store.isStopping()) {
            Map.Entry<Long, List<JobLocation>> first = this.index.getFirst(this.store.getPageFile().tx());
            return first;
        }
        return null;

    }

    void fireJobs(List<JobLocation> list) throws IllegalStateException, IOException {
        for (JobLocation jl : list) {
            ByteSequence bs = this.store.getJob(jl.getLocation());
            for (JobListener l : jobListeners) {
                l.scheduledJob(jl.getJobId(), bs);
            }
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
            try {
                // peek the next job
                long currentTime = System.currentTimeMillis();

                Map.Entry<Long, List<JobLocation>> first = getNextToSchedule();
                if (first != null) {
                    List<JobLocation> list = new ArrayList<JobLocation>(first.getValue());
                    long executionTime = first.getKey();
                    if (executionTime <= currentTime) {
                        fireJobs(list);
                        for (JobLocation jl : list) {
                            int repeat = jl.getRepeat();
                            if (repeat != 0) {
                                repeat--;
                                ByteSequence payload = this.store.getJob(jl.getLocation());
                                String jobId = jl.getJobId();
                                long period = jl.getPeriod();
                                schedule(jobId, payload,0, period, repeat);
                            }
                        }
                        // now remove jobs from this execution time
                        remove(executionTime);
                    } else {
                        long waitTime = executionTime - currentTime;
                        synchronized (this.running) {
                            this.running.wait(waitTime);
                        }
                    }
                } else {
                    synchronized (this.running) {
                        this.running.wait(250);
                    }
                }

            } catch (InterruptedException e) {
            } catch (IOException ioe) {
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
        poke();
        Thread t = this.thread;
        if (t != null) {
            t.join(1000);
        }

    }

    protected void poke() {
        synchronized (this.running) {
            this.running.notifyAll();
        }
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

 

}
