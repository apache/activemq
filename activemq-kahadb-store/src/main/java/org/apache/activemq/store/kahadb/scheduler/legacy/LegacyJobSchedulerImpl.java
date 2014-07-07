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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.activemq.store.kahadb.disk.index.BTreeIndex;
import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.LongMarshaller;
import org.apache.activemq.store.kahadb.disk.util.VariableMarshaller;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;

/**
 * Read-only view of a stored legacy JobScheduler instance.
 */
final class LegacyJobSchedulerImpl extends ServiceSupport {

    private final LegacyJobSchedulerStoreImpl store;
    private String name;
    private BTreeIndex<Long, List<LegacyJobLocation>> index;

    LegacyJobSchedulerImpl(LegacyJobSchedulerStoreImpl store) {
        this.store = store;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    /**
     * Returns the next time that a job would be scheduled to run.
     *
     * @return time of next scheduled job to run.
     *
     * @throws IOException if an error occurs while fetching the time.
     */
    public long getNextScheduleTime() throws IOException {
        Map.Entry<Long, List<LegacyJobLocation>> first = this.index.getFirst(this.store.getPageFile().tx());
        return first != null ? first.getKey() : -1l;
    }

    /**
     * Gets the list of the next batch of scheduled jobs in the store.
     *
     * @return a list of the next jobs that will run.
     *
     * @throws IOException if an error occurs while fetching the jobs list.
     */
    public List<LegacyJobImpl> getNextScheduleJobs() throws IOException {
        final List<LegacyJobImpl> result = new ArrayList<LegacyJobImpl>();

        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            @Override
            public void execute(Transaction tx) throws IOException {
                Map.Entry<Long, List<LegacyJobLocation>> first = index.getFirst(store.getPageFile().tx());
                if (first != null) {
                    for (LegacyJobLocation jl : first.getValue()) {
                        ByteSequence bs = getPayload(jl.getLocation());
                        LegacyJobImpl job = new LegacyJobImpl(jl, bs);
                        result.add(job);
                    }
                }
            }
        });
        return result;
    }

    /**
     * Gets a list of all scheduled jobs in this store.
     *
     * @return a list of all the currently scheduled jobs in this store.
     *
     * @throws IOException if an error occurs while fetching the list of jobs.
     */
    public List<LegacyJobImpl> getAllJobs() throws IOException {
        final List<LegacyJobImpl> result = new ArrayList<LegacyJobImpl>();
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            @Override
            public void execute(Transaction tx) throws IOException {
                Iterator<Map.Entry<Long, List<LegacyJobLocation>>> iter = index.iterator(store.getPageFile().tx());
                while (iter.hasNext()) {
                    Map.Entry<Long, List<LegacyJobLocation>> next = iter.next();
                    if (next != null) {
                        for (LegacyJobLocation jl : next.getValue()) {
                            ByteSequence bs = getPayload(jl.getLocation());
                            LegacyJobImpl job = new LegacyJobImpl(jl, bs);
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

    /**
     * Gets a list of all scheduled jobs that exist between the given start and end time.
     *
     * @param start
     *      The start time to look for scheduled jobs.
     * @param finish
     *      The end time to stop looking for scheduled jobs.
     *
     * @return a list of all scheduled jobs that would run between the given start and end time.
     *
     * @throws IOException if an error occurs while fetching the list of jobs.
     */
    public List<LegacyJobImpl> getAllJobs(final long start, final long finish) throws IOException {
        final List<LegacyJobImpl> result = new ArrayList<LegacyJobImpl>();
        this.store.getPageFile().tx().execute(new Transaction.Closure<IOException>() {
            @Override
            public void execute(Transaction tx) throws IOException {
                Iterator<Map.Entry<Long, List<LegacyJobLocation>>> iter = index.iterator(store.getPageFile().tx(), start);
                while (iter.hasNext()) {
                    Map.Entry<Long, List<LegacyJobLocation>> next = iter.next();
                    if (next != null && next.getKey().longValue() <= finish) {
                        for (LegacyJobLocation jl : next.getValue()) {
                            ByteSequence bs = getPayload(jl.getLocation());
                            LegacyJobImpl job = new LegacyJobImpl(jl, bs);
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

    ByteSequence getPayload(Location location) throws IllegalStateException, IOException {
        return this.store.getPayload(location);
    }

    @Override
    public String toString() {
        return "LegacyJobScheduler: " + this.name;
    }

    @Override
    protected void doStart() throws Exception {
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
    }

    void createIndexes(Transaction tx) throws IOException {
        this.index = new BTreeIndex<Long, List<LegacyJobLocation>>(this.store.getPageFile(), tx.allocate().getPageId());
    }

    void load(Transaction tx) throws IOException {
        this.index.setKeyMarshaller(LongMarshaller.INSTANCE);
        this.index.setValueMarshaller(ValueMarshaller.INSTANCE);
        this.index.load(tx);
    }

    void read(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.index = new BTreeIndex<Long, List<LegacyJobLocation>>(this.store.getPageFile(), in.readLong());
        this.index.setKeyMarshaller(LongMarshaller.INSTANCE);
        this.index.setValueMarshaller(ValueMarshaller.INSTANCE);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeLong(this.index.getPageId());
    }

    static class ValueMarshaller extends VariableMarshaller<List<LegacyJobLocation>> {
        static ValueMarshaller INSTANCE = new ValueMarshaller();

        @Override
        public List<LegacyJobLocation> readPayload(DataInput dataIn) throws IOException {
            List<LegacyJobLocation> result = new ArrayList<LegacyJobLocation>();
            int size = dataIn.readInt();
            for (int i = 0; i < size; i++) {
                LegacyJobLocation jobLocation = new LegacyJobLocation();
                jobLocation.readExternal(dataIn);
                result.add(jobLocation);
            }
            return result;
        }

        @Override
        public void writePayload(List<LegacyJobLocation> value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.size());
            for (LegacyJobLocation jobLocation : value) {
                jobLocation.writeExternal(dataOut);
            }
        }
    }
}
