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
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.activemq.store.kahadb.AbstractKahaDBMetaData;
import org.apache.activemq.store.kahadb.disk.index.BTreeIndex;
import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.IntegerMarshaller;
import org.apache.activemq.store.kahadb.disk.util.LocationMarshaller;
import org.apache.activemq.store.kahadb.disk.util.StringMarshaller;
import org.apache.activemq.store.kahadb.disk.util.VariableMarshaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The KahaDB MetaData used to house the Index data for the KahaDB implementation
 * of a JobSchedulerStore.
 */
public class JobSchedulerKahaDBMetaData extends AbstractKahaDBMetaData<JobSchedulerKahaDBMetaData> {

    static final Logger LOG = LoggerFactory.getLogger(JobSchedulerKahaDBMetaData.class);

    private final JobSchedulerStoreImpl store;

    private UUID token = JobSchedulerStoreImpl.SCHEDULER_STORE_TOKEN;
    private int version = JobSchedulerStoreImpl.CURRENT_VERSION;

    private BTreeIndex<Integer, List<Integer>> removeLocationTracker;
    private BTreeIndex<Integer, Integer> journalRC;
    private BTreeIndex<String, JobSchedulerImpl> storedSchedulers;

    /**
     * Creates a new instance of this meta data object with the assigned
     * parent JobSchedulerStore instance.
     *
     * @param store
     *        the store instance that owns this meta data.
     */
    public JobSchedulerKahaDBMetaData(JobSchedulerStoreImpl store) {
        this.store = store;
    }

    /**
     * @return the current value of the Scheduler store identification token.
     */
    public UUID getToken() {
        return this.token;
    }

    /**
     * @return the current value of the version tag for this meta data instance.
     */
    public int getVersion() {
        return this.version;
    }

    /**
     * Gets the index that contains the location tracking information for Jobs
     * that have been removed from the index but whose add operation has yet
     * to be removed from the Journal.
     *
     * The Journal log file where a remove command is written cannot be released
     * until the log file with the original add command has also been released,
     * otherwise on a log replay the scheduled job could reappear in the scheduler
     * since its corresponding remove might no longer be present.
     *
     * @return the remove command location tracker index.
     */
    public BTreeIndex<Integer, List<Integer>> getRemoveLocationTracker() {
        return this.removeLocationTracker;
    }

    /**
     * Gets the index used to track the number of reference to a Journal log file.
     *
     * A log file in the Journal can only be considered for removal after all the
     * references to it have been released.
     *
     * @return the journal log file reference counter index.
     */
    public BTreeIndex<Integer, Integer> getJournalRC() {
        return this.journalRC;
    }

    /**
     * Gets the index of JobScheduler instances that have been created and stored
     * in the JobSchedulerStore instance.
     *
     * @return the index of stored JobScheduler instances.
     */
    public BTreeIndex<String, JobSchedulerImpl> getJobSchedulers() {
        return this.storedSchedulers;
    }

    @Override
    public void initialize(Transaction tx) throws IOException {
        this.storedSchedulers = new BTreeIndex<String, JobSchedulerImpl>(store.getPageFile(), tx.allocate().getPageId());
        this.journalRC = new BTreeIndex<Integer, Integer>(store.getPageFile(), tx.allocate().getPageId());
        this.removeLocationTracker = new BTreeIndex<Integer, List<Integer>>(store.getPageFile(), tx.allocate().getPageId());
    }

    @Override
    public void load(Transaction tx) throws IOException {
        this.storedSchedulers.setKeyMarshaller(StringMarshaller.INSTANCE);
        this.storedSchedulers.setValueMarshaller(new JobSchedulerMarshaller(this.store));
        this.storedSchedulers.load(tx);
        this.journalRC.setKeyMarshaller(IntegerMarshaller.INSTANCE);
        this.journalRC.setValueMarshaller(IntegerMarshaller.INSTANCE);
        this.journalRC.load(tx);
        this.removeLocationTracker.setKeyMarshaller(IntegerMarshaller.INSTANCE);
        this.removeLocationTracker.setValueMarshaller(new IntegerListMarshaller());
        this.removeLocationTracker.load(tx);
    }

    /**
     * Loads all the stored JobScheduler instances into the provided map.
     *
     * @param tx
     *        the Transaction under which the load operation should be executed.
     * @param schedulers
     *        a Map<String, JobSchedulerImpl> into which the loaded schedulers are stored.
     *
     * @throws IOException if an error occurs while performing the load operation.
     */
    public void loadScheduler(Transaction tx, Map<String, JobSchedulerImpl> schedulers) throws IOException {
        for (Iterator<Entry<String, JobSchedulerImpl>> i = this.storedSchedulers.iterator(tx); i.hasNext();) {
            Entry<String, JobSchedulerImpl> entry = i.next();
            entry.getValue().load(tx);
            schedulers.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void read(DataInput in) throws IOException {
        try {
            long msb = in.readLong();
            long lsb = in.readLong();
            this.token = new UUID(msb, lsb);
        } catch (Exception e) {
            throw new UnknownStoreVersionException(e);
        }

        if (!token.equals(JobSchedulerStoreImpl.SCHEDULER_STORE_TOKEN)) {
            throw new UnknownStoreVersionException(token.toString());
        }
        this.version = in.readInt();
        if (in.readBoolean()) {
            setLastUpdateLocation(LocationMarshaller.INSTANCE.readPayload(in));
        } else {
            setLastUpdateLocation(null);
        }
        this.storedSchedulers = new BTreeIndex<String, JobSchedulerImpl>(store.getPageFile(), in.readLong());
        this.storedSchedulers.setKeyMarshaller(StringMarshaller.INSTANCE);
        this.storedSchedulers.setValueMarshaller(new JobSchedulerMarshaller(this.store));
        this.journalRC = new BTreeIndex<Integer, Integer>(store.getPageFile(), in.readLong());
        this.journalRC.setKeyMarshaller(IntegerMarshaller.INSTANCE);
        this.journalRC.setValueMarshaller(IntegerMarshaller.INSTANCE);
        this.removeLocationTracker = new BTreeIndex<Integer, List<Integer>>(store.getPageFile(), in.readLong());
        this.removeLocationTracker.setKeyMarshaller(IntegerMarshaller.INSTANCE);
        this.removeLocationTracker.setValueMarshaller(new IntegerListMarshaller());

        LOG.info("Scheduler Store version {} loaded", this.version);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.token.getMostSignificantBits());
        out.writeLong(this.token.getLeastSignificantBits());
        out.writeInt(this.version);
        if (getLastUpdateLocation() != null) {
            out.writeBoolean(true);
            LocationMarshaller.INSTANCE.writePayload(getLastUpdateLocation(), out);
        } else {
            out.writeBoolean(false);
        }
        out.writeLong(this.storedSchedulers.getPageId());
        out.writeLong(this.journalRC.getPageId());
        out.writeLong(this.removeLocationTracker.getPageId());
    }

    private class JobSchedulerMarshaller extends VariableMarshaller<JobSchedulerImpl> {
        private final JobSchedulerStoreImpl store;

        JobSchedulerMarshaller(JobSchedulerStoreImpl store) {
            this.store = store;
        }

        @Override
        public JobSchedulerImpl readPayload(DataInput dataIn) throws IOException {
            JobSchedulerImpl result = new JobSchedulerImpl(this.store);
            result.read(dataIn);
            return result;
        }

        @Override
        public void writePayload(JobSchedulerImpl js, DataOutput dataOut) throws IOException {
            js.write(dataOut);
        }
    }

    private class IntegerListMarshaller extends VariableMarshaller<List<Integer>> {

        @Override
        public List<Integer> readPayload(DataInput dataIn) throws IOException {
            List<Integer> result = new ArrayList<Integer>();
            int size = dataIn.readInt();
            for (int i = 0; i < size; i++) {
                result.add(IntegerMarshaller.INSTANCE.readPayload(dataIn));
            }
            return result;
        }

        @Override
        public void writePayload(List<Integer> value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.size());
            for (Integer integer : value) {
                IntegerMarshaller.INSTANCE.writePayload(integer, dataOut);
            }
        }
    }
}
