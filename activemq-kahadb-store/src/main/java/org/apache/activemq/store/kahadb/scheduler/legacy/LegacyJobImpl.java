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

import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.util.ByteSequence;

/**
 * Legacy version Job and Job payload wrapper.  Allows for easy replay of stored
 * legacy jobs into a new JobSchedulerStoreImpl intsance.
 */
final class LegacyJobImpl {

    private final LegacyJobLocation jobLocation;
    private final Buffer payload;

    protected LegacyJobImpl(LegacyJobLocation location, ByteSequence payload) {
        this.jobLocation = location;
        this.payload = new Buffer(payload.data, payload.offset, payload.length);
    }

    public String getJobId() {
        return this.jobLocation.getJobId();
    }

    public Buffer getPayload() {
       return this.payload;
    }

    public long getPeriod() {
       return this.jobLocation.getPeriod();
    }

    public int getRepeat() {
       return this.jobLocation.getRepeat();
    }

    public long getDelay() {
        return this.jobLocation.getDelay();
    }

    public String getCronEntry() {
        return this.jobLocation.getCronEntry();
    }

    public long getNextExecutionTime() {
        return this.jobLocation.getNextTime();
    }

    public long getStartTime() {
        return this.jobLocation.getStartTime();
    }

    @Override
    public String toString() {
        return this.jobLocation.toString();
    }
}
