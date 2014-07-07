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
import java.util.List;

import org.apache.activemq.store.kahadb.disk.util.VariableMarshaller;

/**
 * A VariableMarshaller instance that performs the read and write of a list of
 * JobLocation objects using the JobLocation's built in read and write methods.
 */
class JobLocationsMarshaller extends VariableMarshaller<List<JobLocation>> {
    static JobLocationsMarshaller INSTANCE = new JobLocationsMarshaller();

    @Override
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

    @Override
    public void writePayload(List<JobLocation> value, DataOutput dataOut) throws IOException {
        dataOut.writeInt(value.size());
        for (JobLocation jobLocation : value) {
            jobLocation.writeExternal(dataOut);
        }
    }
}