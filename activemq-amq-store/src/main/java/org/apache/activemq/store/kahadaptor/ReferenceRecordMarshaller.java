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
package org.apache.activemq.store.kahadaptor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.store.ReferenceStore.ReferenceData;

public class ReferenceRecordMarshaller implements Marshaller<ReferenceRecord> {

    public ReferenceRecord readPayload(DataInput dataIn) throws IOException {
        ReferenceRecord rr = new ReferenceRecord();
        rr.setMessageId(dataIn.readUTF());
        ReferenceData referenceData = new ReferenceData();
        referenceData.setFileId(dataIn.readInt());
        referenceData.setOffset(dataIn.readInt());
        referenceData.setExpiration(dataIn.readLong());
        rr.setData(referenceData);
        return rr;
    }

    /**
     * @param object
     * @param dataOut
     * @throws IOException
     * @see org.apache.activemq.kaha.Marshaller#writePayload(java.lang.Object,
     *      java.io.DataOutput)
     */
    public void writePayload(ReferenceRecord rr, DataOutput dataOut) throws IOException {
        dataOut.writeUTF(rr.getMessageId());
        dataOut.writeInt(rr.getData().getFileId());
        dataOut.writeInt(rr.getData().getOffset());
        dataOut.writeLong(rr.getData().getExpiration());
    }
}
