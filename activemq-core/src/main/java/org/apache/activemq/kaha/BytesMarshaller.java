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
package org.apache.activemq.kaha;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implementation of a Marshaller for byte arrays
 * 
 * @version $Revision: 1.2 $
 */
public class BytesMarshaller implements Marshaller {
    /**
     * Write the payload of this entry to the RawContainer
     * 
     * @param object
     * @param dataOut
     * @throws IOException
     */
    public void writePayload(Object object, DataOutput dataOut) throws IOException {
        byte[] data = (byte[])object;
        dataOut.writeInt(data.length);
        dataOut.write(data);
    }

    /**
     * Read the entry from the RawContainer
     * 
     * @param dataIn
     * @return unmarshalled object
     * @throws IOException
     */
    public Object readPayload(DataInput dataIn) throws IOException {
        int size = dataIn.readInt();
        byte[] data = new byte[size];
        dataIn.readFully(data);
        return data;
    }
}
