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
package org.apache.kahadb.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implementation of a Marshaller for Strings
 * 
 * @version $Revision$
 */
public class StringMarshaller implements Marshaller<String> {
    
    public static final StringMarshaller INSTANCE = new StringMarshaller();
    
    /**
     * Write the payload of this entry to the RawContainer
     * 
     * @param object
     * @param dataOut
     * @throws IOException
     */
    public void writePayload(String object, DataOutput dataOut) throws IOException {
        dataOut.writeUTF(object);
    }

    /**
     * Read the entry from the RawContainer
     * 
     * @param dataIn
     * @return unmarshalled object
     * @throws IOException
     */
    public String readPayload(DataInput dataIn) throws IOException {
        return dataIn.readUTF();
    }


    public int getFixedSize() {
        return -1;
    }

    public String deepCopy(String source) {
        return source;
    }

    public boolean isDeepCopySupported() {
        return true;
    }
}
