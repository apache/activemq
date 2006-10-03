/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
/**
 * Implementation of a Marshaller for Objects
 * 
 * @version $Revision: 1.2 $
 */
public class ObjectMarshaller implements Marshaller{
    /**
     * Write the payload of this entry to the RawContainer
     * 
     * @param object
     * @param dataOut
     * @throws IOException
     */
    public void writePayload(Object object,DataOutput dataOut) throws IOException{

// I failed to see why we just did not just used the provided stream directly        
//        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
//        ObjectOutputStream objectOut=new ObjectOutputStream(bytesOut);
//        objectOut.writeObject(object);
//        objectOut.close();
//        byte[] data = bytesOut.toByteArray();
//        dataOut.writeInt(data.length);
//        dataOut.write(data);
        
        ObjectOutputStream objectOut=new ObjectOutputStream((OutputStream) dataOut);
        objectOut.writeObject(object);
        objectOut.flush();
        objectOut.reset();
    }

    /**
     * Read the entry from the RawContainer
     * 
     * @param dataIn
     * @return unmarshalled object
     * @throws IOException
     */
    public Object readPayload(DataInput dataIn) throws IOException{
        
// I failed to see why we just did not just used the provided stream directly        
//        int size = dataIn.readInt();
//        byte[] data = new byte[size];
//        dataIn.readFully(data);
//        ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
//        ObjectInputStream objectIn=new ObjectInputStream(bytesIn);
//        try{
//            return objectIn.readObject();
//        }catch(ClassNotFoundException e){
//            throw new IOException(e.getMessage());
//        }
        
        ObjectInputStream objectIn=new ObjectInputStream((InputStream) dataIn);
        try{
            return objectIn.readObject();
        } catch(ClassNotFoundException e) {
            throw new IOException(e.getMessage());
        }

    }
}
