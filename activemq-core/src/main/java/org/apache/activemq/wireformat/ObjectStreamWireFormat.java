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
package org.apache.activemq.wireformat;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ClassLoadingAwareObjectInputStream;

/**
 * A simple implementation which uses Object Stream serialization.
 *
 * @version $Revision: 1.1 $
 */
public class ObjectStreamWireFormat implements WireFormat {

    public ByteSequence marshal(Object command) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();        
        DataOutputStream ds = new DataOutputStream(baos);
        marshal(command, ds);
        ds.close();
        return baos.toByteSequence();
    }

    public Object unmarshal(ByteSequence packet) throws IOException {
        return unmarshal(new DataInputStream(new ByteArrayInputStream(packet)));
    }

    public void marshal(Object command, DataOutput ds) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream((OutputStream)ds);
        out.writeObject(command);
        out.flush();
        out.reset();
    }

    public Object unmarshal(DataInput ds) throws IOException {
        try {
            ClassLoadingAwareObjectInputStream in = new ClassLoadingAwareObjectInputStream((InputStream)ds);
            Object command;
            command = in.readObject();
            in.close();
            return command;
        } catch (ClassNotFoundException e) {
            throw (IOException)new IOException("unmarshal failed: "+e).initCause(e);
        }
    }

    public void setVersion(int version) {
    }

    public int getVersion() {
        return 0;
    }

}
