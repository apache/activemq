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
package org.apache.activemq.transport.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;

import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Adds the extra methods available to text based wire format implementations
 *
 *
 */
public abstract class TextWireFormat implements WireFormat {

    public abstract Object unmarshalText(String text) throws IOException;

    public abstract Object unmarshalText(Reader reader) throws IOException;

    public abstract String marshalText(Object command) throws IOException;

    public void marshal(Object command, DataOutput out) throws IOException {
        String text = marshalText(command);
        byte[] utf8 = text.getBytes("UTF-8");
        out.writeInt(utf8.length);
        out.write(utf8);
    }

    public Object unmarshal(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] utf8 = new byte[length];
        in.readFully(utf8);
        String text = new String(utf8, "UTF-8");
        return unmarshalText(text);
    }

    public ByteSequence marshal(Object command) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        marshal(command, dos);
        dos.close();
        return baos.toByteSequence();
    }

    public Object unmarshal(ByteSequence packet) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(packet);
        DataInputStream dis = new DataInputStream(stream);
        return unmarshal(dis);
    }

    public boolean inReceive() {
        // TODO Implement for inactivity monitor
        return false;
    }

}
