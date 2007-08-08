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

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Marshall a Message or a MessageReference
 * 
 * @version $Revision: 1.10 $
 */
public class CommandMarshaller implements Marshaller<Object> {

    private WireFormat wireFormat;

    public CommandMarshaller(WireFormat wireFormat) {
        this.wireFormat = wireFormat;

    }

    public CommandMarshaller() {
        this(new OpenWireFormat());
    }

    public void writePayload(Object object, DataOutput dataOut) throws IOException {
        ByteSequence packet = wireFormat.marshal(object);
        dataOut.writeInt(packet.length);
        dataOut.write(packet.data, packet.offset, packet.length);
    }

    public Object readPayload(DataInput dataIn) throws IOException {
        int size = dataIn.readInt();
        byte[] data = new byte[size];
        dataIn.readFully(data);
        return wireFormat.unmarshal(new ByteSequence(data));
    }
}
