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
package org.apache.activemq.replica;

import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.Message;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.DataByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReplicaEventSerializer {

    private final WireFormat wireFormat = new OpenWireFormatFactory().createWireFormat();

    byte[] serializeReplicationData(DataStructure object) throws IOException {
        try {
            ByteSequence packet = wireFormat.marshal(object);
            return ByteSequenceData.toByteArray(packet);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to serialize data: " + object.toString() + " in container: " + e, e);
        }
    }

    byte[] serializeMessageData(Message message) throws IOException {
        try {
            ByteSequence packet = wireFormat.marshal(message);
            return ByteSequenceData.toByteArray(packet);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to serialize message: " + message.getMessageId() + " in container: " + e, e);
        }
    }

    Object deserializeMessageData(ByteSequence sequence) throws IOException {
        return wireFormat.unmarshal(sequence);
    }

    byte[] serializeListOfObjects(List<DataStructure> list) throws IOException {
        List<byte[]> listOfByteArrays = new ArrayList<>();
        for (DataStructure dataStructure : list) {
            listOfByteArrays.add(serializeReplicationData(dataStructure));
        }

        int listSize = listOfByteArrays.stream().map(a -> a.length).reduce(0, Integer::sum);

        DataByteArrayOutputStream dbaos = new DataByteArrayOutputStream(4 + 2 * listOfByteArrays.size() + listSize);

        dbaos.writeInt(listOfByteArrays.size());
        for (byte[] b : listOfByteArrays) {
            dbaos.writeInt(b.length);
            dbaos.write(b);
        }

        return ByteSequenceData.toByteArray(dbaos.toByteSequence());
    }

    List<Object> deserializeListOfObjects(byte[] bytes) throws IOException {
        List<Object> result = new ArrayList<>();

        DataByteArrayInputStream dbais = new DataByteArrayInputStream(bytes);

        int listSize = dbais.readInt();
        for (int i = 0; i < listSize; i++) {
            int size = dbais.readInt();

            byte[] b = new byte[size];
            dbais.readFully(b);
            result.add(deserializeMessageData(new ByteSequence(b)));
        }

        return result;
    }
}
