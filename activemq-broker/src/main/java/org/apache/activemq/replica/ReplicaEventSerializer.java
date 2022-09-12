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
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.wireformat.WireFormat;

import java.io.IOException;

public class ReplicaEventSerializer {

    private final WireFormat wireFormat = new OpenWireFormatFactory().createWireFormat();

    byte[] serializeReplicationData(final DataStructure object) throws IOException {
        try {
            ByteSequence packet = wireFormat.marshal(object);
            return ByteSequenceData.toByteArray(packet);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to serialize data: " + object.toString() + " in container: " + e, e);
        }
    }

    byte[] serializeMessageData(final Message message) throws IOException {
        try {
            ByteSequence packet = wireFormat.marshal(message);
            return ByteSequenceData.toByteArray(packet);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to serialize message: " + message.getMessageId() + " in container: " + e, e);
        }
    }

    Object deserializeMessageData(final ByteSequence sequence) throws IOException {
        return wireFormat.unmarshal(sequence);
    }
}
