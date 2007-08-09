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
package org.apache.activemq.transport.multicast;

import java.net.SocketAddress;
import java.nio.ByteBuffer;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.Endpoint;
import org.apache.activemq.transport.udp.DatagramEndpoint;
import org.apache.activemq.transport.udp.DatagramHeaderMarshaller;

/**
 * 
 * @version $Revision$
 */
public class MulticastDatagramHeaderMarshaller extends DatagramHeaderMarshaller {

    private final String localUri;
    private final byte[] localUriAsBytes;

    public MulticastDatagramHeaderMarshaller(String localUri) {
        this.localUri = localUri;
        this.localUriAsBytes = localUri.getBytes();
    }

    public Endpoint createEndpoint(ByteBuffer readBuffer, SocketAddress address) {
        int size = readBuffer.getInt();
        byte[] data = new byte[size];
        readBuffer.get(data);
        return new DatagramEndpoint(new String(data), address);
    }

    public void writeHeader(Command command, ByteBuffer writeBuffer) {
        writeBuffer.putInt(localUriAsBytes.length);
        writeBuffer.put(localUriAsBytes);
        super.writeHeader(command, writeBuffer);
    }

}
