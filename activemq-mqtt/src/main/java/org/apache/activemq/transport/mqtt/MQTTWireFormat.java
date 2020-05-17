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
package org.apache.activemq.transport.mqtt;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.mqtt.codec.MQTTFrame;

/**
 * Implements marshalling and unmarsalling the <a
 * href="http://mqtt.org/">MQTT</a> protocol.
 */
public class MQTTWireFormat implements WireFormat {

    static final int MAX_MESSAGE_LENGTH = 1024 * 1024 * 256;
    static final long DEFAULT_CONNECTION_TIMEOUT = 30000L;

    private int version = 1;

    private int maxFrameSize = MAX_MESSAGE_LENGTH;
    private long connectAttemptTimeout = MQTTWireFormat.DEFAULT_CONNECTION_TIMEOUT;

    @Override
    public ByteSequence marshal(Object command) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        marshal(command, dos);
        dos.close();
        return baos.toByteSequence();
    }

    @Override
    public Object unmarshal(ByteSequence packet) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(packet);
        DataInputStream dis = new DataInputStream(stream);
        return unmarshal(dis);
    }

    @Override
    public void marshal(Object command, DataOutput dataOut) throws IOException {
        MQTTFrame frame = (MQTTFrame) command;
        dataOut.write(frame.header());

        int remaining = 0;
        for (Buffer buffer : frame.buffers) {
            remaining += buffer.length;
        }
        do {
            byte digit = (byte) (remaining & 0x7F);
            remaining >>>= 7;
            if (remaining > 0) {
                digit |= 0x80;
            }
            dataOut.write(digit);
        } while (remaining > 0);
        for (Buffer buffer : frame.buffers) {
            dataOut.write(buffer.data, buffer.offset, buffer.length);
        }
    }

    @Override
    public Object unmarshal(DataInput dataIn) throws IOException {
        byte header = dataIn.readByte();

        byte digit;
        int multiplier = 1;
        int length = 0;
        do {
            digit = dataIn.readByte();
            length += (digit & 0x7F) * multiplier;
            multiplier <<= 7;
        }
        while ((digit & 0x80) != 0);

        if (length >= 0) {
            if (length > getMaxFrameSize()) {
                throw new IOException("The maximum message length was exceeded");
            }

            if (length > 0) {
                byte[] data = new byte[length];
                dataIn.readFully(data);
                Buffer body = new Buffer(data);
                return new MQTTFrame(body).header(header);
            } else {
                return new MQTTFrame().header(header);
            }
        }
        return null;
    }

    /**
     * @param version the version of the wire format
     */
    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * @return the version of the wire format
     */
    @Override
    public int getVersion() {
        return this.version;
    }

    /**
     * @return the maximum number of bytes a single MQTT message frame is allowed to be.
     */
    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    /**
     * Sets the maximum frame size for an incoming MQTT frame.  The protocl limit is
     * 256 megabytes and this value cannot be set higher.
     *
     * @param maxFrameSize
     *        the maximum allowed frame size for a single MQTT frame.
     */
    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = Math.min(MAX_MESSAGE_LENGTH, maxFrameSize);
    }

    /**
     * @return the timeout value used to fail a connection if no CONNECT frame read.
     */
    public long getConnectAttemptTimeout() {
        return connectAttemptTimeout;
    }

    /**
     * Sets the timeout value used to fail a connection if no CONNECT frame is read
     * in the given interval.
     *
     * @param connectTimeout
     *        the connection frame received timeout value.
     */
    public void setConnectAttemptTimeout(long connectTimeout) {
        this.connectAttemptTimeout = connectTimeout;
    }
}
