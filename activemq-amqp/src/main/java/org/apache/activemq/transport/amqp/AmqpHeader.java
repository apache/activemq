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
package org.apache.activemq.transport.amqp;

import org.fusesource.hawtbuf.Buffer;

/**
 * Represents the AMQP protocol handshake packet that is sent during the
 * initial exchange with a remote peer.
 */
public class AmqpHeader {

    static final Buffer PREFIX = new Buffer(new byte[] { 'A', 'M', 'Q', 'P' });

    private Buffer buffer;

    public AmqpHeader() {
        this(new Buffer(new byte[] { 'A', 'M', 'Q', 'P', 0, 1, 0, 0 }));
    }

    public AmqpHeader(Buffer buffer) {
        this(buffer, true);
    }

    public AmqpHeader(Buffer buffer, boolean validate) {
        setBuffer(buffer, validate);
    }

    public int getProtocolId() {
        return buffer.get(4) & 0xFF;
    }

    public void setProtocolId(int value) {
        buffer.data[buffer.offset + 4] = (byte) value;
    }

    public int getMajor() {
        return buffer.get(5) & 0xFF;
    }

    public void setMajor(int value) {
        buffer.data[buffer.offset + 5] = (byte) value;
    }

    public int getMinor() {
        return buffer.get(6) & 0xFF;
    }

    public void setMinor(int value) {
        buffer.data[buffer.offset + 6] = (byte) value;
    }

    public int getRevision() {
        return buffer.get(7) & 0xFF;
    }

    public void setRevision(int value) {
        buffer.data[buffer.offset + 7] = (byte) value;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public void setBuffer(Buffer value) {
        setBuffer(value, true);
    }

    public void setBuffer(Buffer value, boolean validate) {
        if (validate && !value.startsWith(PREFIX) || value.length() != 8) {
            throw new IllegalArgumentException("Not an AMQP header buffer");
        }
        buffer = value.buffer();
    }

    public boolean hasValidPrefix() {
        return buffer.startsWith(PREFIX);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < buffer.length(); ++i) {
            char value = (char) buffer.get(i);
            if (Character.isLetter(value)) {
                builder.append(value);
            } else {
                builder.append(",");
                builder.append((int) value);
            }
        }
        return builder.toString();
    }
}
