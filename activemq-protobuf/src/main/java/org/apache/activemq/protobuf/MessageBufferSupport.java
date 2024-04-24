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
package org.apache.activemq.protobuf;

import static org.apache.activemq.protobuf.WireFormat.WIRETYPE_LENGTH_DELIMITED;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;


final public class MessageBufferSupport {

    public static final String FORZEN_ERROR_MESSAGE = "Modification not allowed after object has been fozen.  Try modifying a copy of this object.";
    
    static public Buffer toUnframedBuffer(MessageBuffer message) {
        try {
            int size = message.serializedSizeUnframed();
            BufferOutputStream baos = new BufferOutputStream(size);
            CodedOutputStream output = new CodedOutputStream(baos);
            message.writeUnframed(output);
            Buffer rc = baos.toBuffer();
            assert rc.length == size : "Did not write as much data as expected.";
            return rc;
        } catch (IOException e) {
            throw new RuntimeException("Serializing to a byte array threw an IOException " + "(should never happen).", e);
        }
    }

    static public Buffer toFramedBuffer(MessageBuffer message) {
        try {
            int size = message.serializedSizeFramed();
            BufferOutputStream baos = new BufferOutputStream(size);
            CodedOutputStream output = new CodedOutputStream(baos);
            message.writeFramed(output);
            Buffer rc = baos.toBuffer();
            assert rc.length==size : "Did not write as much data as expected.";
            return rc;
        } catch (IOException e) {
            throw new RuntimeException("Serializing to a byte array threw an IOException " + "(should never happen).", e);
        }
    }
    
    public static void writeMessage(CodedOutputStream output, int tag, MessageBuffer message) throws IOException {
        output.writeTag(tag, WIRETYPE_LENGTH_DELIMITED);
        message.writeFramed(output);
    }

    public static int computeMessageSize(int tag, MessageBuffer message) {
        return CodedOutputStream.computeTagSize(tag) + message.serializedSizeFramed();
    }

    public static Buffer readFrame(java.io.InputStream input) throws IOException {
        int length = readRawVarint32(input);
        byte[] data = new byte[length];
        int pos = 0;
        while (pos < length) {
            int r = input.read(data, pos, length - pos);
            if (r < 0) {
                throw new InvalidProtocolBufferException("Input stream ended before a full message frame could be read.");
            }
            pos += r;
        }
        return new Buffer(data);
    }
    
    /**
     * Read a raw Varint from the stream. If larger than 32 bits, discard the
     * upper bits.
     */
    static public int readRawVarint32(InputStream is) throws IOException {
        byte tmp = readRawByte(is);
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = readRawByte(is)) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = readRawByte(is)) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = readRawByte(is)) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = readRawByte(is)) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (readRawByte(is) >= 0)
                                return result;
                        }
                        throw new InvalidProtocolBufferException("CodedInputStream encountered a malformed varint.");
                    }
                }
            }
        }
        return result;
    }
    
    static public byte readRawByte(InputStream is) throws IOException {
        int rc = is.read();
        if (rc == -1) {
            throw new InvalidProtocolBufferException("While parsing a protocol message, the input ended unexpectedly " + "in the middle of a field.  This could mean either than the " + "input has been truncated or that an embedded message "
                    + "misreported its own length.");
        }
        return (byte) rc;
    }
    
    static public <T> void addAll(Iterable<T> values, Collection<? super T> list) {
        if (values instanceof Collection) {
            @SuppressWarnings("unsafe")
            Collection<T> collection = (Collection<T>) values;
            list.addAll(collection);
        } else {
            for (T value : values) {
                list.add(value);
            }
        }
    }


}
