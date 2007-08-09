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
package org.apache.activemq.openwire;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

final public class BooleanStream {

    byte data[] = new byte[48];
    short arrayLimit;
    short arrayPos;
    byte bytePos;

    public boolean readBoolean() throws IOException {
        assert arrayPos <= arrayLimit;
        byte b = data[arrayPos];
        boolean rc = ((b >> bytePos) & 0x01) != 0;
        bytePos++;
        if (bytePos >= 8) {
            bytePos = 0;
            arrayPos++;
        }
        return rc;
    }

    public void writeBoolean(boolean value) throws IOException {
        if (bytePos == 0) {
            arrayLimit++;
            if (arrayLimit >= data.length) {
                // re-grow the array.
                byte d[] = new byte[data.length * 2];
                System.arraycopy(data, 0, d, 0, data.length);
                data = d;
            }
        }
        if (value) {
            data[arrayPos] |= (0x01 << bytePos);
        }
        bytePos++;
        if (bytePos >= 8) {
            bytePos = 0;
            arrayPos++;
        }
    }

    public void marshal(DataOutput dataOut) throws IOException {
        if (arrayLimit < 64) {
            dataOut.writeByte(arrayLimit);
        } else if (arrayLimit < 256) { // max value of unsigned byte
            dataOut.writeByte(0xC0);
            dataOut.writeByte(arrayLimit);
        } else {
            dataOut.writeByte(0x80);
            dataOut.writeShort(arrayLimit);
        }

        dataOut.write(data, 0, arrayLimit);
        clear();
    }

    public void marshal(ByteBuffer dataOut) {
        if (arrayLimit < 64) {
            dataOut.put((byte)arrayLimit);
        } else if (arrayLimit < 256) { // max value of unsigned byte
            dataOut.put((byte)0xC0);
            dataOut.put((byte)arrayLimit);
        } else {
            dataOut.put((byte)0x80);
            dataOut.putShort(arrayLimit);
        }

        dataOut.put(data, 0, arrayLimit);
    }

    public void unmarshal(DataInput dataIn) throws IOException {

        arrayLimit = (short)(dataIn.readByte() & 0xFF);
        if (arrayLimit == 0xC0) {
            arrayLimit = (short)(dataIn.readByte() & 0xFF);
        } else if (arrayLimit == 0x80) {
            arrayLimit = dataIn.readShort();
        }
        if (data.length < arrayLimit) {
            data = new byte[arrayLimit];
        }
        dataIn.readFully(data, 0, arrayLimit);
        clear();
    }

    public void clear() {
        arrayPos = 0;
        bytePos = 0;
    }

    public int marshalledSize() {
        if (arrayLimit < 64) {
            return 1 + arrayLimit;
        } else if (arrayLimit < 256) {
            return 2 + arrayLimit;
        } else {
            return 3 + arrayLimit;
        }
    }

}
