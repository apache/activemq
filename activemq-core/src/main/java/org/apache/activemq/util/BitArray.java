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
package org.apache.activemq.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Simple BitArray to enable setting multiple boolean values efficently Used instead of BitSet because BitSet does not
 * allow for efficent serialization.
 * Will store up to 64 boolean values
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class BitArray {
    static final int LONG_SIZE = 64;
    static final int INT_SIZE = 32;
    static final int SHORT_SIZE = 16;
    static final int BYTE_SIZE = 8;
    private static final long[] BIT_VALUES = {0x0000000000000001L, 0x0000000000000002L, 0x0000000000000004L,
                                              0x0000000000000008L, 0x0000000000000010L, 0x0000000000000020L, 0x0000000000000040L, 0x0000000000000080L,
                                              0x0000000000000100L, 0x0000000000000200L, 0x0000000000000400L, 0x0000000000000800L, 0x0000000000001000L,
                                              0x0000000000002000L, 0x0000000000004000L, 0x0000000000008000L, 0x0000000000010000L, 0x0000000000020000L,
                                              0x0000000000040000L, 0x0000000000080000L, 0x0000000000100000L, 0x0000000000200000L, 0x0000000000400000L,
                                              0x0000000000800000L, 0x0000000001000000L, 0x0000000002000000L, 0x0000000004000000L, 0x0000000008000000L,
                                              0x0000000010000000L, 0x0000000020000000L, 0x0000000040000000L, 0x0000000080000000L, 0x0000000100000000L,
                                              0x0000000200000000L, 0x0000000400000000L, 0x0000000800000000L, 0x0000001000000000L, 0x0000002000000000L,
                                              0x0000004000000000L, 0x0000008000000000L, 0x0000010000000000L, 0x0000020000000000L, 0x0000040000000000L,
                                              0x0000080000000000L, 0x0000100000000000L, 0x0000200000000000L, 0x0000400000000000L, 0x0000800000000000L,
                                              0x0001000000000000L, 0x0002000000000000L, 0x0004000000000000L, 0x0008000000000000L, 0x0010000000000000L,
                                              0x0020000000000000L, 0x0040000000000000L, 0x0080000000000000L, 0x0100000000000000L, 0x0200000000000000L,
                                              0x0400000000000000L, 0x0800000000000000L, 0x1000000000000000L, 0x2000000000000000L, 0x4000000000000000L,
                                              0x8000000000000000L};
    private long bits;
    private int length;

    /**
     * @return the length of bits set
     */
    public int length() {
        return length;
    }

    /**
     * @return the long containing the bits
     */
    public long getBits() {
        return bits;
    }

    /**
     * set the boolean value at the index
     *
     * @param index
     * @param flag
     * @return the old value held at this index
     */
    public boolean set(int index, boolean flag) {
        length = Math.max(length, index + 1);
        boolean oldValue = (bits & BIT_VALUES[index]) != 0;
        if (flag) {
            bits |= BIT_VALUES[index];
        }
        else if (oldValue) {
            bits &= ~(BIT_VALUES[index]);
        }
        return oldValue;
    }

    /**
     * @param index
     * @return the boolean value at this index
     */
    public boolean get(int index) {
        return (bits & BIT_VALUES[index]) != 0;
    }
    
    /**
     * reset all the bit values to false
     */
    public void reset(){
        bits = 0;
    }
    
    /**
     * reset all the bits to the value supplied
     * @param bits
     */
    public void reset(long bits){
        this.bits = bits;
    }

    /**
     * write the bits to an output stream
     *
     * @param dataOut
     * @throws IOException
     */
    public void writeToStream(DataOutput dataOut) throws IOException {
        dataOut.writeByte(length);
        if (length <= BYTE_SIZE) {
            dataOut.writeByte((int) bits);
        }
        else if (length <= SHORT_SIZE) {
            dataOut.writeShort((short) bits);
        }
        else if (length <= INT_SIZE) {
            dataOut.writeInt((int) bits);
        }
        else {
            dataOut.writeLong(bits);
        }
    }

    /**
     * read the bits from an input stream
     *
     * @param dataIn
     * @throws IOException
     */
    public void readFromStream(DataInput dataIn) throws IOException {
        length = dataIn.readByte();
        if (length <= BYTE_SIZE) {
            bits = dataIn.readByte();
        }
        else if (length <= SHORT_SIZE) {
            bits = dataIn.readShort();
        }
        else if (length <= INT_SIZE) {
            bits=dataIn.readInt();
        }
        else {
            bits = dataIn.readLong();
        }
    }
}