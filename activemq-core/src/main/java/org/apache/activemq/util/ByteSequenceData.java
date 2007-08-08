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

import java.io.IOException;

/**
 * Used to write and read primitives to and from a ByteSequence.
 */
final public class ByteSequenceData {

    public static byte[] toByteArray(ByteSequence packet) {
        if (packet.offset == 0 && packet.length == packet.data.length)
            return packet.data;

        byte rc[] = new byte[packet.length];
        System.arraycopy(packet.data, packet.offset, rc, 0, packet.length);
        return rc;
    }

    private static void spaceNeeded(ByteSequence packet, int i) {
        assert packet.offset + i <= packet.length;
    }

    public static int remaining(ByteSequence packet) {
        return packet.length - packet.offset;
    }

    public static int read(ByteSequence packet) {
        return packet.data[packet.offset++] & 0xff;
    }

    public static void readFully(ByteSequence packet, byte[] b) throws IOException {
        readFully(packet, b, 0, b.length);
    }

    public static void readFully(ByteSequence packet, byte[] b, int off, int len) throws IOException {
        spaceNeeded(packet, len);
        System.arraycopy(packet.data, packet.offset, b, off, len);
        packet.offset += len;
    }

    public static int skipBytes(ByteSequence packet, int n) throws IOException {
        int rc = Math.min(n, remaining(packet));
        packet.offset += rc;
        return rc;
    }

    public static boolean readBoolean(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 1);
        return read(packet) != 0;
    }

    public static byte readByte(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 1);
        return (byte)read(packet);
    }

    public static int readUnsignedByte(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 1);
        return read(packet);
    }

    public static short readShortBig(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 2);
        return (short)((read(packet) << 8) + (read(packet) << 0));
    }

    public static short readShortLittle(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 2);
        return (short)((read(packet) << 0) + (read(packet) << 8));
    }

    public static int readUnsignedShortBig(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 2);
        return ((read(packet) << 8) + (read(packet) << 0));
    }

    public static int readUnsignedShortLittle(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 2);
        return ((read(packet) << 0) + (read(packet) << 8));
    }

    public static char readCharBig(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 2);
        return (char)((read(packet) << 8) + (read(packet) << 0));
    }

    public static char readCharLittle(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 2);
        return (char)((read(packet) << 0) + (read(packet) << 8));
    }

    public static int readIntBig(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 4);
        return ((read(packet) << 24) + (read(packet) << 16) + (read(packet) << 8) + (read(packet) << 0));
    }

    public static int readIntLittle(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 4);
        return ((read(packet) << 0) + (read(packet) << 8) + (read(packet) << 16) + (read(packet) << 24));
    }

    public static long readLongBig(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 8);
        return (((long)read(packet) << 56) + ((long)read(packet) << 48) + ((long)read(packet) << 40) + ((long)read(packet) << 32) + ((long)read(packet) << 24)
                + ((read(packet)) << 16) + ((read(packet)) << 8) + ((read(packet)) << 0));
    }

    public static long readLongLittle(ByteSequence packet) throws IOException {
        spaceNeeded(packet, 8);
        return ((read(packet) << 0) + (read(packet) << 8) + (read(packet) << 16) + ((long)read(packet) << 24) + ((long)read(packet) << 32) + ((long)read(packet) << 40)
                + ((long)read(packet) << 48) + ((long)read(packet) << 56));
    }

    public static double readDoubleBig(ByteSequence packet) throws IOException {
        return Double.longBitsToDouble(readLongBig(packet));
    }

    public static double readDoubleLittle(ByteSequence packet) throws IOException {
        return Double.longBitsToDouble(readLongLittle(packet));
    }

    public static float readFloatBig(ByteSequence packet) throws IOException {
        return Float.intBitsToFloat(readIntBig(packet));
    }

    public static float readFloatLittle(ByteSequence packet) throws IOException {
        return Float.intBitsToFloat(readIntLittle(packet));
    }

    public static void write(ByteSequence packet, int b) throws IOException {
        spaceNeeded(packet, 1);
        packet.data[packet.offset++] = (byte)b;
    }

    public static void write(ByteSequence packet, byte[] b) throws IOException {
        write(packet, b, 0, b.length);
    }

    public static void write(ByteSequence packet, byte[] b, int off, int len) throws IOException {
        spaceNeeded(packet, len);
        System.arraycopy(b, off, packet.data, packet.offset, len);
        packet.offset += len;
    }

    public static void writeBoolean(ByteSequence packet, boolean v) throws IOException {
        spaceNeeded(packet, 1);
        write(packet, v ? 1 : 0);
    }

    public static void writeByte(ByteSequence packet, int v) throws IOException {
        spaceNeeded(packet, 1);
        write(packet, v);
    }

    public static void writeShortBig(ByteSequence packet, int v) throws IOException {
        spaceNeeded(packet, 2);
        write(packet, (v >>> 8) & 0xFF);
        write(packet, (v >>> 0) & 0xFF);
    }

    public static void writeShortLittle(ByteSequence packet, int v) throws IOException {
        spaceNeeded(packet, 2);
        write(packet, (v >>> 0) & 0xFF);
        write(packet, (v >>> 8) & 0xFF);
    }

    public static void writeCharBig(ByteSequence packet, int v) throws IOException {
        spaceNeeded(packet, 2);
        write(packet, (v >>> 8) & 0xFF);
        write(packet, (v >>> 0) & 0xFF);
    }

    public static void writeCharLittle(ByteSequence packet, int v) throws IOException {
        spaceNeeded(packet, 2);
        write(packet, (v >>> 0) & 0xFF);
        write(packet, (v >>> 8) & 0xFF);
    }

    public static void writeIntBig(ByteSequence packet, int v) throws IOException {
        spaceNeeded(packet, 4);
        write(packet, (v >>> 24) & 0xFF);
        write(packet, (v >>> 16) & 0xFF);
        write(packet, (v >>> 8) & 0xFF);
        write(packet, (v >>> 0) & 0xFF);
    }

    public static void writeIntLittle(ByteSequence packet, int v) throws IOException {
        spaceNeeded(packet, 4);
        write(packet, (v >>> 0) & 0xFF);
        write(packet, (v >>> 8) & 0xFF);
        write(packet, (v >>> 16) & 0xFF);
        write(packet, (v >>> 24) & 0xFF);
    }

    public static void writeLongBig(ByteSequence packet, long v) throws IOException {
        spaceNeeded(packet, 8);
        write(packet, (int)(v >>> 56) & 0xFF);
        write(packet, (int)(v >>> 48) & 0xFF);
        write(packet, (int)(v >>> 40) & 0xFF);
        write(packet, (int)(v >>> 32) & 0xFF);
        write(packet, (int)(v >>> 24) & 0xFF);
        write(packet, (int)(v >>> 16) & 0xFF);
        write(packet, (int)(v >>> 8) & 0xFF);
        write(packet, (int)(v >>> 0) & 0xFF);
    }

    public static void writeLongLittle(ByteSequence packet, long v) throws IOException {
        spaceNeeded(packet, 8);
        write(packet, (int)(v >>> 0) & 0xFF);
        write(packet, (int)(v >>> 8) & 0xFF);
        write(packet, (int)(v >>> 16) & 0xFF);
        write(packet, (int)(v >>> 24) & 0xFF);
        write(packet, (int)(v >>> 32) & 0xFF);
        write(packet, (int)(v >>> 40) & 0xFF);
        write(packet, (int)(v >>> 48) & 0xFF);
        write(packet, (int)(v >>> 56) & 0xFF);
    }

    public static void writeDoubleBig(ByteSequence packet, double v) throws IOException {
        writeLongBig(packet, Double.doubleToLongBits(v));
    }

    public static void writeDoubleLittle(ByteSequence packet, double v) throws IOException {
        writeLongLittle(packet, Double.doubleToLongBits(v));
    }

    public static void writeFloatBig(ByteSequence packet, float v) throws IOException {
        writeIntBig(packet, Float.floatToIntBits(v));
    }

    public static void writeFloatLittle(ByteSequence packet, float v) throws IOException {
        writeIntLittle(packet, Float.floatToIntBits(v));
    }

    public static void writeRawDoubleBig(ByteSequence packet, double v) throws IOException {
        writeLongBig(packet, Double.doubleToRawLongBits(v));
    }

    public static void writeRawDoubleLittle(ByteSequence packet, double v) throws IOException {
        writeLongLittle(packet, Double.doubleToRawLongBits(v));
    }

    public static void writeRawFloatBig(ByteSequence packet, float v) throws IOException {
        writeIntBig(packet, Float.floatToRawIntBits(v));
    }

    public static void writeRawFloatLittle(ByteSequence packet, float v) throws IOException {
        writeIntLittle(packet, Float.floatToRawIntBits(v));
    }

}
