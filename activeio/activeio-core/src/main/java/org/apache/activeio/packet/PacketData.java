/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.packet;

import java.io.EOFException;
import java.io.IOException;


/**
 * Used to write and read primitives to and from a Packet.
 */
final public class PacketData {

    final private Packet packet;
    private final boolean bigEndian;

    public PacketData(Packet packet) {
        this(packet, true);
    }

    public PacketData(Packet packet, boolean bigEndian) {
        this.packet = packet;
        this.bigEndian = bigEndian;
    }

    private static void spaceNeeded(Packet packet, int space) throws IOException {
        if (packet.remaining() < space)
            throw new EOFException("Not enough space left in the packet.");
    }

    public void readFully(byte[] b) throws IOException {
        readFully(packet, b, 0, b.length);
    }
    
    public static void readFully(Packet packet, byte[] b) throws IOException {
        readFully(packet, b, 0, b.length);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        readFully(packet, b, off, len);
    }
    public static void readFully(Packet packet, byte[] b, int off, int len) throws IOException {
        spaceNeeded(packet, len);
        packet.read(b, off, len);
    }

    public int skipBytes(int n) throws IOException {
        return skipBytes(packet, n);
    }
    public static int skipBytes(Packet packet, int n) throws IOException {
        int rc = Math.min(n, packet.remaining());
        packet.position(packet.position() + rc);
        return rc;
    }

    public boolean readBoolean() throws IOException {
        return readBoolean(packet);
    }
    public static boolean readBoolean(Packet packet) throws IOException {
        spaceNeeded(packet, 1);
        return packet.read() != 0;
    }

    public byte readByte() throws IOException {
        return readByte(packet);
    }
    public static byte readByte(Packet packet) throws IOException {
        spaceNeeded(packet, 1);
        return (byte) packet.read();
    }

    public int readUnsignedByte() throws IOException {
        return readUnsignedByte(packet);
    }
    public static int readUnsignedByte(Packet packet) throws IOException {
        spaceNeeded(packet, 1);
        return packet.read();
    }

    public short readShort() throws IOException {
        if( bigEndian ) {
            return readShortBig(packet);
        } else {
	        return readShortLittle(packet);
        }        
    }
    public static short readShortBig(Packet packet) throws IOException {
        spaceNeeded(packet, 2);
        return (short) ((packet.read() << 8) + (packet.read() << 0));
    }
    public static short readShortLittle(Packet packet) throws IOException {
        spaceNeeded(packet, 2);
        return (short) ((packet.read() << 0) + (packet.read() << 8) );
    }

    public int readUnsignedShort() throws IOException {
        if( bigEndian ) {
            return readUnsignedShortBig(packet);
        } else {
	        return readUnsignedShortLittle(packet);
        }        
    }
    public static int readUnsignedShortBig(Packet packet) throws IOException {
        spaceNeeded(packet, 2);
        return ((packet.read() << 8) + (packet.read() << 0));
    }
    public static int readUnsignedShortLittle(Packet packet) throws IOException {
        spaceNeeded(packet, 2);
        return ((packet.read() << 0) + (packet.read() << 8) );
    }

    public char readChar() throws IOException {
        if( bigEndian ) {
            return readCharBig(packet);
        } else {
	        return readCharLittle(packet);
        }        
    }
    public static char readCharBig(Packet packet) throws IOException {
        spaceNeeded(packet, 2);
        return (char) ((packet.read() << 8) + (packet.read() << 0));
    }
    public static char readCharLittle(Packet packet) throws IOException {
        spaceNeeded(packet, 2);
        return (char) ((packet.read() << 0) + (packet.read() << 8) );
    }

    public int readInt() throws IOException {
        if( bigEndian ) {
	        return readIntBig(packet);
        } else {
	        return readIntLittle(packet);
        }        
    }    
    public static int readIntBig(Packet packet) throws IOException {
        spaceNeeded(packet, 4);
        return ((packet.read() << 24) + 
                (packet.read() << 16) + 
                (packet.read() << 8) + 
                (packet.read() << 0));
    }    
    public static int readIntLittle(Packet packet) throws IOException {
        spaceNeeded(packet, 4);
        return ((packet.read() << 0) +
                (packet.read() << 8) + 
                (packet.read() << 16) + 
                (packet.read() << 24));
    }    
    
    public long readLong() throws IOException {
        if( bigEndian ) {
	        return readLongBig(packet);
        } else {
	        return readLongLittle(packet);	                
        }        
    }
    public static long readLongBig(Packet packet) throws IOException {
        spaceNeeded(packet, 8);
        return (((long) packet.read() << 56) + 
                ((long) packet.read() << 48) + 
                ((long) packet.read() << 40) + 
                ((long) packet.read() << 32) + 
                ((long) packet.read() << 24) + 
                ((packet.read()) << 16) + 
                ((packet.read()) << 8) + 
                ((packet.read()) << 0));
    }
    public static long readLongLittle(Packet packet) throws IOException {
        spaceNeeded(packet, 8);
        return ((packet.read() << 0) +
                (packet.read() << 8) + 
                (packet.read() << 16) + 
                ((long) packet.read() << 24) +
                ((long) packet.read() << 32) + 
                ((long) packet.read() << 40) + 
                ((long) packet.read() << 48) + 
                ((long) packet.read() << 56));                  
    }
    
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }
    public static double readDoubleBig(Packet packet) throws IOException {
        return Double.longBitsToDouble(readLongBig(packet));
    }
    public static double readDoubleLittle(Packet packet) throws IOException {
        return Double.longBitsToDouble(readLongLittle(packet));
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }
    public static float readFloatBig(Packet packet) throws IOException {
        return Float.intBitsToFloat(readIntBig(packet));
    }
    public static float readFloatLittle(Packet packet) throws IOException {
        return Float.intBitsToFloat(readIntLittle(packet));
    }

    public void write(int b) throws IOException {
        write(packet, b);
    }
    public static void write(Packet packet, int b) throws IOException {
        spaceNeeded(packet, 1);
        packet.write(b);
    }

    public void write(byte[] b) throws IOException {
        write(packet, b, 0, b.length);
    }
    public static void write(Packet packet, byte[] b) throws IOException {
        write(packet, b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        write(packet, b, off, len);
    }
    public static void write(Packet packet, byte[] b, int off, int len) throws IOException {
        spaceNeeded(packet, len);
        packet.write(b, off, len);
    }

    public void writeBoolean(boolean v) throws IOException {
        writeBoolean(packet, v);
    }
    public static void writeBoolean(Packet packet, boolean v) throws IOException {
        spaceNeeded(packet, 1);
        packet.write(v ? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        writeByte(packet, v);
    }
    public static void writeByte(Packet packet, int v) throws IOException {
        spaceNeeded(packet, 1);
        packet.write(v);
    }

    public void writeShort(int v) throws IOException {
        if (bigEndian) {
	        writeShortBig(packet,v);
	    } else {
            writeShortLittle(packet,v);
	    }
    }
    public static void writeShortBig(Packet packet, int v) throws IOException {
        spaceNeeded(packet, 2);
        packet.write((v >>> 8) & 0xFF);
        packet.write((v >>> 0) & 0xFF);
    }
    public static void writeShortLittle(Packet packet, int v) throws IOException {
        spaceNeeded(packet, 2);
        packet.write((v >>> 0) & 0xFF);
        packet.write((v >>> 8) & 0xFF);
    }

    public void writeChar(int v) throws IOException {
        if (bigEndian) {
            writeCharBig(packet, v);
        } else {
            writeCharLittle(packet, v);
        }
    }
    public static void writeCharBig(Packet packet, int v) throws IOException {
        spaceNeeded(packet, 2);
        packet.write((v >>> 8) & 0xFF);
        packet.write((v >>> 0) & 0xFF);
    }
    public static void writeCharLittle(Packet packet, int v) throws IOException {
        spaceNeeded(packet, 2);
        packet.write((v >>> 0) & 0xFF);
        packet.write((v >>> 8) & 0xFF);
    }

    public void writeInt(int v) throws IOException {
        if (bigEndian) {
            writeIntBig(packet, v);
        } else {
            writeIntLittle(packet, v);
        }
    }
    public static void writeIntBig(Packet packet, int v) throws IOException {
        spaceNeeded(packet, 4);
        packet.write((v >>> 24) & 0xFF);
        packet.write((v >>> 16) & 0xFF);
        packet.write((v >>> 8) & 0xFF);
        packet.write((v >>> 0) & 0xFF);
    }
    public static void writeIntLittle(Packet packet, int v) throws IOException {
        spaceNeeded(packet, 4);
        packet.write((v >>> 0) & 0xFF);
        packet.write((v >>> 8) & 0xFF);
        packet.write((v >>> 16) & 0xFF);
        packet.write((v >>> 24) & 0xFF);
    }

    public void writeLong(long v) throws IOException {
        if (bigEndian) {
            writeLongBig(packet, v);
        } else {
            writeLongLittle(packet, v);
        }
    }
    public static void writeLongBig(Packet packet, long v) throws IOException {
        spaceNeeded(packet, 8);
        packet.write((int) (v >>> 56) & 0xFF);
        packet.write((int) (v >>> 48) & 0xFF);
        packet.write((int) (v >>> 40) & 0xFF);
        packet.write((int) (v >>> 32) & 0xFF);
        packet.write((int) (v >>> 24) & 0xFF);
        packet.write((int) (v >>> 16) & 0xFF);
        packet.write((int) (v >>> 8) & 0xFF);
        packet.write((int) (v >>> 0) & 0xFF);
    }
    public static void writeLongLittle(Packet packet, long v) throws IOException {
        spaceNeeded(packet, 8);
        packet.write((int) (v >>> 0) & 0xFF);
        packet.write((int) (v >>> 8) & 0xFF);
        packet.write((int) (v >>> 16) & 0xFF);
        packet.write((int) (v >>> 24) & 0xFF);
        packet.write((int) (v >>> 32) & 0xFF);
        packet.write((int) (v >>> 40) & 0xFF);
        packet.write((int) (v >>> 48) & 0xFF);
        packet.write((int) (v >>> 56) & 0xFF);
    }
    
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }
    public static void writeDoubleBig(Packet packet, double v) throws IOException {
        writeLongBig(packet, Double.doubleToLongBits(v));
    }
    public static void writeDoubleLittle(Packet packet, double v) throws IOException {
        writeLongLittle(packet, Double.doubleToLongBits(v));
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }
    public static void writeFloatBig(Packet packet, float v) throws IOException {
        writeIntBig(packet, Float.floatToIntBits(v));
    }
    public static void writeFloatLittle(Packet packet, float v) throws IOException {
        writeIntLittle(packet, Float.floatToIntBits(v));
    }
    
    public void writeRawDouble(double v) throws IOException {
        writeLong(Double.doubleToRawLongBits(v));
    }
    public static void writeRawDoubleBig(Packet packet, double v) throws IOException {
        writeLongBig(packet, Double.doubleToRawLongBits(v));
    }
    public static void writeRawDoubleLittle(Packet packet, double v) throws IOException {
        writeLongLittle(packet, Double.doubleToRawLongBits(v));
    }

    public void writeRawFloat(float v) throws IOException {
        writeInt(Float.floatToRawIntBits(v));
    }
    public static void writeRawFloatBig(Packet packet, float v) throws IOException {
        writeIntBig(packet, Float.floatToRawIntBits(v));
    }
    public static void writeRawFloatLittle(Packet packet, float v) throws IOException {
        writeIntLittle(packet, Float.floatToRawIntBits(v));
    }

}
