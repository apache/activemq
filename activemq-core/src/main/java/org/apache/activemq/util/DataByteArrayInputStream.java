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
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;

/**
 * Optimized ByteArrayInputStream that can be used more than once
 * 
 * @version $Revision: 1.1.1.1 $
 */
public final class DataByteArrayInputStream extends InputStream implements DataInput {
    private byte[] buf;
    private int pos;
    private int offset;

    /**
     * Creates a <code>StoreByteArrayInputStream</code>.
     * 
     * @param buf the input buffer.
     */
    public DataByteArrayInputStream(byte buf[]) {
        this.buf = buf;
        this.pos = 0;
        this.offset = 0;
    }

    /**
     * Creates a <code>StoreByteArrayInputStream</code>.
     * 
     * @param sequence the input buffer.
     */
    public DataByteArrayInputStream(ByteSequence sequence) {
        this.buf = sequence.getData();
        this.offset = this.pos = sequence.getOffset();
    }

    /**
     * Creates <code>WireByteArrayInputStream</code> with a minmalist byte
     * array
     */
    public DataByteArrayInputStream() {
        this(new byte[0]);
    }

    /**
     * @return the size
     */
    public int size() {
        return pos - offset;
    }

    /**
     * @return the underlying data array
     */
    public byte[] getRawData() {
        return buf;
    }

    /**
     * reset the <code>StoreByteArrayInputStream</code> to use an new byte
     * array
     * 
     * @param newBuff
     */
    public void restart(byte[] newBuff) {
        buf = newBuff;
        pos = 0;
    }

    /**
     * reset the <code>StoreByteArrayInputStream</code> to use an new
     * ByteSequence
     * 
     * @param sequence
     */
    public void restart(ByteSequence sequence) {
        this.buf = sequence.getData();
        this.pos = sequence.getOffset();
    }

    /**
     * re-start the input stream - reusing the current buffer
     * 
     * @param size
     */
    public void restart(int size) {
        if (buf == null || buf.length < size) {
            buf = new byte[size];
        }
        restart(buf);
    }

    /**
     * Reads the next byte of data from this input stream. The value byte is
     * returned as an <code>int</code> in the range <code>0</code> to
     * <code>255</code>. If no byte is available because the end of the
     * stream has been reached, the value <code>-1</code> is returned.
     * <p>
     * This <code>read</code> method cannot block.
     * 
     * @return the next byte of data, or <code>-1</code> if the end of the
     *         stream has been reached.
     */
    public int read() {
        return (pos < buf.length) ? (buf[pos++] & 0xff) : -1;
    }

    /**
     * Reads up to <code>len</code> bytes of data into an array of bytes from
     * this input stream.
     * 
     * @param b the buffer into which the data is read.
     * @param off the start offset of the data.
     * @param len the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or
     *         <code>-1</code> if there is no more data because the end of the
     *         stream has been reached.
     */
    public int read(byte b[], int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        }
        if (pos >= buf.length) {
            return -1;
        }
        if (pos + len > buf.length) {
            len = buf.length - pos;
        }
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    /**
     * @return the number of bytes that can be read from the input stream
     *         without blocking.
     */
    public int available() {
        return buf.length - pos;
    }

    public void readFully(byte[] b) {
        read(b, 0, b.length);
    }

    public void readFully(byte[] b, int off, int len) {
        read(b, off, len);
    }

    public int skipBytes(int n) {
        if (pos + n > buf.length) {
            n = buf.length - pos;
        }
        if (n < 0) {
            return 0;
        }
        pos += n;
        return n;
    }

    public boolean readBoolean() {
        return read() != 0;
    }

    public byte readByte() {
        return (byte)read();
    }

    public int readUnsignedByte() {
        return read();
    }

    public short readShort() {
        int ch1 = read();
        int ch2 = read();
        return (short)((ch1 << 8) + (ch2 << 0));
    }

    public int readUnsignedShort() {
        int ch1 = read();
        int ch2 = read();
        return ((ch1 << 8) + (ch2 << 0));
    }

    public char readChar() {
        int ch1 = read();
        int ch2 = read();
        return (char)((ch1 << 8) + (ch2 << 0));
    }

    public int readInt() {
        int ch1 = read();
        int ch2 = read();
        int ch3 = read();
        int ch4 = read();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public long readLong() {
        long rc = ((long)buf[pos++] << 56) + ((long)(buf[pos++] & 255) << 48) + ((long)(buf[pos++] & 255) << 40) + ((long)(buf[pos++] & 255) << 32);
        return rc + ((long)(buf[pos++] & 255) << 24) + ((buf[pos++] & 255) << 16) + ((buf[pos++] & 255) << 8) + ((buf[pos++] & 255) << 0);
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public String readLine() {
        int start = pos;
        while (pos < buf.length) {
            int c = read();
            if (c == '\n') {
                break;
            }
            if (c == '\r') {
                c = read();
                if (c != '\n' && c != -1) {
                    pos--;
                }
                break;
            }
        }
        return new String(buf, start, pos);
    }

    public String readUTF() throws IOException {
        int length = readUnsignedShort();
        char[] characters = new char[length];
        int c, c2, c3;
        int count = 0;
        int total = pos + length;
        while (pos < total) {
            c = (int)buf[pos] & 0xff;
            if (c > 127) {
                break;
            }
            pos++;
            characters[count++] = (char)c;
        }
        while (pos < total) {
            c = (int)buf[pos] & 0xff;
            switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                pos++;
                characters[count++] = (char)c;
                break;
            case 12:
            case 13:
                pos += 2;
                if (pos > length) {
                    throw new UTFDataFormatException("bad string");
                }
                c2 = (int)buf[pos - 1];
                if ((c2 & 0xC0) != 0x80) {
                    throw new UTFDataFormatException("bad string");
                }
                characters[count++] = (char)(((c & 0x1F) << 6) | (c2 & 0x3F));
                break;
            case 14:
                pos += 3;
                if (pos > length) {
                    throw new UTFDataFormatException("bad string");
                }
                c2 = (int)buf[pos - 2];
                c3 = (int)buf[pos - 1];
                if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80)) {
                    throw new UTFDataFormatException("bad string");
                }
                characters[count++] = (char)(((c & 0x0F) << 12) | ((c2 & 0x3F) << 6) | ((c3 & 0x3F) << 0));
                break;
            default:
                throw new UTFDataFormatException("bad string");
            }
        }
        return new String(characters, 0, count);
    }
}
