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
package org.apache.kahadb.util;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;

/**
 * Optimized ByteArrayInputStream that can be used more than once
 *
 *
 */
public final class DataByteArrayInputStream extends InputStream implements DataInput {
    private byte[] buf;
    private int pos;
    private int offset;
    private int length;

    private byte[] work;

    /**
     * Creates a <code>StoreByteArrayInputStream</code>.
     *
     * @param buf the input buffer.
     */
    public DataByteArrayInputStream(byte buf[]) {
        this.buf = buf;
        this.pos = 0;
        this.offset = 0;
        this.length = buf.length;
        this.work = new byte[8];
    }

    /**
     * Creates a <code>StoreByteArrayInputStream</code>.
     *
     * @param sequence the input buffer.
     */
    public DataByteArrayInputStream(ByteSequence sequence) {
        this.buf = sequence.getData();
        this.offset = sequence.getOffset();
        this.pos =  this.offset;
        this.length = sequence.length;
        this.work = new byte[8];
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
        length = newBuff.length;
    }

    public void restart() {
        pos = 0;
        length = buf.length;
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
        this.length = sequence.getLength();
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
        this.length = size;
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
        return (pos < length) ? (buf[pos++] & 0xff) : -1;
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
        if (pos >= length) {
            return -1;
        }
        if (pos + len > length) {
            len = length - pos;
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
        return length - pos;
    }

    public void readFully(byte[] b) {
        read(b, 0, b.length);
    }

    public void readFully(byte[] b, int off, int len) {
        read(b, off, len);
    }

    public int skipBytes(int n) {
        if (pos + n > length) {
            n = length - pos;
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
        this.read(work, 0, 2);
        return (short) (((work[0] & 0xff) << 8) | (work[1] & 0xff));
    }

    public int readUnsignedShort() {
        this.read(work, 0, 2);
        return (int) (((work[0] & 0xff) << 8) | (work[1] & 0xff));
    }

    public char readChar() {
        this.read(work, 0, 2);
        return (char) (((work[0] & 0xff) << 8) | (work[1] & 0xff));
    }

    public int readInt() {
        this.read(work, 0, 4);
        return ((work[0] & 0xff) << 24) | ((work[1] & 0xff) << 16) |
               ((work[2] & 0xff) << 8) | (work[3] & 0xff);
    }

    public long readLong() {
        this.read(work, 0, 8);

        int i1 = ((work[0] & 0xff) << 24) | ((work[1] & 0xff) << 16) |
            ((work[2] & 0xff) << 8) | (work[3] & 0xff);
        int i2 = ((work[4] & 0xff) << 24) | ((work[5] & 0xff) << 16) |
            ((work[6] & 0xff) << 8) | (work[7] & 0xff);

        return ((i1 & 0xffffffffL) << 32) | (i2 & 0xffffffffL);
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public String readLine() {
        int start = pos;
        while (pos < length) {
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
        int endPos = pos + length;
        int count = 0, a;
        char[] characters = new char[length];
        while (pos < endPos) {
            if ((characters[count] = (char) buf[pos++]) < '\u0080')
                count++;
            else if (((a = characters[count]) & 0xE0) == 0xC0) {
                if (pos >= endPos) {
                    throw new UTFDataFormatException("bad string");
                }
                int b = buf[pos++];
                if ((b & 0xC0) != 0x80) {
                    throw new UTFDataFormatException("bad string");
                }
                characters[count++] = (char) (((a & 0x1F) << 6) | (b & 0x3F));
            } else if ((a & 0xf0) == 0xe0) {
                if (pos + 1 >= endPos) {
                    throw new UTFDataFormatException("bad string");
                }
                int b = buf[pos++];
                int c = buf[pos++];
                if (((b & 0xC0) != 0x80) || ((c & 0xC0) != 0x80)) {
                    throw new UTFDataFormatException("bad string");
                }
                characters[count++] = (char) (((a & 0x0F) << 12) | ((b & 0x3F) << 6) | (c & 0x3F));
            } else {
                throw new UTFDataFormatException("bad string");
            }
        }
        return new String(characters, 0, count);
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
