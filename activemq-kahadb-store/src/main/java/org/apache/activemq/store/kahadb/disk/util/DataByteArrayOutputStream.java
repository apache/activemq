/*
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
package org.apache.activemq.store.kahadb.disk.util;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;

import org.apache.activemq.store.kahadb.disk.page.PageFile;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.MarshallingSupport;

/**
 * Optimized ByteArrayOutputStream
 */
public class DataByteArrayOutputStream extends OutputStream implements DataOutput, AutoCloseable {
    private static final int DEFAULT_SIZE = PageFile.DEFAULT_PAGE_SIZE;
    protected byte buf[];
    protected int pos;

    /**
     * Creates a new byte array output stream, with a buffer capacity of the
     * specified size, in bytes.
     *
     * @param size the initial size.
     * @exception IllegalArgumentException if size is negative.
     */
    public DataByteArrayOutputStream(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Invalid size: " + size);
        }
        buf = new byte[size];
    }

    /**
     * Creates a new byte array output stream.
     */
    public DataByteArrayOutputStream() {
        this(DEFAULT_SIZE);
    }

    /**
     * start using a fresh byte array
     *
     * @param size
     */
    public void restart(int size) {
        buf = new byte[size];
        pos = 0;
    }

    /**
     * start using a fresh byte array
     */
    public void restart() {
        restart(DEFAULT_SIZE);
    }

    /**
     * Get a ByteSequence from the stream
     *
     * @return the byte sequence
     */
    public ByteSequence toByteSequence() {
        return new ByteSequence(buf, 0, pos);
    }

    /**
     * Writes the specified byte to this byte array output stream.
     *
     * @param b the byte to be written.
     * @throws IOException
     */
    @Override
    public void write(int b) throws IOException {
        int newcount = pos + 1;
        ensureEnoughBuffer(newcount);
        buf[pos] = (byte)b;
        pos = newcount;
        onWrite();
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array starting at
     * offset <code>off</code> to this byte array output stream.
     *
     * @param b the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @throws IOException
     */
    @Override
    public void write(byte b[], int off, int len) throws IOException {
        if (len == 0) {
            return;
        }
        int newcount = pos + len;
        ensureEnoughBuffer(newcount);
        System.arraycopy(b, off, buf, pos, len);
        pos = newcount;
        onWrite();
    }

    /**
     * @return the underlying byte[] buffer
     */
    public byte[] getData() {
        return buf;
    }

    /**
     * reset the output stream
     */
    public void reset() {
        pos = 0;
    }

    /**
     * Set the current position for writing
     *
     * @param offset
     * @throws IOException
     */
    public void position(int offset) throws IOException {
        ensureEnoughBuffer(offset);
        pos = offset;
        onWrite();
    }

    public int size() {
        return pos;
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        ensureEnoughBuffer(pos + 1);
        buf[pos++] = (byte)(v ? 1 : 0);
        onWrite();
    }

    @Override
    public void writeByte(int v) throws IOException {
        ensureEnoughBuffer(pos + 1);
        buf[pos++] = (byte)(v >>> 0);
        onWrite();
    }

    @Override
    public void writeShort(int v) throws IOException {
        ensureEnoughBuffer(pos + 2);
        buf[pos++] = (byte)(v >>> 8);
        buf[pos++] = (byte)(v >>> 0);
        onWrite();
    }

    @Override
    public void writeChar(int v) throws IOException {
        ensureEnoughBuffer(pos + 2);
        buf[pos++] = (byte)(v >>> 8);
        buf[pos++] = (byte)(v >>> 0);
        onWrite();
    }

    @Override
    public void writeInt(int v) throws IOException {
        ensureEnoughBuffer(pos + 4);
        buf[pos++] = (byte)(v >>> 24);
        buf[pos++] = (byte)(v >>> 16);
        buf[pos++] = (byte)(v >>> 8);
        buf[pos++] = (byte)(v >>> 0);
        onWrite();
    }

    @Override
    public void writeLong(long v) throws IOException {
        ensureEnoughBuffer(pos + 8);
        buf[pos++] = (byte)(v >>> 56);
        buf[pos++] = (byte)(v >>> 48);
        buf[pos++] = (byte)(v >>> 40);
        buf[pos++] = (byte)(v >>> 32);
        buf[pos++] = (byte)(v >>> 24);
        buf[pos++] = (byte)(v >>> 16);
        buf[pos++] = (byte)(v >>> 8);
        buf[pos++] = (byte)(v >>> 0);
        onWrite();
    }

    @Override
    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        int length = s.length();
        for (int i = 0; i < length; i++) {
            write((byte)s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        int length = s.length();
        for (int i = 0; i < length; i++) {
            int c = s.charAt(i);
            write((c >>> 8) & 0xFF);
            write((c >>> 0) & 0xFF);
        }
    }

    @Override
    public void writeUTF(String text) throws IOException {
        long encodedsize = MarshallingSupport.countUTFBytes(text);
        if (encodedsize > 65535) {
            throw new UTFDataFormatException("encoded string too long: " + encodedsize + " bytes");
        }
        ensureEnoughBuffer((int)(pos + encodedsize + 2));
        writeShort((int)encodedsize);

        byte[] buffer = new byte[(int)encodedsize];
        MarshallingSupport.writeUTFBytesToBuffer(text, (int) encodedsize, buf, pos);
        pos += encodedsize;
        onWrite();
    }

    private void ensureEnoughBuffer(int newcount) {
        if (newcount > buf.length) {
            byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
            System.arraycopy(buf, 0, newbuf, 0, pos);
            buf = newbuf;
        }
    }

    /**
     * This method is called after each write to the buffer.  This should allow subclasses
     * to take some action based on the writes, for example flushing data to an external system based on size.
     */
    protected void onWrite() throws IOException {
    }

    public void skip(int size) throws IOException {
        ensureEnoughBuffer(pos + size);
        pos+=size;
        onWrite();
    }

    public ByteSequence getByteSequence() {
        return new ByteSequence(buf, 0, pos);
    }
}
