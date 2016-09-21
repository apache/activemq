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
package org.apache.activemq.transport.amqp.message;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.codec.WritableBuffer;

/**
 *
 */
public class AmqpWritableBuffer implements WritableBuffer {

    public final static int DEFAULT_CAPACITY = 4 * 1024;

    byte buffer[];
    int position;

   /**
    * Creates a new WritableBuffer with default capacity.
    */
   public AmqpWritableBuffer() {
       this(DEFAULT_CAPACITY);
   }

    /**
     * Create a new WritableBuffer with the given capacity.
     */
    public AmqpWritableBuffer(int capacity) {
        this.buffer = new byte[capacity];
    }

    public byte[] getArray() {
        return buffer;
    }

    public int getArrayLength() {
        return position;
    }

    @Override
    public void put(byte b) {
        int newPosition = position + 1;
        ensureCapacity(newPosition);
        buffer[position] = b;
        position = newPosition;
    }

    @Override
    public void putShort(short value) {
        ensureCapacity(position + 2);
        buffer[position++] = (byte)(value >>> 8);
        buffer[position++] = (byte)(value >>> 0);
    }

    @Override
    public void putInt(int value) {
        ensureCapacity(position + 4);
        buffer[position++] = (byte)(value >>> 24);
        buffer[position++] = (byte)(value >>> 16);
        buffer[position++] = (byte)(value >>> 8);
        buffer[position++] = (byte)(value >>> 0);
    }

    @Override
    public void putLong(long value) {
        ensureCapacity(position + 8);
        buffer[position++] = (byte)(value >>> 56);
        buffer[position++] = (byte)(value >>> 48);
        buffer[position++] = (byte)(value >>> 40);
        buffer[position++] = (byte)(value >>> 32);
        buffer[position++] = (byte)(value >>> 24);
        buffer[position++] = (byte)(value >>> 16);
        buffer[position++] = (byte)(value >>> 8);
        buffer[position++] = (byte)(value >>> 0);
    }

    @Override
    public void putFloat(float value) {
        putInt(Float.floatToRawIntBits(value));
    }

    @Override
    public void putDouble(double value) {
        putLong(Double.doubleToRawLongBits(value));
    }

    @Override
    public void put(byte[] src, int offset, int length) {
        if (length == 0) {
            return;
        }

        int newPosition = position + length;
        ensureCapacity(newPosition);
        System.arraycopy(src, offset, buffer, position, length);
        position = newPosition;
    }

    @Override
    public boolean hasRemaining() {
        return position < Integer.MAX_VALUE;
    }

    @Override
    public int remaining() {
        return Integer.MAX_VALUE - position;
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public void position(int position) {
        ensureCapacity(position);
        this.position = position;
    }

    @Override
    public void put(ByteBuffer payload) {
        int newPosition = position + payload.remaining();
        ensureCapacity(newPosition);
        while (payload.hasRemaining()) {
            buffer[position++] = payload.get();
        }

        position = newPosition;
    }

    @Override
    public int limit() {
        return Integer.MAX_VALUE;
    }

    /**
     * Ensures the the buffer has at least the minimumCapacity specified.
     *
     * @param minimumCapacity
     *      the minimum capacity needed to meet the next write operation.
     */
    private void ensureCapacity(int minimumCapacity) {
        if (minimumCapacity > buffer.length) {
            byte newBuffer[] = new byte[Math.max(buffer.length << 1, minimumCapacity)];
            System.arraycopy(buffer, 0, newBuffer, 0, position);
            buffer = newBuffer;
        }
    }
}
