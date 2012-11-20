/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.activemq.transport.amqp.transform;

import org.apache.qpid.proton.codec.WritableBuffer;

import java.nio.ByteBuffer;

public class DroppingWritableBuffer implements WritableBuffer
{
    int pos = 0;

    @Override
    public boolean hasRemaining() {
        return true;
    }

    @Override
    public void put(byte b) {
        pos += 1;
    }

    @Override
    public void putFloat(float f) {
        pos += 4;
    }

    @Override
    public void putDouble(double d) {
        pos += 8;
    }

    @Override
    public void put(byte[] src, int offset, int length) {
        pos += length;
    }

    @Override
    public void putShort(short s) {
        pos += 2;
    }

    @Override
    public void putInt(int i) {
        pos += 4;
    }

    @Override
    public void putLong(long l) {
        pos += 8;
    }

    @Override
    public int remaining() {
        return Integer.MAX_VALUE - pos;
    }

    @Override
    public int position() {
        return pos;
    }

    @Override
    public void position(int position) {
        pos = position;
    }

    @Override
    public void put(ByteBuffer payload) {
        pos += payload.remaining();
        payload.position(payload.limit());
    }

    @Override
    public int limit() {
        return Integer.MAX_VALUE;
    }
}
