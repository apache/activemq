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

public class ByteSequence {

    public byte[] data;
    public int offset;
    public int length;

    public ByteSequence() {
    }

    public ByteSequence(byte data[]) {
        this.data = data;
        this.offset = 0;
        this.length = data.length;
    }

    public ByteSequence(byte data[], int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public byte[] getData() {
        return data;
    }

    public int getLength() {
        return length;
    }

    public int getOffset() {
        return offset;
    }

    public int remaining() { return length - offset; }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void compact() {
        if (length != data.length) {
            byte t[] = new byte[length];
            System.arraycopy(data, offset, t, 0, length);
            data = t;
            offset = 0;
        }
    }

    public void reset() {
        length = remaining();
        if (length > 0) {
            System.arraycopy(data, offset, data, 0, length);
        } else {
            length = 0;
        }
        offset = 0;
    }

    public int indexOf(ByteSequence needle, int pos) {
        int max = length - needle.length - offset;
        for (int i = pos; i < max; i++) {
            if (matches(needle, i)) {
                return i;
            }
        }
        return -1;
    }

    private boolean matches(ByteSequence needle, int pos) {
        for (int i = 0; i < needle.length; i++) {
            if( data[offset + pos+ i] != needle.data[needle.offset + i] ) {
                return false;
            }
        }
        return true;
    }

    private byte getByte(int i) {
        return data[offset+i];
    }

    final public int indexOf(byte value, int pos) {
        for (int i = pos; i < length; i++) {
            if (data[offset + i] == value) {
                return i;
            }
        }
        return -1;
    }

    public boolean startsWith(final byte[] bytes) {
        if (length - offset < bytes.length) {
            return false;
        }
        for (int i = 0; i<bytes.length; i++) {
            if (data[offset+i] != bytes[i]) {
                return false;
            }
        }
        return true;
    }
}
