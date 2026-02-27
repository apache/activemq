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

package org.apache.activemq.protobuf;

import java.util.List;

public class Buffer implements Comparable<Buffer> {

    final public byte[] data;
    final public int offset;
    final public int length;

    public Buffer(Buffer other) {
        this(other.data, other.offset, other.length);
    }

    public Buffer(byte data[]) {
        this(data, 0, data.length);
    }

    public Buffer(byte data[], int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    @Deprecated
    public Buffer(String value) {
        this(UTF8Buffer.encode(value));
    }

    public final Buffer slice(int low, int high) {
        int sz;

        if (high < 0) {
            sz = length + high;
        } else {
            sz = high - low;
        }

        if (sz < 0) {
            sz = 0;
        }

        return new Buffer(data, offset + low, sz);
    }

    public final byte[] getData() {
        return data;
    }

    public final int getLength() {
        return length;
    }

    public final int getOffset() {
        return offset;
    }

    public Buffer compact() {
        if (length != data.length) {
            return new Buffer(toByteArray());
        }
        return this;
    }

    final public byte[] toByteArray() {
        byte[] data = this.data;
        int length = this.length;
        if (length != data.length) {
            byte t[] = new byte[length];
            System.arraycopy(data, offset, t, 0, length);
            data = t;
        }
        return data;
    }

    public byte byteAt(int i) {
        return data[offset + i];
    }


    @Override
    public int hashCode() {
        byte[] target = new byte[4];
        for (int i = 0; i < length; i++) {
            target[i % 4] ^= data[offset + i];
        }
        return target[0] << 24 | target[1] << 16 | target[2] << 8 | target[3];
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj == null || obj.getClass() != Buffer.class)
            return false;

        return equals((Buffer) obj);
    }

    final public boolean equals(Buffer obj) {
        if (length != obj.length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (obj.data[obj.offset + i] != data[offset + i]) {
                return false;
            }
        }
        return true;
    }

    final public BufferInputStream newInput() {
        return new BufferInputStream(this);
    }

    final public BufferOutputStream newOutput() {
        return new BufferOutputStream(this);
    }

    final public boolean isEmpty() {
        return length == 0;
    }

    final public boolean contains(byte value) {
        return indexOf(value, 0) >= 0;
    }

    final public int indexOf(byte value, int pos) {
        for (int i = pos; i < length; i++) {
            if (data[offset + i] == value) {
                return i;
            }
        }
        return -1;
    }

    final public static Buffer join(List<Buffer> items, Buffer seperator) {
        if (items.isEmpty())
            return new Buffer(seperator.data, 0, 0);

        int size = 0;
        for (Buffer item : items) {
            size += item.length;
        }
        size += seperator.length * (items.size() - 1);

        int pos = 0;
        byte data[] = new byte[size];
        for (Buffer item : items) {
            if (pos != 0) {
                System.arraycopy(seperator.data, seperator.offset, data, pos, seperator.length);
                pos += seperator.length;
            }
            System.arraycopy(item.data, item.offset, data, pos, item.length);
            pos += item.length;
        }

        return new Buffer(data, 0, size);
    }

    @Deprecated
    public String toStringUtf8() {
        return UTF8Buffer.decode(this);
    }

    public int compareTo(Buffer o) {
        int minLength = Math.min(length, o.length);
        if (offset == o.offset) {
            int pos = offset;
            int limit = minLength + offset;
            while (pos < limit) {
                byte b1 = data[pos];
                byte b2 = o.data[pos];
                if (b1 != b2) {
                    return b1 - b2;
                }
                pos++;
            }
        } else {
            int offset1 = offset;
            int offset2 = o.offset;
            while ( minLength-- != 0) {
                byte b1 = data[offset1++];
                byte b2 = o.data[offset2++];
                if (b1 != b2) {
                    return b1 - b2;
                }
            }
        }
        return length - o.length;
    }
        
}
