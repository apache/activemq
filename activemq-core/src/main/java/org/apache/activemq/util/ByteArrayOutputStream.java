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

import java.io.OutputStream;


/**
 * Very similar to the java.io.ByteArrayOutputStream but this version 
 * is not thread safe and the resulting data is returned in a ByteSequence
 * to avoid an extra byte[] allocation.
 */
public class ByteArrayOutputStream extends OutputStream {

    byte buffer[];
    int size;

    public ByteArrayOutputStream() {
        this(1028);
    }
    public ByteArrayOutputStream(int capacity) {
        buffer = new byte[capacity];
    }

    public void write(int b) {
        int newsize = size + 1;
        checkCapacity(newsize);
        buffer[size] = (byte) b;
        size = newsize;
    }

    public void write(byte b[], int off, int len) {
        int newsize = size + len;
        checkCapacity(newsize);
        System.arraycopy(b, off, buffer, size, len);
        size = newsize;
    }
    
    /**
     * Ensures the the buffer has at least the minimumCapacity specified. 
     * @param i
     */
    private void checkCapacity(int minimumCapacity) {
        if (minimumCapacity > buffer.length) {
            byte b[] = new byte[Math.max(buffer.length << 1, minimumCapacity)];
            System.arraycopy(buffer, 0, b, 0, size);
            buffer = b;
        }
    }

    public void reset() {
        size = 0;
    }

    public ByteSequence toByteSequence() {
        return new ByteSequence(buffer, 0, size);
    }
    
    public byte[] toByteArray() {
        byte rc[] = new byte[size];
        System.arraycopy(buffer, 0, rc, 0, size);
        return rc;
    }
    
    public int size() {
        return size;
    }
}
