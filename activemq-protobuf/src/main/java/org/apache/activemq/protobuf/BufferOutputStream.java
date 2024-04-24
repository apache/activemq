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

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;


/**
 * Very similar to the java.io.ByteArrayOutputStream but this version 
 * is not thread safe and the resulting data is returned in a Buffer
 * to avoid an extra byte[] allocation.  It also does not re-grow it's 
 * internal buffer.
 */
final public class BufferOutputStream extends OutputStream {

    byte buffer[];
    int offset;
    int limit;
    int pos;

    public BufferOutputStream(int size) {
        this(new byte[size]);
    }   
    
    public BufferOutputStream(byte[] buffer) {
        this.buffer = buffer;
        this.limit = buffer.length;
    }   
    
    public BufferOutputStream(Buffer data) {
        this.buffer = data.data;
        this.pos = this.offset = data.offset;
        this.limit = data.offset+data.length;
    }
    
    
    public void write(int b) throws IOException {
        int newPos = pos + 1;
        checkCapacity(newPos);
        buffer[pos] = (byte) b;
        pos = newPos;
    }

    public void write(byte b[], int off, int len) throws IOException {
        int newPos = pos + len;
        checkCapacity(newPos);
        System.arraycopy(b, off, buffer, pos, len);
        pos = newPos;
    }
    
    public Buffer getNextBuffer(int len) throws IOException {
        int newPos = pos + len;
        checkCapacity(newPos);
        return new Buffer(buffer, pos, len);
    }
    
    /**
     * Ensures the the buffer has at least the minimumCapacity specified. 
     * @param i
     * @throws EOFException 
     */
    private void checkCapacity(int minimumCapacity) throws IOException {
        if( minimumCapacity > limit ) {
            throw new EOFException("Buffer limit reached.");
        }
    }

    public void reset() {
        pos = offset;
    }

    public Buffer toBuffer() {
        return new Buffer(buffer, offset, pos);
    }
    
    public byte[] toByteArray() {
        return toBuffer().toByteArray();
    }
    
    public int size() {
        return offset-pos;
    }
    

}
