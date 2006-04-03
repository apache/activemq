/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.kaha.impl;
import java.io.ByteArrayOutputStream;

/**
 * Optimized ByteArrayOutputStream
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class StoreByteArrayOutputStream extends ByteArrayOutputStream {
    /**
     * Creates a new byte array output stream. 
     */
    public StoreByteArrayOutputStream() {
        super(16 * 1024);
    }

    /**
     * Creates a new byte array output stream, with a buffer capacity of the specified size, in bytes.
     * 
     * @param size the initial size.
     * @exception IllegalArgumentException if size is negative.
     */
    public StoreByteArrayOutputStream(int size) {
        super(size);
    }

    /**
     * start using a fresh byte array
     * 
     * @param size
     */
    public void restart(int size) {
        buf = new byte[size];
        count = 0;
    }

    /**
     * Writes the specified byte to this byte array output stream.
     * 
     * @param b the byte to be written.
     */
    public void write(int b) {
        int newcount = count + 1;
        if (newcount > buf.length) {
            byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
            System.arraycopy(buf, 0, newbuf, 0, count);
            buf = newbuf;
        }
        buf[count] = (byte) b;
        count = newcount;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array starting at offset <code>off</code> to this byte
     * array output stream.
     * 
     * @param b the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     */
    public void write(byte b[], int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        else if (len == 0) {
            return;
        }
        int newcount = count + len;
        if (newcount > buf.length) {
            byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
            System.arraycopy(buf, 0, newbuf, 0, count);
            buf = newbuf;
        }
        System.arraycopy(b, off, buf, count, len);
        count = newcount;
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
    public void reset(){
        count = 0;
    }
    
    /**
     * Set the current position for writing
     * @param offset
     */
    public void position(int offset){
        if (offset > buf.length) {
            byte newbuf[] = new byte[Math.max(buf.length << 1, offset)];
            System.arraycopy(buf, 0, newbuf, 0, count);
            buf = newbuf;
        }
        count = offset;
    }
}