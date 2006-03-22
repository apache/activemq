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
import java.io.ByteArrayInputStream;

/**
 * Optimized ByteArrayInputStream that can be used more than once
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class StoreByteArrayInputStream extends ByteArrayInputStream {
    /**
     * Creates a <code>WireByteArrayInputStream</code>.
     * 
     * @param buf the input buffer.
     */
    public StoreByteArrayInputStream(byte buf[]) {
        super(buf);
    }

    /**
     * Creates <code>WireByteArrayInputStream</code> that uses <code>buf</code> as its buffer array.
     * 
     * @param buf the input buffer.
     * @param offset the offset in the buffer of the first byte to read.
     * @param length the maximum number of bytes to read from the buffer.
     */
    public StoreByteArrayInputStream(byte buf[], int offset, int length) {
        super(buf, offset, length);
    }
    
   
    /**
     * Creates <code>WireByteArrayInputStream</code> with a minmalist byte array
     */
    public StoreByteArrayInputStream() {
        super(new byte[0]);
    }
    
    /**
     * @return the current position in the stream
     */
    public int position(){
        return pos;
    }
    
    /**
     * @return the underlying data array
     */
    public byte[] getRawData(){
        return buf;
    }

    /**
     * reset the <code>WireByteArrayInputStream</code> to use an new byte array
     * 
     * @param newBuff buffer to use
     * @param offset the offset in the buffer of the first byte to read.
     * @param length the maximum number of bytes to read from the buffer.
     */
    public void restart(byte[] newBuff, int offset, int length) {
        buf = newBuff;
        pos = offset;
        count = Math.min(offset + length, newBuff.length);
        mark = offset;
    }

    /**
     * reset the <code>WireByteArrayInputStream</code> to use an new byte array
     * 
     * @param newBuff
     */
    public void restart(byte[] newBuff) {
        restart(newBuff, 0, newBuff.length);
    }
    
    /**
     * re-start the input stream - reusing the current buffer
     * @param size
     */
    public void restart(int size){
        if (buf == null || buf.length < size){
            buf = new byte[size];
        }
        restart(buf);
    }

    /**
     * Reads the next byte of data from this input stream. The value byte is returned as an <code>int</code> in the
     * range <code>0</code> to <code>255</code>. If no byte is available because the end of the stream has been
     * reached, the value <code>-1</code> is returned.
     * <p>
     * This <code>read</code> method cannot block.
     * 
     * @return the next byte of data, or <code>-1</code> if the end of the stream has been reached.
     */
    public int read() {
        return (pos < count) ? (buf[pos++] & 0xff) : -1;
    }

    /**
     * Reads up to <code>len</code> bytes of data into an array of bytes from this input stream.
     * 
     * @param b the buffer into which the data is read.
     * @param off the start offset of the data.
     * @param len the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or <code>-1</code> if there is no more data because the
     * end of the stream has been reached.
     */
    public int read(byte b[], int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        }
        else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        if (pos >= count) {
            return -1;
        }
        if (pos + len > count) {
            len = count - pos;
        }
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    /**
     * @return the number of bytes that can be read from the input stream without blocking.
     */
    public int available() {
        return count - pos;
    }
}