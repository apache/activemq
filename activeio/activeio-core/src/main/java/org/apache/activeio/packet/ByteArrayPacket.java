/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.packet;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;


/**
 * Provides a Packet implementation that is directly backed by a <code>byte[]</code>.
 * 
 * @version $Revision$
 */
final public class ByteArrayPacket implements Packet {

    private final byte buffer[];

    private final int offset;
    private final int capacity;
    private int position;
    private int limit;
    private int remaining;
    

    public ByteArrayPacket(byte buffer[]) {
        this(buffer,0, buffer.length);
    }
    
    public ByteArrayPacket(ByteSequence sequence) {
        this(sequence.getData(), sequence.getOffset(), sequence.getLength());
    }
    
    public ByteArrayPacket(byte buffer[], int offset, int capacity) {
        this.buffer = buffer;
        this.offset=offset;
        this.capacity=capacity;
		this.position = 0;
		this.limit = capacity;
		this.remaining = limit-position;
    }

    public int position() {
        return position;
    }

    public void position(int position) {
        this.position = position;
        remaining = limit-position;
    }

    public int limit() {
        return limit;
    }

    public void limit(int limit) {
        this.limit = limit;
        remaining = limit-position;
    }

    public void flip() {
        limit = position;
        position = 0;
        remaining = limit - position;
    }

    public int remaining() {
        return remaining;
    }

    public void rewind() {
        position = 0;
        remaining = limit - position;
    }

    public boolean hasRemaining() {
        return remaining > 0;
    }

    public void clear() {
        position = 0;
        limit = capacity;
        remaining = limit - position;
    }

    public int capacity() {
        return capacity;
    }

    public Packet slice() {
        return new ByteArrayPacket(buffer, offset+position, remaining);
    }
    
    public Packet duplicate() {
        return new ByteArrayPacket(buffer, offset, capacity);
    }

    public Object duplicate(ClassLoader cl) throws IOException {
        try{
            Class clazz = cl.loadClass(ByteArrayPacket.class.getName());
            Constructor constructor = clazz.getConstructor(new Class[]{byte[].class, int.class, int.class});
            return constructor.newInstance(new Object[]{buffer, new Integer(offset), new Integer(capacity())});
        } catch (Throwable e) {
            throw (IOException)new IOException("Could not duplicate packet in a different classloader: "+e).initCause(e);
        }
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(buffer, offset+position, remaining);
        position=limit;
        remaining = limit-position;
    }
    
    public void writeTo(DataOutput out) throws IOException {
        out.write(buffer, offset+position, remaining);
        position=limit;
        remaining = limit-position;
    }

    /**
     * @see org.apache.activeio.packet.Packet#read()
     */
    public int read() {
        if( !(remaining > 0) )
            return -1;
        int rc = buffer[offset+position];
        position++;
        remaining = limit-position;
        return rc & 0xff;
    }

    /**
     * @see org.apache.activeio.packet.Packet#read(byte[], int, int)
     */
    public int read(byte[] data, int offset, int length) {
        if( !(remaining > 0) )
            return -1;
        
        int copyLength = ((length <= remaining) ? length : remaining);
        System.arraycopy(buffer, this.offset+position, data, offset, copyLength);
        position += copyLength;
        remaining = limit-position;
        return copyLength;
    }

    /**
     * @see org.apache.activeio.packet.Packet#write(int)
     */
    public boolean write(int data) {
        if( !(remaining > 0) )
            return false;
        buffer[offset+position]=(byte) data;
        position++;
        remaining = limit-position;
        return true;
    }

    /**
     * @see org.apache.activeio.packet.Packet#write(byte[], int, int)
     */
    public int write(byte[] data, int offset, int length) {
        if( !(remaining > 0) )
            return -1;
        
        int copyLength = ((length <= remaining) ? length : remaining);
        System.arraycopy(data, offset, buffer, this.offset+position, copyLength);
        position+=copyLength;
        remaining = limit-position;
        return copyLength;
    }

    public ByteSequence asByteSequence() {
        return new ByteSequence(buffer, offset+position, remaining);
    }

    /**
     * @see org.apache.activeio.packet.Packet#sliceAsBytes()
     */
    public byte[] sliceAsBytes() {
        if( buffer.length == remaining ) {
            return buffer;
        } else {
            byte rc[] = new byte[remaining];
            int op = position;
            read(rc,0,remaining);
            position=op;
            remaining = limit-position;
            return rc;
        }
    }
    
    /**
     * @param dest
     * @return the number of bytes read into the dest.
     */
    public int read(Packet dest) {        
	    int a = dest.remaining();
		int rc = ((a <= remaining) ? a : remaining); 
		if( rc > 0 ) {
		    dest.write( buffer, offset+position, rc);
		    position = position+rc;
	        remaining = limit-position;
		}
		return rc;
    }
    
    public String toString() {
        return "{position="+position+",limit="+limit+",capacity="+capacity+"}";
    }

    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return null;
    }
    
    public byte[] getBuffer() {
        return buffer;
    }
    
    public void dispose() {        
    }
}
