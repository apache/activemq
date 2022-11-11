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
package org.apache.activemq.store.journal.packet;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;


/**
 * Provides a Packet implementation that is directly backed by a <code>byte</code>.
 * 
 * @version $Revision$
 */
final public class BytePacket implements Packet {

    private byte data;
    private byte position;
    private byte limit;

    public BytePacket(byte data) {
        this.data = data;
        clear();
    }

    public int position() {
        return position;
    }

    public void position(int position) {
        this.position = (byte) position;
    }

    public int limit() {
        return limit;
    }

    public void limit(int limit) {
        this.limit = (byte) limit;
    }

    public void flip() {
        limit(position());
        position(0);
    }

    public int remaining() {
        return limit() - position();
    }

    public void rewind() {
        position(0);
    }

    public boolean hasRemaining() {
        return remaining() > 0;
    }

    public void clear() {
        position(0);
        limit(capacity());
    }

    public int capacity() {
        return 1;
    }

    public Packet slice() {
        if( hasRemaining() )
            return new BytePacket(data);
        return EmptyPacket.EMPTY_PACKET;
    }
    
    public Packet duplicate() {
        BytePacket packet = new BytePacket(data);
        packet.limit(limit());
        packet.position(position());
        return packet;
    }
    
    public Object duplicate(ClassLoader cl) throws IOException {
        try {
            Class clazz = cl.loadClass(BytePacket.class.getName());
            Constructor constructor = clazz.getConstructor(new Class[]{byte.class});
            return constructor.newInstance(new Object[]{new Byte(data)});
        } catch (Throwable e) {
            throw (IOException)new IOException("Could not duplicate packet in a different classloader: "+e).initCause(e);
        }
    }

    public void writeTo(OutputStream out) throws IOException {
        if( hasRemaining() ) {
            out.write(data);
            position(1);
        }
    }

    public void writeTo(DataOutput out) throws IOException {
        if( hasRemaining() ) {
            out.write(data);
            position(1);
        }
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#read()
     */
    public int read() {
        if( !hasRemaining() )
            return -1;
        position(1);
        return data & 0xff;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#read(byte[], int, int)
     */
    public int read(byte[] data, int offset, int length) {
        if( !hasRemaining() )
            return -1;
        
        if( length > 0 ) {
            data[offset] = this.data;
            position(1);
            return 1;
        }
        return 0;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#write(int)
     */
    public boolean write(int data) {
        if( !hasRemaining() )
            return false;
        
        this.data = (byte) data; 
        position(1);
        return true;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#write(byte[], int, int)
     */
    public int write(byte[] data, int offset, int length) {
        if( !hasRemaining() )
            return -1;

        if( length > 0 ) {
            this.data = data[offset] ;
            position(1);
            return 1;
        }
        return 0;
    }

    public ByteSequence asByteSequence() {
        return null;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#sliceAsBytes()
     */
    public byte[] sliceAsBytes() {
        return null;
    }
    
    /**
     * @param dest
     * @return the number of bytes read into the dest.
     */
    public int read(Packet dest) {
        if( hasRemaining() ) {
            dest.write(data);
            position(1);
            return 1;
        }
        return 0;
    }
    
    public String toString() {
        return "{position="+position()+",limit="+limit()+",capacity="+capacity()+"}";
    }

    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return null;
    }
    
    public void dispose() {        
    }
}
