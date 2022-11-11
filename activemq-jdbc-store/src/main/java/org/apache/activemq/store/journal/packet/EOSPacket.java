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


/**
 * Provides a Packet implementation that is used to represent the end of a stream.
 * 
 * @version $Revision$
 */
final public class EOSPacket implements Packet {

    static final public EOSPacket EOS_PACKET = new EOSPacket(); 
    
    private EOSPacket() {
    }

    public void writeTo(OutputStream out) throws IOException {
    }
    public void writeTo(DataOutput out) throws IOException {
    }

    public int position() {
        return 1;
    }

    public void position(int position) {
    }

    public int limit() {
        return 0;
    }

    public void limit(int limit) {
    }

    public void flip() {
    }

    public int remaining() {
        return -1;
    }

    public void rewind() {
    }

    public boolean hasRemaining() {
        return false;
    }

    public void clear() {
    }

    public int capacity() {
        return 0;
    }

    public Packet slice() {
        return this;
    }
    
    public Packet duplicate() {
        return this;               
    }

    public Object duplicate(ClassLoader cl) throws IOException {
        try {
            Class clazz = cl.loadClass(EOSPacket.class.getName());
            return clazz.getField("EOS_PACKET").get(null);
        } catch (Throwable e) {
            throw (IOException)new IOException("Could not duplicate packet in a different classloader: "+e).initCause(e);
        }
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#read()
     */
    public int read() {
        return -1;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#read(byte[], int, int)
     */
    public int read(byte[] data, int offset, int length) {
        return -1;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#write(int)
     */
    public boolean write(int data) {
        return false;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#write(byte[], int, int)
     */
    public int write(byte[] data, int offset, int length) {
        return -1;
    }
    
    public ByteSequence asByteSequence() {
        return EmptyPacket.EMPTY_BYTE_SEQUENCE;
    }

    public byte[] sliceAsBytes() {
        return EmptyPacket.EMPTY_BYTE_ARRAY;
    }
    
    /**
     * @param dest
     * @return the number of bytes read into the dest.
     */
    public int read(Packet dest) {        
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
