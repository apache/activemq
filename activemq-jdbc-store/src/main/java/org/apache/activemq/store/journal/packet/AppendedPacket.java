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
 * Appends two packets together.
 * 
 * @version $Revision$
 */
final public class AppendedPacket implements Packet {

    private final Packet first;
    private final Packet last;

    private final int capacity;
    private final int firstCapacity;

    static public Packet join(Packet first, Packet last) {
        if( first.hasRemaining() ) {
            if( last.hasRemaining() ) {
                
                //TODO: this might even be a rejoin of the same continous buffer.
                //It would be good if we detected that and avoided just returned the buffer.
                
                return new AppendedPacket(first.slice(), last.slice());               
            } else {
                return first.slice();
            }
        } else {
            if( last.hasRemaining() ) {
                return last.slice();                
            } else {
                return EmptyPacket.EMPTY_PACKET;
            }            
        }
    }
    
    /**
     * @deprecated use {@see #join(Packet, Packet)} instead.
     */
    public AppendedPacket(Packet first, Packet second) {
        this.first = first;
        this.last = second;
        this.firstCapacity = first.capacity();
        this.capacity = first.capacity()+last.capacity();
        clear();        
    }
        
    public void position(int position) {
        if( position <= firstCapacity ) {
            last.position(0);
            first.position(position);
        } else {
            last.position(position-firstCapacity);
            first.position(firstCapacity);
        }
    }
    
    public void limit(int limit) {
        if( limit <= firstCapacity ) {
            last.limit(0);
            first.limit(limit);
        } else {
            last.limit(limit-firstCapacity);
            first.limit(firstCapacity);
        }
    }

    public Packet slice() {
        return join(first,last);
    }

    public Packet duplicate() {
        return new AppendedPacket(first.duplicate(), last.duplicate());               
    }

    public Object duplicate(ClassLoader cl) throws IOException {
        try {
            Class pclazz = cl.loadClass(Packet.class.getName());
            Class clazz = cl.loadClass(AppendedPacket.class.getName());
            Constructor constructor = clazz.getConstructor(new Class[]{pclazz, pclazz});
            return constructor.newInstance(new Object[]{first.duplicate(cl), last.duplicate(cl)});
        } catch (Throwable e) {
            throw (IOException)new IOException("Could not duplicate packet in a different classloader: "+e).initCause(e);
        }
    }
    
    public void flip() {
        limit(position());
        position(0);
    }

    public int position() {
        return first.position()+last.position();
    }
    
    public int limit() {
        return first.limit()+last.limit();
    }    

    public int remaining() {
        return first.remaining()+last.remaining();
    }

    public void rewind() {
        first.rewind();
        last.rewind();
    }

    public boolean hasRemaining() {
        return first.hasRemaining()||last.hasRemaining();
    }

    public void clear() {
        first.clear();
        last.clear();        
    }

    public int capacity() {
        return capacity;
    }

    public void writeTo(OutputStream out) throws IOException {
        first.writeTo(out);
        last.writeTo(out);
    }
    
    public void writeTo(DataOutput out) throws IOException {
        first.writeTo(out);
        last.writeTo(out);
    }


    /**
     * @see org.apache.activemq.store.journal.packet.Packet#read()
     */
    public int read() {
        if( first.hasRemaining() ) {
            return first.read();
        } else if( last.hasRemaining() ) {
            return last.read();
        } else {
            return -1;
        }
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#read(byte[], int, int)
     */
    public int read(byte[] data, int offset, int length) {        
        
        int rc1 = first.read(data, offset, length);        
        if( rc1==-1 ) {
            int rc2 = last.read(data, offset, length);
            return ( rc2==-1 ) ? -1 : rc2;
        } else {
            int rc2 = last.read(data, offset+rc1, length-rc1);
            return ( rc2==-1 ) ? rc1 : rc1+rc2;
        }

    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#write(int)
     */
    public boolean write(int data) {
        if( first.hasRemaining() ) {
            return first.write(data);
        } else if( last.hasRemaining() ) {
            return last.write(data);
        } else {
            return false;
        }
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#write(byte[], int, int)
     */
    public int write(byte[] data, int offset, int length) {
        int rc1 = first.write(data, offset, length);        
        if( rc1==-1 ) {
            int rc2 = last.write(data, offset, length);
            return ( rc2==-1 ) ? -1 : rc2;
        } else {
            int rc2 = last.write(data, offset+rc1, length-rc1);
            return ( rc2==-1 ) ? rc1 : rc1+rc2;
        }
    }

    public int read(Packet dest) {        
	    int rc = first.read(dest);
	    rc += last.read(dest);
	    return rc;
    }    
    
    public String toString() {
        return "{position="+position()+",limit="+limit()+",capacity="+capacity()+"}";
    }

    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        Object object = first.getAdapter(target);
        if( object == null )
            object = last.getAdapter(target);
        return object;
    }

    public ByteSequence asByteSequence() {      
        // TODO: implement me
        return null;
    }

    public byte[] sliceAsBytes() {
        // TODO: implement me
        return null;
    }

    public void dispose() {
        first.dispose();
        last.dispose();
    }

}
