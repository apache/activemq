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
import java.nio.ByteBuffer;


/**
 * Provides a Packet implementation that is backed by a {@see java.nio.ByteBuffer}
 * 
 * @version $Revision$
 */
final public class ByteBufferPacket implements Packet {

	public static final int DEFAULT_BUFFER_SIZE = Integer.parseInt(System.getProperty("org.apache.activemq.store.DefaultByteBufferSize", ""+(64*1024)));
	public static final int DEFAULT_DIRECT_BUFFER_SIZE = Integer.parseInt(System.getProperty("org.apache.activemq.store.DefaultDirectByteBufferSize", ""+(8*1024)));

    private final ByteBuffer buffer;
    private static final int TEMP_BUFFER_SIZE = 64*1024;

    public ByteBufferPacket(ByteBuffer buffer) {
        this.buffer = buffer;
        clear();
    }
    
    public ByteBuffer getByteBuffer() {
        return buffer;
    }
    
    public static ByteBufferPacket createDefaultBuffer(boolean direct) {
    	if( direct )
    		return new ByteBufferPacket( ByteBuffer.allocateDirect(DEFAULT_DIRECT_BUFFER_SIZE) );
    	return new ByteBufferPacket( ByteBuffer.allocate(DEFAULT_BUFFER_SIZE)  );
    }
    
    public void writeTo(OutputStream out) throws IOException {
        if( buffer.hasArray() ) {
            
            // If the buffer is backed by an array.. then use it directly.
            out.write(buffer.array(), position(), remaining());
            position(limit());
            
        } else {
            
            // It's not backed by a buffer.. We can only dump it to a OutputStream via a byte[] so,
            // create a temp buffer that we can use to chunk it out.            
            byte temp[] = new byte[TEMP_BUFFER_SIZE];            
            while( buffer.hasRemaining() ) {
                int maxWrite = buffer.remaining() > temp.length ? temp.length : buffer.remaining();
	            buffer.get(temp, 0, maxWrite);
	            out.write(temp,0, maxWrite);
            }
            
        }        
    }
    
    public void writeTo(DataOutput out) throws IOException {
        if( buffer.hasArray() ) {
            
            // If the buffer is backed by an array.. then use it directly.
            out.write(buffer.array(), position(), remaining());
            position(limit());
            
        } else {
            
            // It's not backed by a buffer.. We can only dump it to a OutputStream via a byte[] so,
            // create a temp buffer that we can use to chunk it out.            
            byte temp[] = new byte[TEMP_BUFFER_SIZE];            
            while( buffer.hasRemaining() ) {
                int maxWrite = buffer.remaining() > temp.length ? temp.length : buffer.remaining();
                buffer.get(temp, 0, maxWrite);
                out.write(temp,0, maxWrite);
            }
            
        }        
    }

    public int capacity() {
        return buffer.capacity();
    }

    public void clear() {
        buffer.clear();
    }

    public Packet compact() {
        buffer.compact();
        return this;
    }

    public void flip() {
        buffer.flip();
    }

    public boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    public boolean isDirect() {
        return buffer.isDirect();
    }

    public boolean isReadOnly() {
        return buffer.isReadOnly();
    }

    public int limit() {
        return buffer.limit();
    }

    public void limit(int arg0) {
        buffer.limit(arg0);
    }

    public Packet mark() {
        buffer.mark();
        return this;
    }

    public int position() {
        return buffer.position();
    }

    public void position(int arg0) {
        buffer.position(arg0);
    }

    public int remaining() {
        return buffer.remaining();
    }

    public void rewind() {
        buffer.rewind();
    }

    public Packet slice() {
        return new ByteBufferPacket(buffer.slice());
    }

    public Packet duplicate() {
        return new ByteBufferPacket(buffer.duplicate());
    }

    public Object duplicate(ClassLoader cl) throws IOException {
        try {
            Class clazz = cl.loadClass(ByteBufferPacket.class.getName());
            Constructor constructor = clazz.getConstructor(new Class[]{ByteBuffer.class});
            return constructor.newInstance(new Object[]{buffer.duplicate()});
        } catch (Throwable e) {
            throw (IOException)new IOException("Could not duplicate packet in a different classloader: "+e).initCause(e);
        }

    }
    

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#read()
     */
    public int read() {
        if( !buffer.hasRemaining() )
            return -1;
        return buffer.get() & 0xff;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#read(byte[], int, int)
     */
    public int read(byte[] data, int offset, int length) {
        if( !hasRemaining() )
            return -1;
        
        int copyLength = Math.min(length, remaining());
        buffer.get(data, offset, copyLength);
        return copyLength;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#write(int)
     */
    public boolean write(int data) {
        if( !buffer.hasRemaining() )
            return false;
        buffer.put((byte)data);
        return true;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#write(byte[], int, int)
     */
    public int write(byte[] data, int offset, int length) {
        if( !hasRemaining() )
            return -1;

        int copyLength = Math.min(length, remaining());
        buffer.put(data, offset, copyLength);
        return copyLength;
    }

    /**
     * @see org.apache.activemq.store.journal.packet.Packet#asByteSequence()
     */
    public ByteSequence asByteSequence() {
        if( buffer.hasArray() ) {
            byte[] bs = buffer.array();
            return new ByteSequence(bs, buffer.position(), buffer.remaining());
        } else {
            byte[] bs = new byte[buffer.remaining()];
        	int p = buffer.position();
        	buffer.get(bs);
        	buffer.position(p);
        	return new ByteSequence(bs, 0, bs.length);
        }
    }
    
    /**
     * @see org.apache.activemq.store.journal.packet.Packet#sliceAsBytes()
     */
    public byte[] sliceAsBytes() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * @param dest
     * @return the number of bytes read into the dest.
     */
    public int read(Packet dest) {
        
	    int rc = Math.min(dest.remaining(), remaining()); 
		if( rc > 0 ) {
		    
	        if( dest.getClass() == ByteBufferPacket.class ) {            

			    // Adjust our limit so that we don't overflow the dest buffer. 
				int limit = limit();
				limit(position()+rc);
				
	            ((ByteBufferPacket)dest).buffer.put(buffer);

	            // restore the limit.
				limit(limit);
	            
	            return 0;
	        } else {	            
	            ByteSequence sequence = dest.asByteSequence();
	            rc = read(sequence.getData(), sequence.getOffset(), sequence.getLength());
	            dest.position(dest.position()+rc);
	        }
		}
		return rc;
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
