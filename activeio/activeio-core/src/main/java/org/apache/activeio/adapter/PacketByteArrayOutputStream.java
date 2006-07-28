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
package org.apache.activeio.adapter;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.activeio.packet.AppendedPacket;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.Packet;

/**
 *
 */
final public class PacketByteArrayOutputStream extends OutputStream {
    
    private Packet result;
    private Packet current;
    int nextAllocationSize=0;
    
    public PacketByteArrayOutputStream() {
    	this( 1024 );
    }
    
    public PacketByteArrayOutputStream(int initialSize) {
    	nextAllocationSize = initialSize;
        current = allocate();
    }
    
    protected Packet allocate() {
    	ByteArrayPacket packet = new ByteArrayPacket(new byte[nextAllocationSize]);
    	nextAllocationSize <<= 3; // x by 8
        return packet;
    }
    
    public void skip(int size) {
        while( size > 0 ) {
            if( !current.hasRemaining() ) {
                allocatedNext();
            }
            
            int skip = ((size <= current.remaining()) ? size : current.remaining());
            current.position(current.position()+skip);
            size -= skip;
        }
    }
    
    public void write(int b) throws IOException {
        if( !current.hasRemaining() ) {
            allocatedNext();
        }
        current.write(b);
    }
    
    public void write(byte[] b, int off, int len) throws IOException {
        while( len > 0 ) {
	        if( !current.hasRemaining() ) {
	            allocatedNext();
	        }
	        int wrote = current.write(b,off,len);
	        off+=wrote;
	        len-=wrote;
        }
    }
    
    private void allocatedNext() {
        if( result == null ) {
            current.flip();
            result = current;
        } else {
            current.flip();
            result = AppendedPacket.join(result, current);            
        }
        current = allocate();
    }
    
    public Packet getPacket() {
        if( result == null ) {
            current.flip();
            return current.slice();
        } else {
            current.flip();
            return AppendedPacket.join(result, current);                
        }
    }

    public void reset() {
        result = null;
        current.clear();
    }

    public int position() {
        return current.position() + (result==null ? 0 : result.remaining());
    }
}
