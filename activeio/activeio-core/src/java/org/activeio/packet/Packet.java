/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activeio.packet;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

import org.activeio.Adaptable;

/**
 * Provides a ByteBuffer like interface to work with IO channel packets of data.
 * 
 * @version $Revision$
 */
public interface Packet extends Adaptable {
    
    public int position();
    public void position(int position);
    public int limit();
    public void limit(int limit);
    public void flip();
    public int remaining();
    public void rewind();
    public boolean hasRemaining();
    public void clear();
    public Packet slice();
    public Packet duplicate();
    public Object duplicate(ClassLoader cl) throws IOException;
    public int capacity();
    public void dispose();
    
    public ByteSequence asByteSequence();
    public byte[] sliceAsBytes();
    
    
    /**
     * Writes the remaing bytes in the packet to the output stream.
     * 
     * @param out
     * @return
     */
    void writeTo(OutputStream out) throws IOException;   
    void writeTo(DataOutput out) throws IOException;
    
    // To read data out of the packet.
    public int read();
    public int read(byte data[], int offset, int length);

    // To write data into the packet.
    public boolean write( int data );
    public int write( byte data[], int offset, int length );
    public int read(Packet dest);
    
}
