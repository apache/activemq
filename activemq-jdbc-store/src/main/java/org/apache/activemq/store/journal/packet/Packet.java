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
 * Provides a ByteBuffer like interface to work with IO channel packets of data.
 * 
 * @version $Revision$
 */
public interface Packet {
    
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
     *  @Return object that is an instance of requested type and is associated this this object.  May return null if no 
     *  object of that type is associated.
     */
    Object getAdapter(Class target);
    
    
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
