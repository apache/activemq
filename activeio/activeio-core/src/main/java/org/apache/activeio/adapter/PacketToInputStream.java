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
import java.io.InputStream;

import org.apache.activeio.packet.Packet;

/**
 * Provides an InputStream for a given Packet.
 *  
 * @version $Revision$
 */
public class PacketToInputStream extends InputStream {
    
    final Packet packet;
    
    /**
     * @param packet
     */
    public PacketToInputStream(Packet packet) {
        this.packet = packet;
    }
    
    /**
     * @see java.io.InputStream#read()
     */
    public int read() throws IOException {
        return packet.read();
    }

    /**
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException {
        return packet.read(b, off, len);
    }

}
