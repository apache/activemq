/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activeio.command;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activeio.packet.Packet;

/**
 * Provides a mechanism to marshal commands into and out of packets
 * or into and out of streams, Channels and Datagrams.
 *
 * @version $Revision: 1.1 $
 */
public interface WireFormat {

    /**
     * Packet based marshaling 
     */
    Packet marshal(Object command) throws IOException;
    
    /**
     * Packet based un-marshaling 
     */
    Object unmarshal(Packet packet) throws IOException;

    /**
     * Stream based marshaling 
     */
    void marshal(Object command, DataOutputStream out) throws IOException;
    
    /**
     * Packet based un-marshaling 
     */
    Object unmarshal(DataInputStream in) throws IOException;
    
    /**
     * @param the version of the wire format
     */
    public void setVersion(int version);
    
    /**
     * @return the version of the wire format
     */
    public int getVersion();
    
}
