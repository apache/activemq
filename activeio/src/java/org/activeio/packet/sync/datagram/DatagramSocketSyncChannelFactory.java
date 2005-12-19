/** 
 * 
 * Copyright 2004 Hiram Chirino
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
 * 
 **/
package org.activeio.packet.sync.datagram;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.URI;

import org.activeio.packet.sync.SyncChannel;
import org.activeio.packet.sync.SyncChannelFactory;
import org.activeio.packet.sync.SyncChannelServer;

/**
 * A TcpSynchChannelFactory creates {@see org.activeio.net.TcpSynchChannel}
 * and {@see org.activeio.net.TcpSynchChannelServer} objects.
 * 
 * @version $Revision$
 */
public class DatagramSocketSyncChannelFactory implements SyncChannelFactory {

    /**
     * Uses the {@param location}'s host and port to create a tcp connection to a remote host.
     * 
     * @see org.activeio.SynchChannelFactory#openSyncChannel(java.net.URI)
     */
    public SyncChannel openSyncChannel(URI location) throws IOException {
        DatagramSocket socket=null;
        socket = new DatagramSocket();
        if( location != null ) {
            InetAddress address = InetAddress.getByName(location.getHost());
            socket.connect(address, location.getPort());
        }
        return createSyncChannel(socket);
    }

    /**
     * Uses the {@param location}'s host and port to create a tcp connection to a remote host.
     * 
     */
    public SyncChannel openSyncChannel(URI location, URI localLocation) throws IOException {
        DatagramSocket socket=null;
        InetAddress address = InetAddress.getByName(localLocation.getHost());
        socket = new DatagramSocket(localLocation.getPort(), address);

        if( location != null ) {
            address = InetAddress.getByName(location.getHost());
            socket.connect(address, location.getPort());
        }
        return createSyncChannel(socket);
    }

    /**
     * @param socket
     * @return
     * @throws IOException
     */
    protected SyncChannel createSyncChannel(DatagramSocket socket) throws IOException {
        return new DatagramSocketSyncChannel(socket);
    }

    /**
     * @throws IOException allways thrown.
     * @see org.activeio.SynchChannelFactory#bindSynchChannel(java.net.URI)
     */
    public SyncChannelServer bindSyncChannel(URI location) throws IOException {
        throw new IOException("A SynchChannelServer is not available for this channel.");
    }

}
