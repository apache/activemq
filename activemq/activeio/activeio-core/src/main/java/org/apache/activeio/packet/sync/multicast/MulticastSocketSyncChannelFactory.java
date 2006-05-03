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
package org.apache.activeio.packet.sync.multicast;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.URI;

import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.packet.sync.SyncChannelFactory;
import org.apache.activeio.packet.sync.SyncChannelServer;


/**
 * @version $Revision: $ $Date: $
 */
public class MulticastSocketSyncChannelFactory implements SyncChannelFactory {

    public SyncChannel openSyncChannel(URI groupURI) throws IOException {
        if (groupURI == null) throw new IllegalArgumentException("group URI cannot be null");

        MulticastSocket socket = new MulticastSocket(groupURI.getPort());

        return createSyncChannel(socket, InetAddress.getByName(groupURI.getHost()));
    }

    public SyncChannel openSyncChannel(URI groupURI, URI localLocation) throws IOException {
        if (groupURI == null) throw new IllegalArgumentException("group URI cannot be null");

        MulticastSocket socket = new MulticastSocket(groupURI.getPort());
        if (localLocation != null) {
            socket.setInterface(InetAddress.getByName(localLocation.getHost()));
        }

        return createSyncChannel(socket, InetAddress.getByName(groupURI.getHost()));
    }

    protected SyncChannel createSyncChannel(MulticastSocket socket, InetAddress groupAddress) throws IOException {
        return new MulticastSocketSyncChannel(socket, groupAddress);
    }

    public SyncChannelServer bindSyncChannel(URI location) throws IOException {
        throw new IOException("A SyncChannelServer is not available for this channel.");
    }
}
