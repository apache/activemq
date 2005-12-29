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
package org.apache.activeio.oneport.openorb;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.activeio.adapter.AsyncToSyncChannelServer;
import org.apache.activeio.adapter.SyncChannelServerToServerSocket;
import org.apache.activeio.oneport.IIOPRecognizer;
import org.apache.activeio.oneport.OnePortAsyncChannelServer;
import org.apache.activeio.packet.sync.SyncChannelServer;
import org.openorb.orb.net.SocketFactory;

/**
 * 
 */
public class OpenORBOpenPortSocketFactory implements SocketFactory {

    private final OnePortAsyncChannelServer channelServer;

    public OpenORBOpenPortSocketFactory(OnePortAsyncChannelServer channelServer) {
        this.channelServer = channelServer;
    }
    
    /**
     * Outbound sockets are normal.
     */
    public Socket createSocket(InetAddress address, int port) throws IOException {
        return new Socket(address, port);
    }

    /**
     * Server sockets bind against the OnePortAsyncChannelServer.
     */
    public ServerSocket createServerSocket(InetAddress address, int port) throws IOException {
        SyncChannelServer sychServer = AsyncToSyncChannelServer.adapt(channelServer.bindAsyncChannel(IIOPRecognizer.IIOP_RECOGNIZER));
		sychServer.start();
		return new SyncChannelServerToServerSocket(sychServer);
    }
    
}
