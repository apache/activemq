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
package org.activeio.oneport.jetty;

import java.io.IOException;
import java.net.ServerSocket;

import org.activeio.adapter.AsyncToSyncChannelServer;
import org.activeio.adapter.SyncChannelServerToServerSocket;
import org.activeio.oneport.HttpRecognizer;
import org.activeio.oneport.OnePortAsyncChannelServer;
import org.activeio.packet.sync.SyncChannelServer;
import org.mortbay.http.SocketListener;
import org.mortbay.util.InetAddrPort;

/**
 *
 */
public class JettyOnePortSocketListener extends SocketListener {

    private static final long serialVersionUID = 3257567321504561206L;
    
    private final OnePortAsyncChannelServer channelServer;

    public JettyOnePortSocketListener(OnePortAsyncChannelServer channelServer) {
        this.channelServer = channelServer;
    }

    public JettyOnePortSocketListener(OnePortAsyncChannelServer channelServer, InetAddrPort arg0) {
        super(arg0);
        this.channelServer = channelServer;
    }
    
    protected ServerSocket newServerSocket(InetAddrPort addrPort, int backlog) throws IOException {
       SyncChannelServer syncServer = AsyncToSyncChannelServer.adapt(channelServer.bindAsyncChannel(HttpRecognizer.HTTP_RECOGNIZER));
       syncServer.start();
       return new SyncChannelServerToServerSocket(syncServer);
    }
}
