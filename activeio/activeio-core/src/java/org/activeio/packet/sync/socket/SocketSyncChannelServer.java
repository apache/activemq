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

package org.activeio.packet.sync.socket;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;

import org.activeio.Channel;
import org.activeio.packet.sync.SyncChannelServer;
import org.activeio.stream.sync.socket.SocketStreamChannel;
import org.activeio.stream.sync.socket.SocketStreamChannelServer;

/**
 * A SynchChannelServer that creates
 * {@see org.activeio.net.TcpSynchChannel}objects from accepted
 * TCP socket connections.
 * 
 * @version $Revision$
 */
public class SocketSyncChannelServer implements SyncChannelServer {

    private final SocketStreamChannelServer server;

    public SocketSyncChannelServer(SocketStreamChannelServer server) {
        this.server = server;
    }

    public SocketSyncChannelServer(ServerSocket socket, URI bindURI, URI connectURI) {
        this(new SocketStreamChannelServer(socket, bindURI, connectURI));
    }

    public Channel accept(long timeout) throws IOException {
        Channel channel = server.accept(timeout);
        if( channel != null ) {
            channel = createChannel((SocketStreamChannel) channel);
        }
        return channel;
    }

    protected Channel createChannel(SocketStreamChannel channel) throws IOException {
        return new SocketSyncChannel(channel);
    }

    /**
     * @see org.activeio.Disposable#dispose()
     */
    public void dispose() {
        server.dispose();
    }

    /**
     * @return Returns the bindURI.
     */
    public URI getBindURI() {
        return server.getBindURI();
    }

    /**
     * @return Returns the connectURI.
     */
    public URI getConnectURI() {
        return server.getConnectURI();
    }

    public void start() throws IOException {
        server.start();
    }

    public void stop() throws IOException {
        server.stop();
    }
    
    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return server.getAdapter(target);
    }    
    
    public String toString() {
        return server.toString();
    }    
}