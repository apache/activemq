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
package org.apache.activeio.stream.sync.socket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.apache.activeio.stream.sync.StreamChannel;
import org.apache.activeio.stream.sync.StreamChannelFactory;
import org.apache.activeio.stream.sync.StreamChannelServer;
import org.apache.activeio.util.URISupport;

/**
 * A TcpStreamChannelFactory creates {@see org.apache.activeio.net.TcpStreamChannel}
 * and {@see org.apache.activeio.net.TcpStreamChannelServer} objects.
 * 
 * @version $Revision$
 */
public class SocketStreamChannelFactory implements StreamChannelFactory {

    protected static final int DEFAULT_BACKLOG = 500;

    private final SocketFactory socketFactory;
    private final ServerSocketFactory serverSocketFactory;
    private int backlog = DEFAULT_BACKLOG;
    
    public SocketStreamChannelFactory() {
        this(SocketFactory.getDefault(), ServerSocketFactory.getDefault());
    }

    public SocketStreamChannelFactory(SocketFactory socketFactory, ServerSocketFactory serverSocketFactory) {
        this.socketFactory = socketFactory;
        this.serverSocketFactory = serverSocketFactory;
    }
        
    /**
     * Uses the {@param location}'s host and port to create a tcp connection to a remote host.
     * 
     * @see org.apache.activeio.StreamChannelFactory#openStreamChannel(java.net.URI)
     */
    public StreamChannel openStreamChannel(URI location) throws IOException {
        Socket socket=null;
        socket = socketFactory.createSocket(location.getHost(), location.getPort());
        return createStreamChannel(socket);
    }

    /**
     * @param socket
     * @return
     * @throws IOException
     */
    protected StreamChannel createStreamChannel(Socket socket) throws IOException {
        return new SocketStreamChannel(socket);
    }

    /**
     * Binds a server socket a the {@param bindURI}'s port.
     * 
     * @see org.apache.activeio.StreamChannelFactory#bindStreamChannel(java.net.URI)
     */
    public StreamChannelServer bindStreamChannel(URI bindURI) throws IOException {
        
        String host = bindURI.getHost();
        InetAddress addr;
        if( host == null || host.length() == 0 || host.equals("localhost") || host.equals("0.0.0.0") || InetAddress.getLocalHost().getHostName().equals(host) ) {            
            addr = InetAddress.getLocalHost();
        } else {
            addr = InetAddress.getByName(host);
        }
        ServerSocket serverSocket;
        
        if (addr.equals(InetAddress.getLocalHost())) {
            serverSocket = serverSocketFactory.createServerSocket(bindURI.getPort(), backlog);
        } else {
            serverSocket = serverSocketFactory.createServerSocket(bindURI.getPort(), backlog, addr);
        }

        URI connectURI=bindURI;
        try {
            // connectURI = URISupport.changeHost(connectURI, addr.getHostName());
            connectURI = URISupport.changePort(connectURI, serverSocket.getLocalPort());
        } catch (URISyntaxException e) {
            throw (IOException)new IOException("Could build connect URI: "+e).initCause(e);
        }
        
        return new SocketStreamChannelServer(serverSocket, bindURI, connectURI);
    }
    
    /**
     * @return Returns the backlog.
     */
    public int getBacklog() {
        return backlog;
    }

    /**
     * @param backlog
     *            The backlog to set.
     */
    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }


}
