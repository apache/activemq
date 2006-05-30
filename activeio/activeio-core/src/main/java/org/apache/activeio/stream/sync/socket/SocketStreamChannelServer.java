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

package org.apache.activeio.stream.sync.socket;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;

import org.apache.activeio.Channel;
import org.apache.activeio.stream.sync.StreamChannelServer;

/**
 * A StreamChannelServer that creates
 * {@see org.apache.activeio.net.TcpStreamChannel}objects from accepted
 * tcp socket connections.
 * 
 * @version $Revision$
 */
public class SocketStreamChannelServer implements StreamChannelServer {

    private ServerSocket serverSocket;
    private int curentSoTimeout = 0;
    private final URI bindURI;
    private final URI connectURI;


    /**
     * @param serverSocket
     * @param bindURI
     * @param connectURI
     */
    public SocketStreamChannelServer(ServerSocket serverSocket, URI bindURI, URI connectURI) {
        this.serverSocket=serverSocket;
        this.bindURI=bindURI;
        this.connectURI=connectURI;
    }

    public Channel accept(long timeout) throws IOException {
        try {
            if (timeout == StreamChannelServer.WAIT_FOREVER_TIMEOUT)
                setSoTimeout(0);
            else if (timeout == StreamChannelServer.NO_WAIT_TIMEOUT)
                setSoTimeout(1);
            else
                setSoTimeout((int) timeout);

            Socket socket = serverSocket.accept();
            return createChannel(socket);

        } catch (SocketTimeoutException ignore) {
        }
        return null;
    }

    protected Channel createChannel(Socket socket) throws IOException {
        return new SocketStreamChannel(socket);
    }

    private void setSoTimeout(int i) throws SocketException {
        if (curentSoTimeout != i) {
            serverSocket.setSoTimeout(i);
            curentSoTimeout = i;
        }
    }

    /**
     * @see org.apache.activeio.Disposable#dispose()
     */
    public void dispose() {
        if (serverSocket == null)
            return;
        try {
            serverSocket.close();
        } catch (IOException ignore) {
        }
        serverSocket = null;
    }

    /**
     * @return Returns the bindURI.
     */
    public URI getBindURI() {
        return bindURI;
    }

    /**
     * @return Returns the connectURI.
     */
    public URI getConnectURI() {
        return connectURI;
    }

    public void start() throws IOException {
    }

    public void stop() throws IOException {
    }
    
    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return null;
    }    
    
    public String toString() {
        return "Socket Server: "+getConnectURI();
    }    
}