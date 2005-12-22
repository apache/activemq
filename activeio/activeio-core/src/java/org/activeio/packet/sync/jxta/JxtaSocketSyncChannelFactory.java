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
package org.activeio.packet.sync.jxta;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.activeio.packet.sync.socket.SocketSyncChannelFactory;
import org.p2psockets.P2PServerSocket;
import org.p2psockets.P2PSocket;

/**
 * A SslSynchChannelFactory creates {@see org.activeio.net.TcpSynchChannel}
 * and {@see org.activeio.net.TcpSynchChannelServer} objects that use SSL.
 * 
 * @version $Revision$
 */
public class JxtaSocketSyncChannelFactory extends SocketSyncChannelFactory {

    static public final class JxtaServerSocketFactory extends ServerSocketFactory {

        private static JxtaServerSocketFactory defaultJxtaServerSocketFactory = new JxtaServerSocketFactory();
        public static ServerSocketFactory getDefault() {
            return defaultJxtaServerSocketFactory;
        }

        private JxtaServerSocketFactory() {}        
        
        public ServerSocket createServerSocket(int localPort) throws IOException {           
            return new P2PServerSocket(localPort);
        }

        public ServerSocket createServerSocket(int localPort, int backlog) throws IOException {
            return new P2PServerSocket(localPort, backlog);
        }

        public ServerSocket createServerSocket(int localPort, int backlog, InetAddress localHost) throws IOException {
            return new P2PServerSocket(localPort, backlog, localHost);
        }
    }

    static public final class JxtaSocketFactory extends SocketFactory {

        private static JxtaSocketFactory defaultJxtaSocketFactory = new JxtaSocketFactory();
        public static SocketFactory getDefault() {
            return defaultJxtaSocketFactory;
        }       
        private JxtaSocketFactory() {}
        
        public Socket createSocket(String remoteHost, int remotePort) throws IOException, UnknownHostException {
            return new P2PSocket(remoteHost, remotePort);
        }

        public Socket createSocket(String remoteHost, int remotePort, InetAddress localHost, int localPort) throws IOException, UnknownHostException {
            return new P2PSocket(remoteHost, remotePort, localHost, localPort);
        }

        public Socket createSocket(InetAddress remoteHost, int remotePort) throws IOException {
            return new P2PSocket(remoteHost, remotePort);
        }

        public Socket createSocket(InetAddress remoteHost, int remotePort, InetAddress localHost, int localPort) throws IOException {
            return new P2PSocket(remoteHost, remotePort, localHost, localPort);
        }
    }

    public JxtaSocketSyncChannelFactory() {
        super(JxtaSocketFactory.getDefault(), JxtaServerSocketFactory.getDefault());
    }
}
