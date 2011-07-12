/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.wireformat.WireFormat;

public class NIOTransportFactory extends TcpTransportFactory {

    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new TcpTransportServer(this, location, serverSocketFactory) {
            protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
                return new NIOTransport(format, socket);
            }
        };
    }

    protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws UnknownHostException, IOException {
        return new NIOTransport(wf, socketFactory, location, localLocation);
    }

    protected ServerSocketFactory createServerSocketFactory() {
        return new ServerSocketFactory() {
            public ServerSocket createServerSocket(int port) throws IOException {
                ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
                serverSocketChannel.socket().bind(new InetSocketAddress(port));
                return serverSocketChannel.socket();
            }

            public ServerSocket createServerSocket(int port, int backlog) throws IOException {
                ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
                serverSocketChannel.socket().bind(new InetSocketAddress(port), backlog);
                return serverSocketChannel.socket();
            }

            public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress) throws IOException {
                ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
                serverSocketChannel.socket().bind(new InetSocketAddress(ifAddress, port), backlog);
                return serverSocketChannel.socket();
            }
        };
    }

    protected SocketFactory createSocketFactory() throws IOException {
        return new SocketFactory() {

            public Socket createSocket() throws IOException {
                SocketChannel channel = SocketChannel.open();
                return channel.socket();
            }

            public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
                SocketChannel channel = SocketChannel.open();
                channel.connect(new InetSocketAddress(host, port));
                return channel.socket();
            }

            public Socket createSocket(InetAddress address, int port) throws IOException {
                SocketChannel channel = SocketChannel.open();
                channel.connect(new InetSocketAddress(address, port));
                return channel.socket();
            }

            public Socket createSocket(String address, int port, InetAddress localAddresss, int localPort) throws IOException, UnknownHostException {
                SocketChannel channel = SocketChannel.open();
                channel.socket().bind(new InetSocketAddress(localAddresss, localPort));
                channel.connect(new InetSocketAddress(address, port));
                return channel.socket();
            }

            public Socket createSocket(InetAddress address, int port, InetAddress localAddresss, int localPort) throws IOException {
                SocketChannel channel = SocketChannel.open();
                channel.socket().bind(new InetSocketAddress(localAddresss, localPort));
                channel.connect(new InetSocketAddress(address, port));
                return channel.socket();
            }
        };
    }
}
