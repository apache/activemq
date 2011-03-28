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
package org.apache.activemq.transport.tcp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.activemq.util.ServiceListener;
import org.apache.activemq.transport.tcp.TcpTransportServer;

import javax.net.ServerSocketFactory;

/**
 * A TCP based implementation of {@link TransportServer}
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com (logging improvement modifications)
 * 
 */

public class TcpFaultyTransportServer extends TcpTransportServer implements ServiceListener{

    public TcpFaultyTransportServer(TcpFaultyTransportFactory transportFactory, URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
	super(transportFactory, location, serverSocketFactory);
    }

    /**
     * @return pretty print of this
     */
    public String toString() {
        return "" + getBindLocation();
    }
}
