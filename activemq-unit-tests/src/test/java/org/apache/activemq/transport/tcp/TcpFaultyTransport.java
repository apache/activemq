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
import java.net.URI;
import java.net.UnknownHostException;

import org.apache.activemq.Service;
import org.apache.activemq.transport.Transport;

import org.apache.activemq.wireformat.WireFormat;

import javax.net.SocketFactory;

/**
 * An implementation of the {@link Transport} interface using raw tcp/ip
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com (logging improvement modifications)
 * 
 */
public class TcpFaultyTransport extends TcpTransport implements Transport, Service, Runnable {

    public TcpFaultyTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation,
                        URI localLocation) throws UnknownHostException, IOException {
	super(wireFormat, socketFactory, remoteLocation, localLocation);
    }

    /**
     * @return pretty print of 'this'
     */
    public String toString() {
        return "tcpfaulty://" + socket.getInetAddress() + ":" + socket.getPort();
    }
}
