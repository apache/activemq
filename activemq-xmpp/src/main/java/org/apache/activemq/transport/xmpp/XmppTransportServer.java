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
package org.apache.activemq.transport.xmpp;

import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.wireformat.WireFormat;

import javax.net.ServerSocketFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.Socket;
import java.io.IOException;

/**
 * @version $Revision$
 */
public class XmppTransportServer extends TcpTransportServer {

    public XmppTransportServer(TcpTransportFactory transportFactory, URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        super(transportFactory, location, serverSocketFactory);
    }

    @Override
    protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
        return new XmppTransport(format, socket);
    }
}
