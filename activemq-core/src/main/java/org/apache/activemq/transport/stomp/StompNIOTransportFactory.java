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
package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.nio.NIOTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * A <a href="http://stomp.codehaus.org/">STOMP</a> over NIO transport factory
 *
 *
 */
public class StompNIOTransportFactory extends NIOTransportFactory implements BrokerServiceAware {

    private BrokerContext brokerContext = null;

    protected String getDefaultWireFormatType() {
        return "stomp";
    }

    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new TcpTransportServer(this, location, serverSocketFactory) {
            protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
                return new StompNIOTransport(format, socket);
            }
        };
    }

    protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws UnknownHostException, IOException {
        return new StompNIOTransport(wf, socketFactory, location, localLocation);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Transport serverConfigure(Transport transport, WireFormat format, HashMap options) throws Exception {
        transport = super.serverConfigure(transport, format, options);

        MutexTransport mutex = transport.narrow(MutexTransport.class);
        if (mutex != null) {
            mutex.setSyncOnCommand(true);
        }

        return transport;
    }

    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        transport = new StompTransportFilter(transport, format, brokerContext);
        IntrospectionSupport.setProperties(transport, options);
        return super.compositeConfigure(transport, format, options);
    }

    public void setBrokerService(BrokerService brokerService) {
        this.brokerContext = brokerService.getBrokerContext();
    }

}

