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
package org.apache.activemq.transport.amqp;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.nio.NIOTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * A <a href="http://amqp.org/">AMQP</a> over NIO transport factory
 */
public class AmqpNioTransportFactory extends NIOTransportFactory implements BrokerServiceAware {

    private BrokerService brokerService = null;

    @Override
    protected String getDefaultWireFormatType() {
        return "amqp";
    }

    @Override
    protected TcpTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new TcpTransportServer(this, location, serverSocketFactory) {
            @Override
            protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
                return new AmqpNioTransport(format, socket);
            }
        };
    }

    @Override
    protected TcpTransport createTcpTransport(WireFormat wf, SocketFactory socketFactory, URI location, URI localLocation) throws UnknownHostException, IOException {
        return new AmqpNioTransport(wf, socketFactory, location, localLocation);
    }

    @Override
    public TcpTransport createTransport(WireFormat wireFormat, Socket socket,
            InitBuffer initBuffer) throws IOException {
        return new AmqpNioTransport(wireFormat, socket, initBuffer);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Transport serverConfigure(Transport transport, WireFormat format, HashMap options) throws Exception {
        transport = super.serverConfigure(transport, format, options);

        // strip off the mutex transport.
        if( transport instanceof MutexTransport ) {
            transport = ((MutexTransport)transport).getNext();
        }

        return transport;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        AmqpTransportFilter amqpTransport = new AmqpTransportFilter(transport, format, brokerService);

        Map<String, Object> wireFormatOptions = IntrospectionSupport.extractProperties(options, "wireFormat.");

        IntrospectionSupport.setProperties(amqpTransport, options);
        IntrospectionSupport.setProperties(amqpTransport.getWireFormat(), wireFormatOptions);

        return super.compositeConfigure(amqpTransport, format, options);
    }

    @Override
    protected Transport createInactivityMonitor(Transport transport, WireFormat format) {
        AmqpInactivityMonitor monitor = new AmqpInactivityMonitor(transport, format);
        AmqpTransportFilter filter = transport.narrow(AmqpTransportFilter.class);
        filter.setInactivityMonitor(monitor);
        return monitor;
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }
}

