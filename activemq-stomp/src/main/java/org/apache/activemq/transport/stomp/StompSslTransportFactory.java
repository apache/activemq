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
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;

import org.apache.activemq.broker.BrokerContext;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.transport.tcp.SslTransportFactory;
import org.apache.activemq.transport.tcp.SslTransportServer;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * A <a href="http://stomp.codehaus.org/">STOMP</a> over SSL transport factory
 *
 *
 */
public class StompSslTransportFactory extends SslTransportFactory implements BrokerServiceAware {

    private BrokerContext brokerContext = null;

    @Override
    protected String getDefaultWireFormatType() {
        return "stomp";
    }

    @Override
    protected SslTransportServer createSslTransportServer(final URI location, SSLServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new SslTransportServer(this, location, serverSocketFactory) {

            @Override
            protected Transport createTransport(Socket socket, WireFormat format) throws IOException {
                return new SslTransport(format, (SSLSocket)socket) {

                    private X509Certificate[] cachedPeerCerts;

                    @Override
                    public void doConsume(Object command) {
                        StompFrame frame = (StompFrame) command;
                        if (cachedPeerCerts == null) {
                            cachedPeerCerts = getPeerCertificates();
                        }
                        frame.setTransportContext(cachedPeerCerts);
                        super.doConsume(command);
                    }
                };
            }
        };
    }

    @Override
    public SslTransport createTransport(WireFormat wireFormat, Socket socket, InitBuffer initBuffer)
            throws IOException {

        return new SslTransport(wireFormat, (SSLSocket)socket, initBuffer) {

            private X509Certificate[] cachedPeerCerts;

            @Override
            public void doConsume(Object command) {
                StompFrame frame = (StompFrame) command;
                if (cachedPeerCerts == null) {
                    cachedPeerCerts = getPeerCertificates();
                }
                frame.setTransportContext(cachedPeerCerts);
                super.doConsume(command);
            }
        };
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {
        transport = new StompTransportFilter(transport, format, brokerContext);
        IntrospectionSupport.setProperties(transport, options);
        return super.compositeConfigure(transport, format, options);
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

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerContext = brokerService.getBrokerContext();
    }

    @Override
    protected Transport createInactivityMonitor(Transport transport, WireFormat format) {
        StompInactivityMonitor monitor = new StompInactivityMonitor(transport, format);

        StompTransportFilter filter = (StompTransportFilter) transport.narrow(StompTransportFilter.class);
        filter.setInactivityMonitor(monitor);

        return monitor;
    }
}
