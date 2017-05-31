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
package org.apache.activemq.transport.auto.nio;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLEngine;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.openwire.OpenWireFormatFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.auto.AutoTcpTransportServer;
import org.apache.activemq.transport.auto.AutoTransportUtils;
import org.apache.activemq.transport.nio.NIOSSLTransport;
import org.apache.activemq.transport.nio.NIOSSLTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 *
 *
 */
public class AutoNioSslTransportFactory extends NIOSSLTransportFactory implements BrokerServiceAware {
    protected BrokerService brokerService;

    /* (non-Javadoc)
     * @see org.apache.activemq.broker.BrokerServiceAware#setBrokerService(org.apache.activemq.broker.BrokerService)
     */
    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @Override
    protected AutoNIOSSLTransportServer createTcpTransportServer(URI location, ServerSocketFactory serverSocketFactory) throws IOException, URISyntaxException {
        return new AutoNIOSSLTransportServer(context, this, location, serverSocketFactory, brokerService, enabledProtocols) {

            @Override
            protected Transport createTransport(Socket socket, WireFormat format, SSLEngine engine, InitBuffer initBuffer,
                     ByteBuffer inputBuffer, TcpTransportFactory detectedFactory) throws IOException {
                NIOSSLTransport nioSslTransport = (NIOSSLTransport) detectedFactory.createTransport(
                        format, socket, engine, initBuffer, inputBuffer);

                if (format.getClass().toString().contains("MQTT")) {
                    if (!allowLinkStealingSet) {
                        this.setAllowLinkStealing(true);
                    }
                }

                if (context != null) {
                    nioSslTransport.setSslContext(context);
                }

                nioSslTransport.setNeedClientAuth(isNeedClientAuth());
                nioSslTransport.setWantClientAuth(isWantClientAuth());

                return nioSslTransport;
            }

        };

    }

    boolean allowLinkStealingSet = false;
    private Set<String> enabledProtocols;

    @Override
    public TransportServer doBind(final URI location) throws IOException {
        try {
            if (SslContext.getCurrentSslContext() != null) {
                try {
                    context = SslContext.getCurrentSslContext().getSSLContext();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }

            Map<String, String> options = new HashMap<String, String>(URISupport.parseParameters(location));

            Map<String, Object> autoProperties = IntrospectionSupport.extractProperties(options, "auto.");
            this.enabledProtocols = AutoTransportUtils.parseProtocols((String) autoProperties.get("protocols"));

            ServerSocketFactory serverSocketFactory = createServerSocketFactory();
            AutoTcpTransportServer server = createTcpTransportServer(location, serverSocketFactory);
            server.setWireFormatFactory(new OpenWireFormatFactory());
            if (options.get("allowLinkStealing") != null){
                allowLinkStealingSet = true;
            }
            IntrospectionSupport.setProperties(server, options);
            server.setAutoTransportOptions(IntrospectionSupport.extractProperties(options, "auto."));
            server.setTransportOption(IntrospectionSupport.extractProperties(options, "transport."));
            server.setWireFormatOptions(AutoTransportUtils.extractWireFormatOptions(options));
            server.bind();

            return server;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }


}
