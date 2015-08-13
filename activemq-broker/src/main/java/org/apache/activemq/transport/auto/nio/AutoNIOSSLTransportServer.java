package org.apache.activemq.transport.auto.nio;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.auto.AutoTcpTransportServer;
import org.apache.activemq.transport.nio.AutoInitNioSSLTransport;
import org.apache.activemq.transport.nio.NIOSSLTransport;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.transport.tcp.TcpTransport.InitBuffer;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransportServer;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class AutoNIOSSLTransportServer extends AutoTcpTransportServer {

    private static final Logger LOG = LoggerFactory.getLogger(AutoNIOSSLTransportServer.class);

    private SSLContext context;

    public AutoNIOSSLTransportServer(SSLContext context, TcpTransportFactory transportFactory, URI location, ServerSocketFactory serverSocketFactory,
            BrokerService brokerService, Set<String> enabledProtocols) throws IOException, URISyntaxException {
        super(transportFactory, location, serverSocketFactory, brokerService, enabledProtocols);

        this.context = context;
    }

    private boolean needClientAuth;
    private boolean wantClientAuth;

    protected Transport createTransport(Socket socket, WireFormat format, SSLEngine engine,
            InitBuffer initBuffer, ByteBuffer inputBuffer, TcpTransportFactory detectedFactory) throws IOException {
        NIOSSLTransport transport = new NIOSSLTransport(format, socket, engine, initBuffer, inputBuffer);
        if (context != null) {
            transport.setSslContext(context);
        }

        transport.setNeedClientAuth(needClientAuth);
        transport.setWantClientAuth(wantClientAuth);


        return transport;
    }

    @Override
    protected TcpTransport createTransport(Socket socket, WireFormat format) throws IOException {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public boolean isSslServer() {
        return true;
    }

    public boolean isNeedClientAuth() {
        return this.needClientAuth;
    }

    public void setNeedClientAuth(boolean value) {
        this.needClientAuth = value;
    }

    public boolean isWantClientAuth() {
        return this.wantClientAuth;
    }

    public void setWantClientAuth(boolean value) {
        this.wantClientAuth = value;
    }


    @Override
    protected TransportInfo configureTransport(final TcpTransportServer server, final Socket socket) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();

        //The SSLEngine needs to be initialized and handshake done to get the first command and detect the format
        final AutoInitNioSSLTransport in = new AutoInitNioSSLTransport(wireFormatFactory.createWireFormat(), socket);
        if (context != null) {
            in.setSslContext(context);
        }
        in.start();
        SSLEngine engine = in.getSslSession();

        Future<?> future = executor.submit(new Runnable() {
            @Override
            public void run() {
                //Wait for handshake to finish initializing
                do {
                    in.serviceRead();
                } while(in.getReadSize().get() < 8);
            }
        });

        waitForProtocolDetectionFinish(future, in.getReadSize());
        in.stop();

        initBuffer = new InitBuffer(in.getReadSize().get(), ByteBuffer.allocate(in.getReadData().length));
        initBuffer.buffer.put(in.getReadData());

        ProtocolInfo protocolInfo = detectProtocol(in.getReadData());

        if (protocolInfo.detectedTransportFactory instanceof BrokerServiceAware) {
            ((BrokerServiceAware) protocolInfo.detectedTransportFactory).setBrokerService(brokerService);
        }

        WireFormat format = protocolInfo.detectedWireFormatFactory.createWireFormat();
        Transport transport = createTransport(socket, format, engine, initBuffer, in.getInputBuffer(), protocolInfo.detectedTransportFactory);

        return new TransportInfo(format, transport, protocolInfo.detectedTransportFactory);
    }


}


