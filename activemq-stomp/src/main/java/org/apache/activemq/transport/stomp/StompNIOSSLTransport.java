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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;

import javax.net.SocketFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;

import org.apache.activemq.transport.nio.NIOSSLTransport;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompNIOSSLTransport extends NIOSSLTransport {

    private final static Logger LOGGER = LoggerFactory.getLogger(StompNIOSSLTransport.class);

    StompCodec codec;

    private X509Certificate[] cachedPeerCerts;

    public StompNIOSSLTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);
    }

    public StompNIOSSLTransport(WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket, null, null, null);
    }

    public StompNIOSSLTransport(WireFormat wireFormat, Socket socket,
            SSLEngine engine, InitBuffer initBuffer, ByteBuffer inputBuffer) throws IOException {
        super(wireFormat, socket, engine, initBuffer, inputBuffer);
    }

    @Override
    public String getRemoteAddress() {
        String remoteAddress = super.getRemoteAddress();
        if (remoteAddress == null) {
            return remoteLocation.toString();
        }
        return remoteAddress;
    }

    @Override
    protected void initializeStreams() throws IOException {
        codec = new StompCodec(this);
        super.initializeStreams();
        if (inputBuffer.position() != 0 && inputBuffer.hasRemaining()) {
            serviceRead();
        }
    }

    @Override
    protected void processCommand(ByteBuffer plain) throws Exception {
        byte[] fill = new byte[plain.remaining()];
        plain.get(fill);
        ByteArrayInputStream input = new ByteArrayInputStream(fill);
        codec.parse(input, fill.length);
    }

    @Override
    public void doConsume(Object command) {
        StompFrame frame = (StompFrame) command;

        if (cachedPeerCerts == null) {
            cachedPeerCerts = getPeerCertificates();
        }
        frame.setTransportContext(cachedPeerCerts);

        super.doConsume(command);
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.transport.nio.NIOSSLTransport#doInit()
     */
    @Override
    protected void doInit() throws Exception {
        if (initBuffer != null) {
            nextFrameSize = -1;
            receiveCounter += initBuffer.readSize;
            initBuffer.buffer.flip();
            processCommand(initBuffer.buffer);
        }
        super.doInit();
    }
}
