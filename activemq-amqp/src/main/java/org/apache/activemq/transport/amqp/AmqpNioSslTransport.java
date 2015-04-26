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
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import javax.net.SocketFactory;

import org.apache.activemq.transport.nio.NIOSSLTransport;
import org.apache.activemq.wireformat.WireFormat;

public class AmqpNioSslTransport extends NIOSSLTransport {

    private final AmqpFrameParser frameReader = new AmqpFrameParser(this);

    public AmqpNioSslTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);

        frameReader.setWireFormat((AmqpWireFormat) wireFormat);
    }

    public AmqpNioSslTransport(WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket);

        frameReader.setWireFormat((AmqpWireFormat) wireFormat);
    }

    @Override
    protected void initializeStreams() throws IOException {
        super.initializeStreams();
        if (inputBuffer.position() != 0 && inputBuffer.hasRemaining()) {
            serviceRead();
        }
    }

    @Override
    protected void processCommand(ByteBuffer plain) throws Exception {
        frameReader.parse(plain);
    }
}