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

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import javax.net.SocketFactory;

import org.apache.activemq.transport.nio.NIOOutputStream;
import org.apache.activemq.transport.nio.SelectorManager;
import org.apache.activemq.transport.nio.SelectorSelection;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link org.apache.activemq.transport.Transport} interface for using AMQP over NIO
 */
public class AmqpNioTransport extends TcpTransport {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpNioTransport.class);

    private SocketChannel channel;
    private SelectorSelection selection;
    private final AmqpFrameParser frameReader = new AmqpFrameParser(this);

    private ByteBuffer inputBuffer;

    public AmqpNioTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);

        frameReader.setWireFormat((AmqpWireFormat) wireFormat);
    }

    public AmqpNioTransport(WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket);

        frameReader.setWireFormat((AmqpWireFormat) wireFormat);
    }

    public AmqpNioTransport(WireFormat wireFormat, Socket socket, InitBuffer initBuffer) throws IOException {
        super(wireFormat, socket, initBuffer);

        frameReader.setWireFormat((AmqpWireFormat) wireFormat);
    }

    @Override
    protected void initializeStreams() throws IOException {
        channel = socket.getChannel();
        channel.configureBlocking(false);
        // listen for events telling us when the socket is readable.
        selection = SelectorManager.getInstance().register(channel, new SelectorManager.Listener() {
            @Override
            public void onSelect(SelectorSelection selection) {
                if (!isStopped()) {
                    serviceRead();
                }
            }

            @Override
            public void onError(SelectorSelection selection, Throwable error) {
                LOG.trace("Error detected: {}", error.getMessage());
                if (error instanceof IOException) {
                    onException((IOException) error);
                } else {
                    onException(IOExceptionSupport.create(error));
                }
            }
        });

        inputBuffer = ByteBuffer.allocate(8 * 1024);
        NIOOutputStream outPutStream = new NIOOutputStream(channel, 8 * 1024);
        this.dataOut = new DataOutputStream(outPutStream);
        this.buffOut = outPutStream;

        try {
            if (initBuffer != null) {
                processBuffer(initBuffer.buffer, initBuffer.readSize);
            }
        } catch (IOException e) {
            onException(e);
        } catch (Throwable e) {
            onException(IOExceptionSupport.create(e));
        }

    }

    boolean magicRead = false;

    private void serviceRead() {
        try {

            while (isStarted()) {
                // read channel
                int readSize = channel.read(inputBuffer);

                // channel is closed, cleanup
                if (readSize == -1) {
                    onException(new EOFException());
                    selection.close();
                    break;
                }
                // nothing more to read, break
                if (readSize == 0) {
                    break;
                }

                processBuffer(inputBuffer, readSize);
            }
        } catch (IOException e) {
            onException(e);
        } catch (Throwable e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    protected void processBuffer(ByteBuffer buffer, int readSize) throws Exception {
        receiveCounter += readSize;

        buffer.flip();
        frameReader.parse(buffer);
        buffer.clear();
    }

    @Override
    protected void doStart() throws Exception {
        connect();
        selection.setInterestOps(SelectionKey.OP_READ);
        selection.enable();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        try {
            if (selection != null) {
                selection.close();
            }
        } finally {
            super.doStop(stopper);
        }
    }
}