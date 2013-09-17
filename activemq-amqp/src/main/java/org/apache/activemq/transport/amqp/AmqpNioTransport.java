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

import java.io.DataInputStream;
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

import org.apache.activemq.transport.nio.NIOInputStream;
import org.apache.activemq.transport.nio.NIOOutputStream;
import org.apache.activemq.transport.nio.SelectorManager;
import org.apache.activemq.transport.nio.SelectorSelection;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtbuf.Buffer;

/**
 * An implementation of the {@link org.apache.activemq.transport.Transport} interface for using AMQP over NIO
 */
public class AmqpNioTransport extends TcpTransport {

    private SocketChannel channel;
    private SelectorSelection selection;

    private ByteBuffer inputBuffer;

    public AmqpNioTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);
    }

    public AmqpNioTransport(WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket);
    }

    protected void initializeStreams() throws IOException {
        channel = socket.getChannel();
        channel.configureBlocking(false);
        // listen for events telling us when the socket is readable.
        selection = SelectorManager.getInstance().register(channel, new SelectorManager.Listener() {
            public void onSelect(SelectorSelection selection) {
                if (!isStopped()) {
                    serviceRead();
                }
            }

            public void onError(SelectorSelection selection, Throwable error) {
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

                receiveCounter += readSize;

                inputBuffer.flip();

                if( !magicRead ) {
                    if( inputBuffer.remaining()>= 8 ) {
                        magicRead = true;
                        Buffer magic = new Buffer(8);
                        for (int i = 0; i < 8; i++) {
                            magic.data[i] = inputBuffer.get();
                        }
                        doConsume(new AmqpHeader(magic));
                    } else {
                        inputBuffer.flip();
                        continue;
                    }
                }

                doConsume(AmqpSupport.toBuffer(inputBuffer));
                // clear the buffer
                inputBuffer.clear();

            }
        } catch (IOException e) {
            onException(e);
        } catch (Throwable e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    protected void doStart() throws Exception {
        connect();
        selection.setInterestOps(SelectionKey.OP_READ);
        selection.enable();
    }

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