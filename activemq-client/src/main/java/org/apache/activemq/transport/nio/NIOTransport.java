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
package org.apache.activemq.transport.nio;

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

import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;

/**
 * An implementation of the {@link Transport} interface using raw tcp/ip
 *
 *
 */
public class NIOTransport extends TcpTransport {

    // private static final Logger log = LoggerFactory.getLogger(NIOTransport.class);
    protected SocketChannel channel;
    protected SelectorSelection selection;
    protected ByteBuffer inputBuffer;
    protected ByteBuffer currentBuffer;
    protected int nextFrameSize;

    public NIOTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);
    }

    public NIOTransport(WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket);
    }

    /**
     * @param format
     * @param socket
     * @param initBuffer
     * @throws IOException
     */
    public NIOTransport(WireFormat format, Socket socket, InitBuffer initBuffer) throws IOException {
        super(format, socket, initBuffer);
    }

    @Override
    protected void initializeStreams() throws IOException {
        channel = socket.getChannel();
        channel.configureBlocking(false);

        // listen for events telling us when the socket is readable.
        selection = SelectorManager.getInstance().register(channel, new SelectorManager.Listener() {
            @Override
            public void onSelect(SelectorSelection selection) {
                serviceRead();
            }

            @Override
            public void onError(SelectorSelection selection, Throwable error) {
                if (error instanceof IOException) {
                    onException((IOException)error);
                } else {
                    onException(IOExceptionSupport.create(error));
                }
            }
        });

        // Send the data via the channel
        // inputBuffer = ByteBuffer.allocateDirect(8*1024);
        inputBuffer = ByteBuffer.allocateDirect(getIoBufferSize());
        currentBuffer = inputBuffer;
        nextFrameSize = -1;
        currentBuffer.limit(4);
        NIOOutputStream outPutStream = new NIOOutputStream(channel, getIoBufferSize());
        this.dataOut = new DataOutputStream(outPutStream);
        this.buffOut = outPutStream;
    }

    protected int readFromBuffer() throws IOException {
        return channel.read(currentBuffer);
    }

    protected void serviceRead() {
        try {
            while (true) {

                int readSize = readFromBuffer();
                if (readSize == -1) {
                    onException(new EOFException());
                    selection.close();
                    break;
                }
                if (readSize == 0) {
                    break;
                }

                this.receiveCounter += readSize;
                if (currentBuffer.hasRemaining()) {
                    continue;
                }

                // Are we trying to figure out the size of the next frame?
                if (nextFrameSize == -1) {
                    assert inputBuffer == currentBuffer;

                    // If the frame is too big to fit in our direct byte buffer,
                    // Then allocate a non direct byte buffer of the right size
                    // for it.
                    inputBuffer.flip();
                    nextFrameSize = inputBuffer.getInt() + 4;

                    if (wireFormat instanceof OpenWireFormat) {
                        long maxFrameSize = ((OpenWireFormat)wireFormat).getMaxFrameSize();
                        if (nextFrameSize > maxFrameSize) {
                            throw new IOException("Frame size of " + (nextFrameSize / (1024 * 1024)) + " MB larger than max allowed " + (maxFrameSize / (1024 * 1024)) + " MB");
                        }
                    }

                    if (nextFrameSize > inputBuffer.capacity()) {
                        currentBuffer = ByteBuffer.allocateDirect(nextFrameSize);
                        currentBuffer.putInt(nextFrameSize);
                    } else {
                        inputBuffer.limit(nextFrameSize);
                    }

                } else {
                    currentBuffer.flip();

                    Object command = wireFormat.unmarshal(new DataInputStream(new NIOInputStream(currentBuffer)));
                    doConsume(command);

                    nextFrameSize = -1;
                    inputBuffer.clear();
                    inputBuffer.limit(4);
                    currentBuffer = inputBuffer;
                }

            }

        } catch (IOException e) {
            onException(e);
        } catch (Throwable e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    @Override
    protected void doStart() throws Exception {
        connect();
        selection.setInterestOps(SelectionKey.OP_READ);
        selection.enable();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        if (selection != null) {
            selection.close();
            selection = null;
        }
        super.doStop(stopper);
    }
}
