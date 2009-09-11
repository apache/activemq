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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import javax.net.SocketFactory;

import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.nio.NIOBufferedInputStream;
import org.apache.activemq.transport.nio.NIOOutputStream;
import org.apache.activemq.transport.nio.SelectorManager;
import org.apache.activemq.transport.nio.SelectorSelection;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;

/**
 * An implementation of the {@link Transport} interface for using Stomp over NIO
 * 
 * @version $Revision$
 */
public class StompNIOTransport extends TcpTransport {

    private SocketChannel channel;
    private SelectorSelection selection;

    public StompNIOTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);
    }

    public StompNIOTransport(WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket);
    }

    protected void initializeStreams() throws IOException {
        channel = socket.getChannel();
        channel.configureBlocking(false);

        // listen for events telling us when the socket is readable.
        selection = SelectorManager.getInstance().register(channel, new SelectorManager.Listener() {
            public void onSelect(SelectorSelection selection) {
                serviceRead();
            }

            public void onError(SelectorSelection selection, Throwable error) {
                if (error instanceof IOException) {
                    onException((IOException)error);
                } else {
                    onException(IOExceptionSupport.create(error));
                }
            }
        });

        this.dataOut = new DataOutputStream(new NIOOutputStream(channel, 8 * 1024));
    }

    private void serviceRead() {
        try {
            DataInputStream in = new DataInputStream(new NIOBufferedInputStream(channel, 8 * 1024));
            while (true) {
                Object command = wireFormat.unmarshal(in);
                doConsume((Command)command);
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
        selection.disable();
        super.doStop(stopper);
    }
}
