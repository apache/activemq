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
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;

import javax.net.SocketFactory;

import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.nio.NIOOutputStream;
import org.apache.activemq.transport.nio.SelectorManager;
import org.apache.activemq.transport.nio.SelectorSelection;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.DataByteArrayInputStream;
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

    private ByteBuffer inputBuffer;
    ByteArrayOutputStream currentCommand = new ByteArrayOutputStream();
    boolean processedHeaders = false;
    String action;
    HashMap<String, String> headers;
    int contentLength = -1;
    int readLength = 0;
    int previousByte = -1;

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

        inputBuffer = ByteBuffer.allocate(8 * 1024);
        NIOOutputStream outPutStream = new NIOOutputStream(channel, 8 * 1024);
        this.dataOut = new DataOutputStream(outPutStream);
        this.buffOut = outPutStream;
    }

    private void serviceRead() {
        try {

           while (true) {
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

               inputBuffer.flip();

               int b;
               ByteArrayInputStream input = new ByteArrayInputStream(inputBuffer.array());

               int i = 0;
               while(i++ < readSize) {
                   b = input.read();
                   // skip repeating nulls
                   if (!processedHeaders && previousByte == 0 && b == 0) {
                       continue;
                   }

                   if (!processedHeaders) {
                       currentCommand.write(b);
                       // end of headers section, parse action and header
                       if (previousByte == '\n' && b == '\n') {
                           if (wireFormat instanceof StompWireFormat) {
                               DataByteArrayInputStream data = new DataByteArrayInputStream(currentCommand.toByteArray());
                               action = ((StompWireFormat)wireFormat).parseAction(data);
                               headers = ((StompWireFormat)wireFormat).parseHeaders(data);
                               String contentLengthHeader = headers.get(Stomp.Headers.CONTENT_LENGTH);
                               if (contentLengthHeader != null) {
                                   contentLength = ((StompWireFormat)wireFormat).parseContentLength(contentLengthHeader);
                               } else {
                                   contentLength = -1;
                               }
                           }
                           processedHeaders = true;
                           currentCommand.reset();
                       }
                   } else {

                       if (contentLength == -1) {
                           // end of command reached, unmarshal
                           if (b == 0) {
                               processCommand();
                           } else {
                               currentCommand.write(b);
                           }
                       } else {
                           // read desired content length
                           if (readLength++ == contentLength) {
                               processCommand();
                               readLength = 0;
                           } else {
                               currentCommand.write(b);
                           }
                       }
                   }

                   previousByte = b;
               }
               // clear the buffer
               inputBuffer.clear();

           }
        } catch (IOException e) {
            onException(e);
        } catch (Throwable e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    private void processCommand() throws Exception {
        StompFrame frame = new StompFrame(action, headers, currentCommand.toByteArray());
        doConsume(frame);
        processedHeaders = false;
        currentCommand.reset();
        contentLength = -1;
    }

    protected void doStart() throws Exception {
        connect();
        selection.setInterestOps(SelectionKey.OP_READ);
        selection.enable();
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        try {
            selection.close();
        } catch (Exception e) {
        	e.printStackTrace();
        }
        super.doStop(stopper);
    }
}
