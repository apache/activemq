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
import java.net.UnknownHostException;

import javax.net.SocketFactory;

import org.apache.activemq.transport.nio.NIOTransport;
import org.apache.activemq.wireformat.WireFormat;

/**
 *
 *
 */
public class AutoNIOTransport extends NIOTransport {

    public AutoNIOTransport(WireFormat format, Socket socket,
            InitBuffer initBuffer) throws IOException {
        super(format, socket, initBuffer);
    }

    public AutoNIOTransport(WireFormat wireFormat, Socket socket)
            throws IOException {
        super(wireFormat, socket);
    }

    public AutoNIOTransport(WireFormat wireFormat, SocketFactory socketFactory,
            URI remoteLocation, URI localLocation) throws UnknownHostException,
            IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);
    }


    boolean doneInitBuffer = false;

    /**
     * Read from the initial buffer if it is set
     */
    @Override
    protected int readFromBuffer() throws IOException {
        int readSize = 0;
        if (!doneInitBuffer) {
            if (initBuffer == null || initBuffer.readSize < 8) {
                throw new IOException("Protocol type could not be determined.");
            }
            if (nextFrameSize == -1) {
                readSize = 4;
                this.initBuffer.buffer.flip();
                if (this.initBuffer.buffer.remaining() < 8) {
                    throw new IOException("Protocol type could not be determined.");
                }
                for (int i = 0; i < 4; i++) {
                    currentBuffer.put(initBuffer.buffer.get());
                }
            } else {
                for (int i = 0; i < 4; i++) {
                    currentBuffer.put(initBuffer.buffer.get());
                }
                readSize = 4;
                doneInitBuffer = true;
            }

        } else {
            readSize += channel.read(currentBuffer);
        }
        return readSize;
    }


}
