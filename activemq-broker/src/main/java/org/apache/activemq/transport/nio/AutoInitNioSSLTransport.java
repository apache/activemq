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

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLParameters;

import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;

/**
 * This transport initializes the SSLEngine and reads the first command before
 * handing off to the detected transport.
 *
 */
public class AutoInitNioSSLTransport extends NIOSSLTransport {

    public AutoInitNioSSLTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);
    }

    public AutoInitNioSSLTransport(WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket, null, null, null);
    }

    @Override
    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public ByteBuffer getInputBuffer() {
        return this.inputBuffer;
    }

    @Override
    protected void initializeStreams() throws IOException {
        NIOOutputStream outputStream = null;
        try {
            channel = socket.getChannel();
            channel.configureBlocking(false);

            if (sslContext == null) {
                sslContext = SSLContext.getDefault();
            }

            String remoteHost = null;
            int remotePort = -1;

            try {
                URI remoteAddress = new URI(this.getRemoteAddress());
                remoteHost = remoteAddress.getHost();
                remotePort = remoteAddress.getPort();
            } catch (Exception e) {
            }

            // initialize engine, the initial sslSession we get will need to be
            // updated once the ssl handshake process is completed.
            if (remoteHost != null && remotePort != -1) {
                sslEngine = sslContext.createSSLEngine(remoteHost, remotePort);
            } else {
                sslEngine = sslContext.createSSLEngine();
            }

            if (verifyHostName) {
                SSLParameters sslParams = new SSLParameters();
                sslParams.setEndpointIdentificationAlgorithm("HTTPS");
                sslEngine.setSSLParameters(sslParams);
            }

            sslEngine.setUseClientMode(false);
            if (enabledCipherSuites != null) {
                sslEngine.setEnabledCipherSuites(enabledCipherSuites);
            }

            if (enabledProtocols != null) {
                sslEngine.setEnabledProtocols(enabledProtocols);
            }

            if (wantClientAuth) {
                sslEngine.setWantClientAuth(wantClientAuth);
            }

            if (needClientAuth) {
                sslEngine.setNeedClientAuth(needClientAuth);
            }

            sslSession = sslEngine.getSession();

            inputBuffer = ByteBuffer.allocate(sslSession.getPacketBufferSize());
            inputBuffer.clear();

            outputStream = new NIOOutputStream(channel);
            outputStream.setEngine(sslEngine);
            this.dataOut = new DataOutputStream(outputStream);
            this.buffOut = outputStream;
            sslEngine.beginHandshake();
            handshakeStatus = sslEngine.getHandshakeStatus();
            doHandshake();

        } catch (Exception e) {
            try {
                if(outputStream != null) {
                    outputStream.close();
                }
                super.closeStreams();
            } catch (Exception ex) {}
            throw new IOException(e);
        }
    }

    @Override
    protected void doOpenWireInit() throws Exception {

    }

    public SSLEngine getSslSession() {
        return this.sslEngine;
    }

    private volatile byte[] readData;

    private final AtomicInteger readSize = new AtomicInteger();

    public byte[] getReadData() {
        return readData != null ? readData : new byte[0];
    }

    public AtomicInteger getReadSize() {
        return readSize;
    }

    @Override
    public void serviceRead() {
        try {
            if (handshakeInProgress) {
                doHandshake();
            }

            ByteBuffer plain = ByteBuffer.allocate(sslSession.getApplicationBufferSize());
            plain.position(plain.limit());

            while (true) {
                //If the transport was already stopped then break
                if (this.isStopped()) {
                    return;
                }

                if (!plain.hasRemaining()) {
                    int readCount = secureRead(plain);

                    if (readCount == 0) {
                        break;
                    }

                    // channel is closed, cleanup
                    if (readCount == -1) {
                        onException(new EOFException());
                        break;
                    }

                    receiveCounter += readCount;
                    readSize.addAndGet(readCount);
                }

                if (status == SSLEngineResult.Status.OK && handshakeStatus != SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                    processCommand(plain);
                    //we have received enough bytes to detect the protocol
                    if (receiveCounter >= 8) {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            onException(e);
        } catch (Throwable e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    @Override
    protected void processCommand(ByteBuffer plain) throws Exception {
        ByteBuffer newBuffer = ByteBuffer.allocate(receiveCounter);
        if (readData != null) {
            newBuffer.put(readData);
        }
        newBuffer.put(plain);
        newBuffer.flip();
        readData = newBuffer.array();
    }


    @Override
    public void doStart() throws Exception {
        taskRunnerFactory = new TaskRunnerFactory("ActiveMQ NIOSSLTransport Task");
        // no need to init as we can delay that until demand (eg in doHandshake)
        connect();
    }


    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        if (taskRunnerFactory != null) {
            taskRunnerFactory.shutdownNow();
            taskRunnerFactory = null;
        }
    }


}
