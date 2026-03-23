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

    public byte[] getReadData() {
        return readData != null ? readData : new byte[0];
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

                    /*
                     * 1) If data is read, continue below to the processCommand() call
                     *    and handle processing the data in the buffer. This takes priority
                     *    and some handshake status updates (like NEED_WRAP) can be handled
                     *    concurrently with application data (like TLSv1.3 key updates)
                     *    when the broker sends data to a client.
                     *
                     * 2) If no data is read, it's possible that the connection is waiting
                     *    for us to process a handshake update (either KeyUpdate for
                     *    TLS1.3 or renegotiation for TLSv1.2) so we need to check and process
                     *    any handshake updates. If the handshake status was updated,
                     *    we want to continue and loop again to recheck if we can now read new
                     *    application data into the buffer after processing the updates.
                     *
                     * 3) If no data is read, and no handshake update is needed, then we
                     *    are finished and can break.
                     */
                    if (readCount == 0 && !handleHandshakeUpdate()) {
                        break;
                    }

                    // channel is closed, cleanup
                    if (readCount == -1) {
                        onException(new EOFException());
                        break;
                    }

                    receiveCounter.addAndGet(readCount);
                }

                // Try and process commands if there is any data in plain if status is OK
                // Handshake renegotiation can happen concurrently with application data reads
                // so it's possible to have read data that needs processing even if the
                // handshake status indicates NEED_UNWRAP
                if (status == SSLEngineResult.Status.OK && plain.hasRemaining()) {
                    processCommand(plain);
                    //we have received enough bytes to detect the protocol
                    if (receiveCounter.get() >= 8) {
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
        ByteBuffer newBuffer = ByteBuffer.allocate(receiveCounter.get());
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
