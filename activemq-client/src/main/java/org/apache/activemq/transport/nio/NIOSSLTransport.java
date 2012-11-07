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
import java.security.cert.X509Certificate;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NIOSSLTransport extends NIOTransport {

    private static final Logger LOG = LoggerFactory.getLogger(NIOSSLTransport.class);

    protected boolean needClientAuth;
    protected boolean wantClientAuth;
    protected String[] enabledCipherSuites;

    protected SSLContext sslContext;
    protected SSLEngine sslEngine;
    protected SSLSession sslSession;

    protected volatile boolean handshakeInProgress = false;
    protected SSLEngineResult.Status status = null;
    protected SSLEngineResult.HandshakeStatus handshakeStatus = null;
    protected TaskRunnerFactory taskRunnerFactory;

    public NIOSSLTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);
    }

    public NIOSSLTransport(WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket);
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    protected void initializeStreams() throws IOException {
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

            sslEngine.setUseClientMode(false);
            if (enabledCipherSuites != null) {
                sslEngine.setEnabledCipherSuites(enabledCipherSuites);
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

            NIOOutputStream outputStream = new NIOOutputStream(channel);
            outputStream.setEngine(sslEngine);
            this.dataOut = new DataOutputStream(outputStream);
            this.buffOut = outputStream;
            sslEngine.beginHandshake();
            handshakeStatus = sslEngine.getHandshakeStatus();
            doHandshake();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected void finishHandshake() throws Exception {
        if (handshakeInProgress) {
            handshakeInProgress = false;
            nextFrameSize = -1;

            // Once handshake completes we need to ask for the now real sslSession
            // otherwise the session would return 'SSL_NULL_WITH_NULL_NULL' for the
            // cipher suite.
            sslSession = sslEngine.getSession();

            // listen for events telling us when the socket is readable.
            selection = SelectorManager.getInstance().register(channel, new SelectorManager.Listener() {
                public void onSelect(SelectorSelection selection) {
                    serviceRead();
                }

                public void onError(SelectorSelection selection, Throwable error) {
                    if (error instanceof IOException) {
                        onException((IOException) error);
                    } else {
                        onException(IOExceptionSupport.create(error));
                    }
                }
            });
        }
    }

    protected void serviceRead() {
        try {
            if (handshakeInProgress) {
                doHandshake();
            }

            ByteBuffer plain = ByteBuffer.allocate(sslSession.getApplicationBufferSize());
            plain.position(plain.limit());

            while (true) {
                if (!plain.hasRemaining()) {

                    int readCount = secureRead(plain);

                    if (readCount == 0) {
                        break;
                    }

                    // channel is closed, cleanup
                    if (readCount == -1) {
                        onException(new EOFException());
                        selection.close();
                        break;
                    }

                    receiveCounter += readCount;
                }

                if (status == SSLEngineResult.Status.OK && handshakeStatus != SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                    processCommand(plain);
                }
            }
        } catch (IOException e) {
            onException(e);
        } catch (Throwable e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    protected void processCommand(ByteBuffer plain) throws Exception {

        // Are we waiting for the next Command or are we building on the current one
        if (nextFrameSize == -1) {

            // We can get small packets that don't give us enough for the frame size
            // so allocate enough for the initial size value and
            if (plain.remaining() < Integer.SIZE) {
                if (currentBuffer == null) {
                    currentBuffer = ByteBuffer.allocate(4);
                }

                // Go until we fill the integer sized current buffer.
                while (currentBuffer.hasRemaining() && plain.hasRemaining()) {
                    currentBuffer.put(plain.get());
                }

                // Didn't we get enough yet to figure out next frame size.
                if (currentBuffer.hasRemaining()) {
                    return;
                } else {
                    currentBuffer.flip();
                    nextFrameSize = currentBuffer.getInt();
                }

            } else {

                // Either we are completing a previous read of the next frame size or its
                // fully contained in plain already.
                if (currentBuffer != null) {

                    // Finish the frame size integer read and get from the current buffer.
                    while (currentBuffer.hasRemaining()) {
                        currentBuffer.put(plain.get());
                    }

                    currentBuffer.flip();
                    nextFrameSize = currentBuffer.getInt();

                } else {
                    nextFrameSize = plain.getInt();
                }
            }

            if (wireFormat instanceof OpenWireFormat) {
                long maxFrameSize = ((OpenWireFormat) wireFormat).getMaxFrameSize();
                if (nextFrameSize > maxFrameSize) {
                    throw new IOException("Frame size of " + (nextFrameSize / (1024 * 1024)) +
                                          " MB larger than max allowed " + (maxFrameSize / (1024 * 1024)) + " MB");
                }
            }

            // now we got the data, lets reallocate and store the size for the marshaler.
            // if there's more data in plain, then the next call will start processing it.
            currentBuffer = ByteBuffer.allocate(nextFrameSize + 4);
            currentBuffer.putInt(nextFrameSize);

        } else {

            // If its all in one read then we can just take it all, otherwise take only
            // the current frame size and the next iteration starts a new command.
            if (currentBuffer.remaining() >= plain.remaining()) {
                currentBuffer.put(plain);
            } else {
                byte[] fill = new byte[currentBuffer.remaining()];
                plain.get(fill);
                currentBuffer.put(fill);
            }

            // Either we have enough data for a new command or we have to wait for some more.
            if (currentBuffer.hasRemaining()) {
                return;
            } else {
                currentBuffer.flip();
                Object command = wireFormat.unmarshal(new DataInputStream(new NIOInputStream(currentBuffer)));
                doConsume((Command) command);
                nextFrameSize = -1;
                currentBuffer = null;
            }
        }
    }

    protected int secureRead(ByteBuffer plain) throws Exception {

        if (!(inputBuffer.position() != 0 && inputBuffer.hasRemaining()) || status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            int bytesRead = channel.read(inputBuffer);

            if (bytesRead == 0) {
                return 0;
            }

            if (bytesRead == -1) {
                sslEngine.closeInbound();
                if (inputBuffer.position() == 0 || status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                    return -1;
                }
            }
        }

        plain.clear();

        inputBuffer.flip();
        SSLEngineResult res;
        do {
            res = sslEngine.unwrap(inputBuffer, plain);
        } while (res.getStatus() == SSLEngineResult.Status.OK && res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                && res.bytesProduced() == 0);

        if (res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
            finishHandshake();
        }

        status = res.getStatus();
        handshakeStatus = res.getHandshakeStatus();

        // TODO deal with BUFFER_OVERFLOW

        if (status == SSLEngineResult.Status.CLOSED) {
            sslEngine.closeInbound();
            return -1;
        }

        inputBuffer.compact();
        plain.flip();

        return plain.remaining();
    }

    protected void doHandshake() throws Exception {
        handshakeInProgress = true;
        while (true) {
            switch (sslEngine.getHandshakeStatus()) {
            case NEED_UNWRAP:
                secureRead(ByteBuffer.allocate(sslSession.getApplicationBufferSize()));
                break;
            case NEED_TASK:
                Runnable task;
                while ((task = sslEngine.getDelegatedTask()) != null) {
                    taskRunnerFactory.execute(task);
                }
                break;
            case NEED_WRAP:
                ((NIOOutputStream) buffOut).write(ByteBuffer.allocate(0));
                break;
            case FINISHED:
            case NOT_HANDSHAKING:
                finishHandshake();
                return;
            }
        }
    }

    @Override
    protected void doStart() throws Exception {
        taskRunnerFactory = new TaskRunnerFactory("ActiveMQ NIOSSLTransport Task");
        // no need to init as we can delay that until demand (eg in doHandshake)
        super.doStart();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        if (taskRunnerFactory != null) {
            taskRunnerFactory.shutdownNow();
            taskRunnerFactory = null;
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
        super.doStop(stopper);
    }

    /**
     * Overriding in order to add the client's certificates to ConnectionInfo Commands.
     *
     * @param command
     *            The Command coming in.
     */
    @Override
    public void doConsume(Object command) {
        if (command instanceof ConnectionInfo) {
            ConnectionInfo connectionInfo = (ConnectionInfo) command;
            connectionInfo.setTransportContext(getPeerCertificates());
        }
        super.doConsume(command);
    }

    /**
     * @return peer certificate chain associated with the ssl socket
     */
    public X509Certificate[] getPeerCertificates() {

        X509Certificate[] clientCertChain = null;
        try {
            if (sslEngine.getSession() != null) {
                clientCertChain = (X509Certificate[]) sslEngine.getSession().getPeerCertificates();
            }
        } catch (SSLPeerUnverifiedException e) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Failed to get peer certificates.", e);
            }
        }

        return clientCertChain;
    }

    public boolean isNeedClientAuth() {
        return needClientAuth;
    }

    public void setNeedClientAuth(boolean needClientAuth) {
        this.needClientAuth = needClientAuth;
    }

    public boolean isWantClientAuth() {
        return wantClientAuth;
    }

    public void setWantClientAuth(boolean wantClientAuth) {
        this.wantClientAuth = wantClientAuth;
    }

    public String[] getEnabledCipherSuites() {
        return enabledCipherSuites;
    }

    public void setEnabledCipherSuites(String[] enabledCipherSuites) {
        this.enabledCipherSuites = enabledCipherSuites;
    }
}
