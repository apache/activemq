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
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;

import javax.net.SocketFactory;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;

import com.sun.prism.shader.Solid_TextureYV12_AlphaTest_Loader;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NIOSASLTransport extends NIOTransport {

    private static final Logger LOG = LoggerFactory.getLogger(NIOSASLTransport.class);

    protected boolean needClientAuth;
    protected boolean wantClientAuth;
    protected String[] enabledCipherSuites;
    protected String[] enabledProtocols;

    protected volatile boolean handshakeInProgress = false;
    protected TaskRunnerFactory taskRunnerFactory;
    private SaslServer saslServer;
    private SaslClient saslClient;

    NIOOutputStream.DataWrappingEngine<Integer> dataWrappingEngine;
    private String krb5ConfigName;
    private Subject currentSubject;

    public NIOSASLTransport(Subject subject, SaslClient saslClient, WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws IOException {
        super(wireFormat, socketFactory, remoteLocation, localLocation);
        this.saslClient = saslClient;
        this.currentSubject = subject;
    }

    public NIOSASLTransport(Subject subject, SaslServer saslServer, WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket);
        this.saslServer = saslServer;
        this.currentSubject = subject;
    }

    @Override
    protected void initializeStreams() throws IOException {
        NIOOutputStream outputStream = null;

        try {
            channel = socket.getChannel();
            channel.configureBlocking(false);

            if (saslClient != null) {
                dataWrappingEngine = new NIOOutputStream.SASLAuthenticationEngine(saslClient);
            } else if (saslServer != null) {
                dataWrappingEngine = new NIOOutputStream.SASLAuthenticationEngine(saslServer);
            }

            inputBuffer = ByteBuffer.allocate(dataWrappingEngine.getPacketBufferSize());
            inputBuffer.clear();

            outputStream = new NIOOutputStream(channel);
            outputStream.setEngine(dataWrappingEngine);

            this.dataOut = new DataOutputStream(outputStream);
            this.buffOut = outputStream;

            //FIXME RECONSIDER
            Subject.doAs(currentSubject, new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    doHandshake();
                    return null;
                }
            });

            if (saslClient != null) {
                dataWrappingEngine = new NIOOutputStream.SASLDataWrappingEngine(saslClient);
            } else if (saslServer != null) {
                dataWrappingEngine = new NIOOutputStream.SASLDataWrappingEngine(saslServer);
            }

            outputStream.setEngine(dataWrappingEngine);

            handshakeInProgress = false;

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
    protected void serviceRead() {
        try {
            ByteBuffer plain = ByteBuffer.allocate(dataWrappingEngine.getPacketBufferSize());
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
                processCommand(plain);
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
                doConsume(command);
                nextFrameSize = -1;
                currentBuffer = null;
            }
        }
    }

    protected int secureRead(ByteBuffer plain) throws Exception {
        if (!(inputBuffer.position() != 0 && inputBuffer.hasRemaining())) {
            int bytesRead = channel.read(inputBuffer);

            if (bytesRead == 0) {
                return 0;
            }

            if (bytesRead == -1) {
                if (inputBuffer.position() == 0) {
                    return -1;
                }
            }
        }

        plain.clear();

        inputBuffer.flip();
        int responseBytes;

        if (!handshakeInProgress) {
            do {
                responseBytes = dataWrappingEngine.unwrap(inputBuffer, plain);
            } while (responseBytes == 0);
        }

        inputBuffer.compact();
        plain.flip();

        return plain.remaining();
    }

    // Implementation basing on official Oracle documentation
    protected void doHandshake() throws Exception {
        handshakeInProgress = true;
        byte[] token = new byte[0];

        if (saslClient != null) {
            token = saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(token) : token;

            while (!saslClient.isComplete()) {

                if (token != null) {
                    if (token.length > 0) {
                        dataOut.write(token);
                    }
                    dataOut.flush();
                }

                int bytesRead;
                do {
                    bytesRead = channel.read(inputBuffer);
                } while (bytesRead == 0);

                inputBuffer.flip();
                token = new byte[inputBuffer.getInt()];
                inputBuffer.get(token);

                token = saslClient.evaluateChallenge(token);
                inputBuffer.compact();
            }

            // Requesting/Confirmation of protection level
            if (token.length > 0) {
                dataOut.write(token);
            }
            dataOut.flush();
        } else {
            while (!saslServer.isComplete()) {
                int bytesRead;
                do {
                    bytesRead = channel.read(inputBuffer);
                } while (bytesRead == 0 && !saslServer.isComplete());

                inputBuffer.flip();

                token = new byte[inputBuffer.getInt()];
                inputBuffer.get(token);

                token = saslServer.evaluateResponse(token);

                inputBuffer.compact();

                if (saslServer.isComplete()) {
                    break;
                }

                // Send a token to the peer if one was generated by acceptSecContext
                if (token != null) {
                    dataOut.write(token);
                    dataOut.flush();
                }
            }
        }

        nextFrameSize = -1;

        // listen for events telling us when the socket is readable.
        selection = SelectorManager.getInstance().register(channel, new SelectorManager.Listener() {
            @Override
            public void onSelect(SelectorSelection selection) {
                serviceRead();
            }

            @Override
            public void onError(SelectorSelection selection, Throwable error) {
                if (error instanceof IOException) {
                    onException((IOException) error);
                } else {
                    onException(IOExceptionSupport.create(error));
                }
            }
        });
    }

    @Override
    protected void doStart() throws Exception {
        taskRunnerFactory = new TaskRunnerFactory("ActiveMQ NIOSASLTransport Task");
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

    public void setKrb5ConfigName(String krb5ConfigName) {
        // no-op
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

    public String[] getEnabledProtocols() {
        return enabledProtocols;
    }

    public void setEnabledProtocols(String[] enabledProtocols) {
        this.enabledProtocols = enabledProtocols;
    }

    public void setSaslServer(SaslServer saslServer) {
        this.saslServer = saslServer;
    }

    public void setSaslClient(SaslClient saslClient) {
        this.saslClient = saslClient;
    }
}
