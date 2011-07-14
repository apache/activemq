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

import org.apache.activemq.command.Command;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.thread.DefaultThreadPools;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;

import javax.net.SocketFactory;
import javax.net.ssl.*;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class NIOSSLTransport extends NIOTransport  {

    private boolean needClientAuth;
    private boolean wantClientAuth;
    private String[] enabledCipherSuites;

    protected SSLContext sslContext;
    protected SSLEngine sslEngine;
    protected SSLSession sslSession;


    boolean handshakeInProgress = false;
    SSLEngineResult.Status status = null;
    SSLEngineResult.HandshakeStatus handshakeStatus = null;

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

            // initialize engine
            sslEngine = sslContext.createSSLEngine();
            sslEngine.setUseClientMode(false);
            sslEngine.setEnabledCipherSuites(enabledCipherSuites);
            sslEngine.setNeedClientAuth(needClientAuth);
            sslEngine.setWantClientAuth(wantClientAuth);

            sslSession = sslEngine.getSession();

            inputBuffer = ByteBuffer.allocate(sslSession.getPacketBufferSize());
            inputBuffer.clear();
            currentBuffer = ByteBuffer.allocate(sslSession.getApplicationBufferSize());

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

    protected void finishHandshake() throws Exception  {
          if (handshakeInProgress) {
              handshakeInProgress = false;
              nextFrameSize = -1;

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
                if (nextFrameSize == -1) {
                    if (!plain.hasRemaining()) {
                        plain.clear();
                        int readCount = secureRead(plain);
                        if (readCount == 0)
                            break;
                    }
                    nextFrameSize = plain.getInt();
                    if (wireFormat instanceof OpenWireFormat) {
                        long maxFrameSize = ((OpenWireFormat)wireFormat).getMaxFrameSize();
                        if (nextFrameSize > maxFrameSize) {
                            throw new IOException("Frame size of " + (nextFrameSize / (1024 * 1024)) + " MB larger than max allowed " + (maxFrameSize / (1024 * 1024)) + " MB");
                        }
                    }
                    currentBuffer = ByteBuffer.allocate(nextFrameSize + 4);
                    currentBuffer.putInt(nextFrameSize);
                    if (currentBuffer.hasRemaining()) {
                        if (currentBuffer.remaining() >= plain.remaining()) {
                            currentBuffer.put(plain);
                        } else {
                            byte[] fill = new byte[currentBuffer.remaining()];
                            plain.get(fill);
                            currentBuffer.put(fill);
                        }
                    }

                    if (currentBuffer.hasRemaining()) {
                        continue;
                    } else {
                        currentBuffer.flip();
                        Object command = wireFormat.unmarshal(new DataInputStream(new NIOInputStream(currentBuffer)));
                        doConsume((Command) command);

                        nextFrameSize = -1;
                    }
                }
            }

        } catch (IOException e) {
            onException(e);
        } catch (Throwable e) {
            onException(IOExceptionSupport.create(e));
        }

    }



    private int secureRead(ByteBuffer plain) throws Exception {
        int bytesRead = channel.read(inputBuffer);
        if (bytesRead == -1) {
            sslEngine.closeInbound();
            if (inputBuffer.position() == 0 ||
                    status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                return -1;
            }
        }

        plain.clear();

        inputBuffer.flip();
        SSLEngineResult res;
        do {
            res = sslEngine.unwrap(inputBuffer, plain);
        } while (res.getStatus() == SSLEngineResult.Status.OK &&
                res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP &&
                res.bytesProduced() == 0);

        if (res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
            finishHandshake();
        }

        status = res.getStatus();
        handshakeStatus = res.getHandshakeStatus();

        //TODO deal with BUFFER_OVERFLOW

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
                        DefaultThreadPools.getDefaultTaskRunnerFactory().execute(task);
                    }
                    break;
                case NEED_WRAP:
                    ((NIOOutputStream)buffOut).write(ByteBuffer.allocate(0));
                    break;
                case FINISHED:
                case NOT_HANDSHAKING:
                    finishHandshake();
                    return;
            }
        }
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        if (channel != null) {
            channel.close();
        }
        super.doStop(stopper);
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
