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
package org.apache.activemq.transport.ws;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.transport.stomp.ProtocolConverter;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.activemq.transport.stomp.StompInactivityMonitor;
import org.apache.activemq.transport.stomp.StompTransport;
import org.apache.activemq.transport.stomp.StompWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation of a STOMP based WebSocket handler.
 */
public abstract class AbstractStompSocket extends TransportSupport implements StompTransport {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStompSocket.class);

    protected ReentrantLock protocolLock = new ReentrantLock();
    protected ProtocolConverter protocolConverter = new ProtocolConverter(this, null);
    protected StompWireFormat wireFormat = new StompWireFormat();
    protected final CountDownLatch socketTransportStarted = new CountDownLatch(1);
    protected final StompInactivityMonitor stompInactivityMonitor = new StompInactivityMonitor(this, wireFormat);
    protected volatile int receiveCounter;
    protected final String remoteAddress;
    protected X509Certificate[] certificates;

    public AbstractStompSocket(String remoteAddress) {
        super();
        this.remoteAddress = remoteAddress;
    }

    @Override
    public void oneway(Object command) throws IOException {
        protocolLock.lock();
        try {
            protocolConverter.onActiveMQCommand((Command)command);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        } finally {
            protocolLock.unlock();
        }
    }

    @Override
    public void sendToActiveMQ(Command command) {
        protocolLock.lock();
        try {
            doConsume(command);
        } finally {
            protocolLock.unlock();
        }
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        stompInactivityMonitor.stop();
        handleStopped();
    }

    @Override
    protected void doStart() throws Exception {
        socketTransportStarted.countDown();
        stompInactivityMonitor.setTransportListener(getTransportListener());
        stompInactivityMonitor.startConnectCheckTask();
    }

    //----- Abstract methods for subclasses to implement ---------------------//

    @Override
    public abstract void sendToStomp(StompFrame command) throws IOException;

    /**
     * Called when the transport is stopping to allow the dervied classes
     * a chance to close WebSocket resources.
     *
     * @throws IOException if an error occurs during the stop.
     */
    public abstract void handleStopped() throws IOException;

    //----- Accessor methods -------------------------------------------------//

    @Override
    public StompInactivityMonitor getInactivityMonitor() {
        return stompInactivityMonitor;
    }

    @Override
    public StompWireFormat getWireFormat() {
        return wireFormat;
    }

    @Override
    public String getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public int getReceiveCounter() {
        return receiveCounter;
    }

    //----- Internal implementation ------------------------------------------//

    protected void processStompFrame(String data) {
        if (!transportStartedAtLeastOnce()) {
            LOG.debug("Waiting for StompSocket to be properly started...");
            try {
                socketTransportStarted.await();
            } catch (InterruptedException e) {
                LOG.warn("While waiting for StompSocket to be properly started, we got interrupted!! Should be okay, but you could see race conditions...");
            }
        }

        protocolLock.lock();
        try {
            if (data != null) {
                receiveCounter += data.length();

                if (data.equals("\n")) {
                    stompInactivityMonitor.onCommand(new KeepAliveInfo());
                } else {
                    StompFrame frame = (StompFrame)wireFormat.unmarshal(new ByteSequence(data.getBytes("UTF-8")));
                    frame.setTransportContext(getPeerCertificates());
                    protocolConverter.onStompCommand(frame);
                }
            }
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        } finally {
            protocolLock.unlock();
        }
    }

    private boolean transportStartedAtLeastOnce() {
        return socketTransportStarted.getCount() == 0;
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        return certificates;
    }

    @Override
    public void setPeerCertificates(X509Certificate[] certificates) {
        this.certificates = certificates;
    }
}
