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
import java.util.concurrent.CountDownLatch;

import org.apache.activemq.command.Command;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.transport.stomp.ProtocolConverter;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.activemq.transport.stomp.StompInactivityMonitor;
import org.apache.activemq.transport.stomp.StompTransport;
import org.apache.activemq.transport.stomp.StompWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements web socket and mediates between servlet and the broker
 */
class StompSocket extends TransportSupport implements WebSocket.OnTextMessage, StompTransport {
    private static final Logger LOG = LoggerFactory.getLogger(StompSocket.class);

    Connection outbound;
    ProtocolConverter protocolConverter = new ProtocolConverter(this, null);
    StompWireFormat wireFormat = new StompWireFormat();
    private final CountDownLatch socketTransportStarted = new CountDownLatch(1);

    @Override
    public void onOpen(Connection connection) {
        this.outbound = connection;
    }

    @Override
    public void onClose(int closeCode, String message) {
        try {
            protocolConverter.onStompCommand(new StompFrame(Stomp.Commands.DISCONNECT));
        } catch (Exception e) {
            LOG.warn("Failed to close WebSocket", e);
        }
    }

    @Override
    public void onMessage(String data) {

        if (!transportStartedAtLeastOnce()) {
            LOG.debug("Waiting for StompSocket to be properly started...");
            try {
                socketTransportStarted.await();
            } catch (InterruptedException e) {
                LOG.warn("While waiting for StompSocket to be properly started, we got interrupted!! Should be okay, but you could see race conditions...");
            }
        }


        try {
            protocolConverter.onStompCommand((StompFrame)wireFormat.unmarshal(new ByteSequence(data.getBytes("UTF-8"))));
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    private boolean transportStartedAtLeastOnce() {
        return socketTransportStarted.getCount() == 0;
    }

    @Override
    protected void doStart() throws Exception {
        socketTransportStarted.countDown();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
    }

    @Override
    public int getReceiveCounter() {
        return 0;
    }

    @Override
    public String getRemoteAddress() {
        return "StompSocket_" + this.hashCode();
    }

    @Override
    public void oneway(Object command) throws IOException {
        try {
            protocolConverter.onActiveMQCommand((Command)command);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    @Override
    public void sendToActiveMQ(Command command) {
        doConsume(command);
    }

    @Override
    public void sendToStomp(StompFrame command) throws IOException {
        outbound.sendMessage(command.format());
    }

    @Override
    public StompInactivityMonitor getInactivityMonitor() {
        return null;
    }

    @Override
    public StompWireFormat getWireFormat() {
        return this.wireFormat;
    }
}
