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

import org.apache.activemq.command.Command;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.transport.mqtt.MQTTInactivityMonitor;
import org.apache.activemq.transport.mqtt.MQTTProtocolConverter;
import org.apache.activemq.transport.mqtt.MQTTTransport;
import org.apache.activemq.transport.mqtt.MQTTWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.eclipse.jetty.websocket.WebSocket;
import org.fusesource.mqtt.codec.DISCONNECT;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CountDownLatch;

public class MQTTSocket  extends TransportSupport implements WebSocket.OnBinaryMessage, MQTTTransport {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTSocket.class);
    Connection outbound;
    MQTTProtocolConverter protocolConverter = new MQTTProtocolConverter(this, null);
    MQTTWireFormat wireFormat = new MQTTWireFormat();
    private final CountDownLatch socketTransportStarted = new CountDownLatch(1);

    @Override
    public void onMessage(byte[] bytes, int offset, int length) {
        if (!transportStartedAtLeastOnce()) {
            LOG.debug("Waiting for StompSocket to be properly started...");
            try {
                socketTransportStarted.await();
            } catch (InterruptedException e) {
                LOG.warn("While waiting for StompSocket to be properly started, we got interrupted!! Should be okay, but you could see race conditions...");
            }
        }

        try {
            MQTTFrame frame = (MQTTFrame)wireFormat.unmarshal(new ByteSequence(bytes, offset, length));
            protocolConverter.onMQTTCommand(frame);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    @Override
    public void onOpen(Connection connection) {
        this.outbound = connection;
    }

    @Override
    public void onClose(int closeCode, String message) {
        try {
            protocolConverter.onMQTTCommand(new DISCONNECT().encode());
        } catch (Exception e) {
            LOG.warn("Failed to close WebSocket", e);
        }
    }

    protected void doStart() throws Exception {
        socketTransportStarted.countDown();
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
    }

    private boolean transportStartedAtLeastOnce() {
        return socketTransportStarted.getCount() == 0;
    }

    @Override
    public int getReceiveCounter() {
        return 0;
    }

    @Override
    public String getRemoteAddress() {
        return "MQTTSocket_" + this.hashCode();
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
    public void sendToMQTT(MQTTFrame command) throws IOException {
        ByteSequence bytes = wireFormat.marshal(command);
        outbound.sendMessage(bytes.getData(), 0, bytes.getLength());
    }

    @Override
    public X509Certificate[] getPeerCertificates() {
        return new X509Certificate[0];
    }

    @Override
    public MQTTInactivityMonitor getInactivityMonitor() {
        return null;
    }

    @Override
    public MQTTWireFormat getWireFormat() {
        return wireFormat;
    }
}
