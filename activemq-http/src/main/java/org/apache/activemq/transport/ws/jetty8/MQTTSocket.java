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
package org.apache.activemq.transport.ws.jetty8;

import java.io.IOException;

import org.apache.activemq.transport.ws.AbstractMQTTSocket;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.eclipse.jetty.websocket.WebSocket;
import org.fusesource.mqtt.codec.DISCONNECT;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTSocket extends AbstractMQTTSocket implements WebSocket.OnBinaryMessage {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTSocket.class);

    private Connection outbound;

    public MQTTSocket(String remoteAddress) {
        super(remoteAddress);
    }

    @Override
    public void sendToMQTT(MQTTFrame command) throws IOException {
        ByteSequence bytes = wireFormat.marshal(command);
        outbound.sendMessage(bytes.getData(), 0, bytes.getLength());
    }

    @Override
    public void handleStopped() throws IOException {
        if (outbound != null && outbound.isOpen()) {
            outbound.close();
        }
    }

    //----- WebSocket.OnTextMessage callback handlers ------------------------//

    @Override
    public void onOpen(Connection connection) {
        this.outbound = connection;
    }

    @Override
    public void onMessage(byte[] bytes, int offset, int length) {
        if (!transportStartedAtLeastOnce()) {
            LOG.debug("Waiting for MQTTSocket to be properly started...");
            try {
                socketTransportStarted.await();
            } catch (InterruptedException e) {
                LOG.warn("While waiting for MQTTSocket to be properly started, we got interrupted!! Should be okay, but you could see race conditions...");
            }
        }

        receiveCounter += length;

        try {
            MQTTFrame frame = (MQTTFrame)wireFormat.unmarshal(new ByteSequence(bytes, offset, length));
            getProtocolConverter().onMQTTCommand(frame);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    @Override
    public void onClose(int closeCode, String message) {
        try {
            getProtocolConverter().onMQTTCommand(new DISCONNECT().encode());
        } catch (Exception e) {
            LOG.warn("Failed to close WebSocket", e);
        }
    }
}
