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
package org.apache.activemq.transport.ws.jetty9;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.transport.mqtt.MQTTCodec;
import org.apache.activemq.transport.ws.AbstractMQTTSocket;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.mqtt.codec.DISCONNECT;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTTSocket extends AbstractMQTTSocket implements MQTTCodec.MQTTFrameSink, WebSocketListener {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTSocket.class);

    private final int ORDERLY_CLOSE_TIMEOUT = 10;
    private Session session;
    private final AtomicBoolean receivedDisconnect = new AtomicBoolean();

    private final MQTTCodec codec;

    public MQTTSocket(String remoteAddress) {
        super(remoteAddress);

        this.codec = new MQTTCodec(this, getWireFormat());
    }

    @Override
    public void sendToMQTT(MQTTFrame command) throws IOException {
        ByteSequence bytes = wireFormat.marshal(command);
        try {
            //timeout after a period of time so we don't wait forever and hold the protocol lock
            session.getRemote().sendBytesByFuture(
                    ByteBuffer.wrap(bytes.getData(), 0, bytes.getLength())).get(getDefaultSendTimeOut(), TimeUnit.SECONDS);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public void handleStopped() throws IOException {
        if (session != null && session.isOpen()) {
            session.close();
        }
    }

    //----- WebSocket.OnTextMessage callback handlers ------------------------//

    @Override
    public void onWebSocketBinary(byte[] bytes, int offset, int length) {
        if (!transportStartedAtLeastOnce()) {
            LOG.debug("Waiting for MQTTSocket to be properly started...");
            try {
                socketTransportStarted.await();
            } catch (InterruptedException e) {
                LOG.warn("While waiting for MQTTSocket to be properly started, we got interrupted!! Should be okay, but you could see race conditions...");
            }
        }

        protocolLock.lock();
        try {
            receiveCounter += length;
            codec.parse(new DataByteArrayInputStream(new Buffer(bytes, offset, length)), length);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        } finally {
            protocolLock.unlock();
        }
    }

    @Override
    public void onWebSocketClose(int arg0, String arg1) {
        try {
            if (protocolLock.tryLock() || protocolLock.tryLock(ORDERLY_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                LOG.debug("MQTT WebSocket closed: code[{}] message[{}]", arg0, arg1);
                //Check if we received a disconnect packet before closing
                if (!receivedDisconnect.get()) {
                    getProtocolConverter().onTransportError();
                }
                getProtocolConverter().onMQTTCommand(new DISCONNECT().encode());
            }
        } catch (Exception e) {
            LOG.debug("Failed to close MQTT WebSocket cleanly", e);
        } finally {
            if (protocolLock.isHeldByCurrentThread()) {
                protocolLock.unlock();
            }
        }
    }

    @Override
    public void onWebSocketConnect(Session session) {
        this.session = session;
    }

    @Override
    public void onWebSocketError(Throwable arg0) {

    }

    @Override
    public void onWebSocketText(String arg0) {
    }

    private static int getDefaultSendTimeOut() {
        return Integer.getInteger("org.apache.activemq.transport.ws.MQTTSocket.sendTimeout", 30);
    }

    //----- MQTTCodec Frame Sink event point ---------------------------------//

    @Override
    public void onFrame(MQTTFrame mqttFrame) {
        try {
            if (mqttFrame.messageType() == DISCONNECT.TYPE) {
                receivedDisconnect.set(true);
            }
            getProtocolConverter().onMQTTCommand(mqttFrame);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        }
    }
}
