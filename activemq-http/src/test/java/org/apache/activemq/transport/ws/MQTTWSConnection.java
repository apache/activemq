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
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.mqtt.MQTTWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.codec.CONNACK;
import org.fusesource.mqtt.codec.CONNECT;
import org.fusesource.mqtt.codec.DISCONNECT;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.fusesource.mqtt.codec.PINGREQ;
import org.fusesource.mqtt.codec.PINGRESP;
import org.fusesource.mqtt.codec.PUBACK;
import org.fusesource.mqtt.codec.PUBCOMP;
import org.fusesource.mqtt.codec.PUBLISH;
import org.fusesource.mqtt.codec.PUBREC;
import org.fusesource.mqtt.codec.PUBREL;
import org.fusesource.mqtt.codec.SUBACK;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a simple WebSocket based MQTT Client that can be used for unit testing.
 */
public class MQTTWSConnection extends WebSocketAdapter implements WebSocketListener {

    private static final Logger LOG = LoggerFactory.getLogger(MQTTWSConnection.class);

    private static final MQTTFrame PING_RESP_FRAME = new PINGRESP().encode();

    private Session connection;
    private final CountDownLatch connectLatch = new CountDownLatch(1);
    private final MQTTWireFormat wireFormat = new MQTTWireFormat();

    private final BlockingQueue<MQTTFrame> prefetch = new LinkedBlockingDeque<>();

    private boolean writePartialFrames;
    private int closeCode = -1;
    private String closeMessage;

    @Override
    public boolean isConnected() {
        return connection != null ? connection.isOpen() : false;
    }

    public void close() {
        if (connection != null) {
            connection.close();
        }
    }

    protected Session getConnection() {
        return connection;
    }

    //----- Connection and Disconnection methods -----------------------------//

    public void connect() throws Exception {
        connect(UUID.randomUUID().toString());
    }

    public void connect(String clientId) throws Exception {
        CONNECT command = new CONNECT();
        command.clientId(new UTF8Buffer(clientId));
        command.cleanSession(false);
        command.version(3);
        command.keepAlive((short) 0);
        connect(command);
    }

    public void connect(CONNECT command) throws Exception {
        checkConnected();

        sendBytes(wireFormat.marshal(command.encode()));

        MQTTFrame incoming = receive(15, TimeUnit.SECONDS);

        if (incoming == null || incoming.messageType() != CONNACK.TYPE) {
            throw new IOException("Failed to connect to remote service.");
        } else {
            CONNACK connack = new CONNACK().decode(incoming);
            if (!connack.code().equals(CONNACK.Code.CONNECTION_ACCEPTED)) {
                throw new IOException("Failed to connect to remote service: " + connack.code());
            }
        }
    }

    public void disconnect() throws Exception {
        if (!isConnected()) {
            return;
        }

        DISCONNECT command = new DISCONNECT();
        sendBytes(wireFormat.marshal(command.encode()));
    }

    //---- Send methods ------------------------------------------------------//

    public void sendFrame(MQTTFrame frame) throws Exception {
        checkConnected();
        sendBytes(wireFormat.marshal(frame));
    }

    public void keepAlive() throws Exception {
        checkConnected();
        sendBytes(wireFormat.marshal(new PINGREQ().encode()));
    }

    //----- Receive methods --------------------------------------------------//

    public MQTTFrame receive() throws Exception {
        checkConnected();
        return prefetch.take();
    }

    public MQTTFrame receive(long timeout, TimeUnit unit) throws Exception {
        checkConnected();
        return prefetch.poll(timeout, unit);
    }

    public MQTTFrame receiveNoWait() throws Exception {
        checkConnected();
        return prefetch.poll();
    }

    //---- Blocking state change calls ---------------------------------------//

    public void awaitConnection() throws InterruptedException {
        connectLatch.await();
    }

    public boolean awaitConnection(long time, TimeUnit unit) throws InterruptedException {
        return connectLatch.await(time, unit);
    }

    //----- Property Accessors -----------------------------------------------//

    public int getCloseCode() {
        return closeCode;
    }

    public String getCloseMessage() {
        return closeMessage;
    }

    public boolean isWritePartialFrames() {
        return writePartialFrames;
    }

    public MQTTWSConnection setWritePartialFrames(boolean value) {
        this.writePartialFrames = value;
        return this;
    }

    //----- WebSocket callback handlers --------------------------------------//

    @Override
    public void onWebSocketBinary(byte[] data, int offset, int length) {
        if (data ==null || length <= 0) {
            return;
        }

        MQTTFrame frame = null;

        try {
            frame = (MQTTFrame)wireFormat.unmarshal(new ByteSequence(data, offset, length));
        } catch (IOException e) {
            LOG.error("Could not decode incoming MQTT Frame: {}", e.getMessage());
            connection.close();
        }

        try {
            switch (frame.messageType()) {
            case PINGREQ.TYPE:
                PINGREQ ping = new PINGREQ().decode(frame);
                LOG.info("WS-Client read frame: {}", ping);
                sendFrame(PING_RESP_FRAME);
                break;
            case PINGRESP.TYPE:
                LOG.info("WS-Client ping response received.");
                break;
            case CONNACK.TYPE:
                CONNACK connAck = new CONNACK().decode(frame);
                LOG.info("WS-Client read frame: {}", connAck);
                prefetch.put(frame);
                break;
            case SUBACK.TYPE:
                SUBACK subAck = new SUBACK().decode(frame);
                LOG.info("WS-Client read frame: {}", subAck);
                prefetch.put(frame);
                break;
            case PUBLISH.TYPE:
                PUBLISH publish = new PUBLISH().decode(frame);
                LOG.info("WS-Client read frame: {}", publish);
                prefetch.put(frame);
                break;
            case PUBACK.TYPE:
                PUBACK pubAck = new PUBACK().decode(frame);
                LOG.info("WS-Client read frame: {}", pubAck);
                prefetch.put(frame);
                break;
            case PUBREC.TYPE:
                PUBREC pubRec = new PUBREC().decode(frame);
                LOG.info("WS-Client read frame: {}", pubRec);
                prefetch.put(frame);
                break;
            case PUBREL.TYPE:
                PUBREL pubRel = new PUBREL().decode(frame);
                LOG.info("WS-Client read frame: {}", pubRel);
                prefetch.put(frame);
                break;
            case PUBCOMP.TYPE:
                PUBCOMP pubComp = new PUBCOMP().decode(frame);
                LOG.info("WS-Client read frame: {}", pubComp);
                prefetch.put(frame);
                break;
            default:
                LOG.error("Unknown MQTT  Frame received.");
                connection.close();
            }
        } catch (Exception e) {
            LOG.error("Could not decode incoming MQTT Frame: {}", e.getMessage());
            connection.close();
        }
    }

    //----- Internal implementation ------------------------------------------//

    private void sendBytes(ByteSequence payload) throws IOException {
        if (!isWritePartialFrames()) {
            connection.getRemote().sendBytes(ByteBuffer.wrap(payload.data, payload.offset, payload.length));
        } else {
            connection.getRemote().sendBytes(ByteBuffer.wrap(
                payload.data, payload.offset, payload.length / 2));
            connection.getRemote().sendBytes(ByteBuffer.wrap(
                payload.data, payload.offset + payload.length / 2, payload.length / 2));
        }
    }

    private void checkConnected() throws IOException {
        if (!isConnected()) {
            throw new IOException("MQTT WS Connection is closed.");
        }
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        LOG.trace("MQTT WS Connection closed, code:{} message:{}", statusCode, reason);

        this.connection = null;
        this.closeCode = statusCode;
        this.closeMessage = reason;

    }

    @Override
    public void onWebSocketConnect(org.eclipse.jetty.websocket.api.Session session) {
        this.connection = session;
        this.connectLatch.countDown();
    }
}
