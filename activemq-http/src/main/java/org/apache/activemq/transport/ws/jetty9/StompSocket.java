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
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.activemq.transport.ws.AbstractStompSocket;
import org.apache.activemq.util.IOExceptionSupport;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements web socket and mediates between servlet and the broker
 */
public class StompSocket extends AbstractStompSocket implements WebSocketListener {

    private static final Logger LOG = LoggerFactory.getLogger(StompSocket.class);

    private final int ORDERLY_CLOSE_TIMEOUT = 10;

    private Session session;

    public StompSocket(String remoteAddress) {
        super(remoteAddress);
    }

    @Override
    public void sendToStomp(StompFrame command) throws IOException {
        try {
            //timeout after a period of time so we don't wait forever and hold the protocol lock
            session.getRemote().sendStringByFuture(getWireFormat().marshalToString(command)).get(getDefaultSendTimeOut(), TimeUnit.SECONDS);
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

    //----- WebSocketListener event callbacks --------------------------------//

    @Override
    public void onWebSocketBinary(byte[] arg0, int arg1, int arg2) {
    }

    @Override
    public void onWebSocketClose(int arg0, String arg1) {
        try {
            if (protocolLock.tryLock() || protocolLock.tryLock(ORDERLY_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                LOG.debug("Stomp WebSocket closed: code[{}] message[{}]", arg0, arg1);
                protocolConverter.onStompCommand(new StompFrame(Stomp.Commands.DISCONNECT));
            }
        } catch (Exception e) {
            LOG.debug("Failed to close STOMP WebSocket cleanly", e);
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
    public void onWebSocketText(String data) {
        processStompFrame(data);
    }

    private static int getDefaultSendTimeOut() {
        return Integer.getInteger("org.apache.activemq.transport.ws.StompSocket.sendTimeout", 30);
    }
}
