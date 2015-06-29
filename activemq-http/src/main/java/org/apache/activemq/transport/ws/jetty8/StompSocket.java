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

import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.activemq.transport.ws.AbstractStompSocket;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements web socket and mediates between servlet and the broker
 */
public class StompSocket extends AbstractStompSocket implements WebSocket.OnTextMessage {

    private static final Logger LOG = LoggerFactory.getLogger(StompSocket.class);

    private Connection outbound;

    public StompSocket(String remoteAddress) {
        super(remoteAddress);
    }

    @Override
    public void handleStopped() throws IOException {
        if (outbound != null && outbound.isOpen()) {
            outbound.close();
        }
    }

    @Override
    public void sendToStomp(StompFrame command) throws IOException {
        outbound.sendMessage(command.format());
    }

    //----- WebSocket.OnTextMessage callback handlers ------------------------//

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
        processStompFrame(data);
    }
}
