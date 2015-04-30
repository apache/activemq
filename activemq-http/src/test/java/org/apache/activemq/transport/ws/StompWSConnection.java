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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.transport.stomp.StompFrame;
import org.eclipse.jetty.websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * STOMP over WS based Connection class
 */
public class StompWSConnection implements WebSocket, WebSocket.OnTextMessage {

    private static final Logger LOG = LoggerFactory.getLogger(StompWSConnection.class);

    private Connection connection;
    private final CountDownLatch connectLatch = new CountDownLatch(1);

    private final BlockingQueue<String> prefetch = new LinkedBlockingDeque<String>();

    private int closeCode = -1;
    private String closeMessage;

    public boolean isConnected() {
        return connection != null ? connection.isOpen() : false;
    }

    public void close() {
        if (connection != null) {
            connection.close();
        }
    }

    //---- Send methods ------------------------------------------------------//

    public void sendRawFrame(String rawFrame) throws Exception {
        checkConnected();
        connection.sendMessage(rawFrame);
    }

    public void sendFrame(StompFrame frame) throws Exception {
        checkConnected();
        connection.sendMessage(frame.format());
    }

    public void keepAlive() throws Exception {
        checkConnected();
        connection.sendMessage("\n");
    }

    //----- Receive methods --------------------------------------------------//

    public String receive() throws Exception {
        checkConnected();
        return prefetch.take();
    }

    public String receive(long timeout, TimeUnit unit) throws Exception {
        checkConnected();
        return prefetch.poll(timeout, unit);
    }

    public String receiveNoWait() throws Exception {
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

    //----- WebSocket callback handlers --------------------------------------//

    @Override
    public void onMessage(String data) {
        if (data == null) {
            return;
        }

        if (data.equals("\n")) {
            LOG.debug("New incoming heartbeat read");
        } else {
            LOG.trace("New incoming STOMP Frame read: \n{}", data);
            prefetch.add(data);
        }
    }

    @Override
    public void onOpen(Connection connection) {
        this.connection = connection;
        this.connectLatch.countDown();
    }

    @Override
    public void onClose(int closeCode, String message) {
        LOG.trace("STOMP WS Connection closed, code:{} message:{}", closeCode, message);

        this.connection = null;
        this.closeCode = closeCode;
        this.closeMessage = message;
    }

    //----- Internal implementation ------------------------------------------//

    private void checkConnected() throws IOException {
        if (!isConnected()) {
            throw new IOException("STOMP WS Connection is closed.");
        }
    }
}
