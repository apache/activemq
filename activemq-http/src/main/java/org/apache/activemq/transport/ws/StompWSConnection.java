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
import org.apache.activemq.transport.stomp.StompWireFormat;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * STOMP over WS based Connection class
 */
public class StompWSConnection extends WebSocketAdapter implements WebSocketListener {

    private static final Logger LOG = LoggerFactory.getLogger(StompWSConnection.class);

    private Session connection;
    private final CountDownLatch connectLatch = new CountDownLatch(1);

    private final BlockingQueue<String> prefetch = new LinkedBlockingDeque<String>();
    private final StompWireFormat wireFormat = new StompWireFormat();

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

    //---- Send methods ------------------------------------------------------//

    public synchronized void sendRawFrame(String rawFrame) throws Exception {
        checkConnected();
        connection.getRemote().sendString(rawFrame);
    }

    public synchronized void sendFrame(StompFrame frame) throws Exception {
        checkConnected();
        connection.getRemote().sendString(wireFormat.marshalToString(frame));
    }

    public synchronized void keepAlive() throws Exception {
        checkConnected();
        connection.getRemote().sendString("\n");
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
    public void onWebSocketText(String data) {
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
    public void onWebSocketClose(int statusCode, String reason) {
        LOG.trace("STOMP WS Connection closed, code:{} message:{}", statusCode, reason);

        this.connection = null;
        this.closeCode = statusCode;
        this.closeMessage = reason;
    }

    @Override
    public void onWebSocketConnect(org.eclipse.jetty.websocket.api.Session session) {
        this.connection = session;
        this.connectLatch.countDown();
    }

    //----- Internal implementation ------------------------------------------//

    private void checkConnected() throws IOException {
        if (!isConnected()) {
            throw new IOException("STOMP WS Connection is closed.");
        }
    }
}
