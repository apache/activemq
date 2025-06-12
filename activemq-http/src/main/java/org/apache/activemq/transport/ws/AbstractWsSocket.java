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

import org.apache.activemq.transport.TransportSupport;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractWsSocket extends TransportSupport implements WebSocketListener {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractWsSocket.class);

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        WebSocketListener.super.onWebSocketClose(statusCode, reason);

        try {
            stop();
            LOG.debug("Stopped socket: {}", getRemoteAddress());
        } catch (Exception e) {
            LOG.debug("Could not stop socket to {}. This exception is ignored.", getRemoteAddress(), e);
        }

        doWebSocketClose(statusCode, reason);
    }

    public abstract void doWebSocketClose(int statusCode, String reason);
}
