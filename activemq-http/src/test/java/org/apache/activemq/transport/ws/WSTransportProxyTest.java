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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.junit.Test;

/**
 * Unit tests for {@link WSTransportProxy} frame handling. In particular that outbound
 * text is emitted as a WebSocket TEXT frame and outbound binary as a BINARY frame -
 * a regression guard: outbound text was previously sent as a binary frame.
 */
public class WSTransportProxyTest {

    private WSTransportProxy startedProxy(WSTransport wsTransport, Session session) throws Exception {
        Transport transport = mock(Transport.class);
        when(transport.narrow(WSTransport.class)).thenReturn(wsTransport);

        WSTransportProxy proxy = new WSTransportProxy("ws://localhost:61614", transport);
        proxy.setTransportListener(mock(TransportListener.class));
        proxy.start();                  // counts down the "started" latch so sends don't block
        proxy.onWebSocketOpen(session); // installs the session used by the outbound sends
        return proxy;
    }

    @Test
    public void testOutboundTextIsSentAsTextFrame() throws Exception {
        WSTransport wsTransport = mock(WSTransport.class);
        Session session = mock(Session.class);
        // The proxy blocks on the send callback, so complete it when sendText is invoked.
        doAnswer(inv -> { inv.getArgument(1, Callback.class).succeed(); return null; })
            .when(session).sendText(any(), any(Callback.class));

        WSTransportProxy proxy = startedProxy(wsTransport, session);

        proxy.onSocketOutboundText("CONNECTED\n");

        // Must be a WebSocket TEXT frame (sendText), never a binary frame.
        verify(session, times(1)).sendText(eq("CONNECTED\n"), any(Callback.class));
        verify(session, never()).sendBinary(any(ByteBuffer.class), any(Callback.class));
    }

    @Test
    public void testOutboundBinaryIsSentAsBinaryFrame() throws Exception {
        WSTransport wsTransport = mock(WSTransport.class);
        Session session = mock(Session.class);
        doAnswer(inv -> { inv.getArgument(1, Callback.class).succeed(); return null; })
            .when(session).sendBinary(any(ByteBuffer.class), any(Callback.class));

        WSTransportProxy proxy = startedProxy(wsTransport, session);

        ByteBuffer payload = ByteBuffer.wrap("frame-bytes".getBytes(StandardCharsets.UTF_8));
        proxy.onSocketOutboundBinary(payload);

        // Must be a WebSocket BINARY frame (sendBinary), never a text frame.
        verify(session, times(1)).sendBinary(any(ByteBuffer.class), any(Callback.class));
        verify(session, never()).sendText(any(), any(Callback.class));
    }
}
