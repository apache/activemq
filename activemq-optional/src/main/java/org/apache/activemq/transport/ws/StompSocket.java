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
import java.security.cert.X509Certificate;

import org.apache.activemq.command.Command;
import org.apache.activemq.transport.TransportSupport;
import org.apache.activemq.transport.stomp.LegacyFrameTranslator;
import org.apache.activemq.transport.stomp.ProtocolConverter;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.activemq.transport.stomp.StompTransport;
import org.apache.activemq.transport.stomp.StompWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.eclipse.jetty.websocket.WebSocket;

/**
 * 
 * Implements web socket and mediates between servlet and the broker
 *
 */
class StompSocket extends TransportSupport implements WebSocket, StompTransport {
    Outbound outbound;
    ProtocolConverter protocolConverter = new ProtocolConverter(this, new LegacyFrameTranslator(), null);
    StompWireFormat wireFormat = new StompWireFormat();

    public void onConnect(Outbound outbound) {
        this.outbound=outbound;
    }
    
    public void onMessage(byte frame, byte[] data,int offset, int length) {}

    public void onMessage(byte frame, String data) {
        try {
            protocolConverter.onStompCommand((StompFrame)wireFormat.unmarshal(new ByteSequence(data.getBytes())));
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    public void onDisconnect() {
    }

    protected void doStart() throws Exception {
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
    }

    public int getReceiveCounter() {
        return 0;
    }

    public String getRemoteAddress() {
        return "StompSocket_" + this.hashCode();
    }

    public void oneway(Object command) throws IOException {
        try {
            protocolConverter.onActiveMQCommand((Command)command);
        } catch (Exception e) {
            onException(IOExceptionSupport.create(e));
        }
    }

    public X509Certificate[] getPeerCertificates() {
        return null;
    }

    public void sendToActiveMQ(Command command) {
        doConsume(command);
    }

    public void sendToStomp(StompFrame command) throws IOException {
        outbound.sendMessage(WebSocket.SENTINEL_FRAME, command.toString());
    }
}
