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
package org.apache.activemq.transport.stomp;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.Endpoint;
import org.apache.activemq.command.Response;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.util.MarshallingSupport;

/**
 * Represents all the data in a STOMP frame.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class StompFrame implements Command {

    public static final byte[] NO_DATA = new byte[] {};

    private String action;
    private Map<String, String> headers = new HashMap<String, String>();
    private byte[] content = NO_DATA;

    private transient Object transportContext = null;

    public StompFrame(String command) {
        this(command, null, null);
    }

    public StompFrame(String command, Map<String, String> headers) {
        this(command, headers, null);
    }

    public StompFrame(String command, Map<String, String> headers, byte[] data) {
        this.action = command;
        if (headers != null)
            this.headers = headers;
        if (data != null)
            this.content = data;
    }

    public StompFrame() {
    }

    public String getAction() {
        return action;
    }

    public void setAction(String command) {
        this.action = command;
    }

    public byte[] getContent() {
        return content;
    }

    public String getBody() {
        try {
            return new String(content, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return new String(content);
        }
    }

    public void setContent(byte[] data) {
        this.content = data;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    public int getCommandId() {
        return 0;
    }

    @Override
    public Endpoint getFrom() {
        return null;
    }

    @Override
    public Endpoint getTo() {
        return null;
    }

    @Override
    public boolean isBrokerInfo() {
        return false;
    }

    @Override
    public boolean isMessage() {
        return false;
    }

    @Override
    public boolean isMessageAck() {
        return false;
    }

    @Override
    public boolean isMessageDispatch() {
        return false;
    }

    @Override
    public boolean isMessageDispatchNotification() {
        return false;
    }

    @Override
    public boolean isResponse() {
        return false;
    }

    @Override
    public boolean isResponseRequired() {
        return false;
    }

    @Override
    public boolean isShutdownInfo() {
        return false;
    }

    @Override
    public boolean isConnectionControl() {
        return false;
    }

    @Override
    public boolean isConsumerControl() {
        return false;
    }

    @Override
    public boolean isWireFormatInfo() {
        return false;
    }

    @Override
    public void setCommandId(int value) {
    }

    @Override
    public void setFrom(Endpoint from) {
    }

    @Override
    public void setResponseRequired(boolean responseRequired) {
    }

    @Override
    public void setTo(Endpoint to) {
    }

    @Override
    public Response visit(CommandVisitor visitor) throws Exception {
        return null;
    }

    @Override
    public byte getDataStructureType() {
        return 0;
    }

    @Override
    public boolean isMarshallAware() {
        return false;
    }

    @Override
    public String toString() {
        return format(true);
    }

    public String format() {
        return format(false);
    }

    public String format(boolean forLogging) {
        if( !forLogging && getAction().equals(Stomp.Commands.KEEPALIVE) ) {
            return "\n";
        }
        StringBuilder buffer = new StringBuilder();
        buffer.append(getAction());
        buffer.append("\n");
        Map<String, String> headers = getHeaders();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            buffer.append(entry.getKey());
            buffer.append(":");
            if (forLogging && entry.getKey().toString().toLowerCase(Locale.ENGLISH).contains(Stomp.Headers.Connect.PASSCODE)) {
                buffer.append("*****");
            } else {
                buffer.append(entry.getValue());
            }
            buffer.append("\n");
        }
        buffer.append("\n");
        if (getContent() != null) {
            try {
                String contentString = new String(getContent(), "UTF-8");
                if (forLogging) {
                    contentString = MarshallingSupport.truncate64(contentString);
                }
                buffer.append(contentString);
            } catch (Throwable e) {
                buffer.append(Arrays.toString(getContent()));
            }
        }
        // terminate the frame
        buffer.append('\u0000');
        return buffer.toString();
    }

    /**
     * Transports may wish to associate additional data with the connection. For
     * example, an SSL transport may use this field to attach the client
     * certificates used when the connection was established.
     *
     * @return the transport context.
     */
    public Object getTransportContext() {
        return transportContext;
    }

    /**
     * Transports may wish to associate additional data with the connection. For
     * example, an SSL transport may use this field to attach the client
     * certificates used when the connection was established.
     *
     * @param transportContext value used to set the transport context
     */
    public void setTransportContext(Object transportContext) {
        this.transportContext = transportContext;
    }
}
