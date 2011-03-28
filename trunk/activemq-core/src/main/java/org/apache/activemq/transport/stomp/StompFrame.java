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
import java.util.Iterator;
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

    //
    // Methods in the Command interface
    //
    public int getCommandId() {
        return 0;
    }

    public Endpoint getFrom() {
        return null;
    }

    public Endpoint getTo() {
        return null;
    }

    public boolean isBrokerInfo() {
        return false;
    }

    public boolean isMessage() {
        return false;
    }

    public boolean isMessageAck() {
        return false;
    }

    public boolean isMessageDispatch() {
        return false;
    }

    public boolean isMessageDispatchNotification() {
        return false;
    }

    public boolean isResponse() {
        return false;
    }

    public boolean isResponseRequired() {
        return false;
    }

    public boolean isShutdownInfo() {
        return false;
    }

    public boolean isConnectionControl() {
        return false;
    }

    public boolean isWireFormatInfo() {
        return false;
    }

    public void setCommandId(int value) {
    }

    public void setFrom(Endpoint from) {
    }

    public void setResponseRequired(boolean responseRequired) {
    }

    public void setTo(Endpoint to) {
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        return null;
    }

    public byte getDataStructureType() {
        return 0;
    }

    public boolean isMarshallAware() {
        return false;
    }

    public String toString() {
        return format(true);
    }

    public String format() {
        return format(false);
    }

    public String format(boolean forLogging) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(getAction());
        buffer.append("\n");
        Map headers = getHeaders();
        for (Iterator iter = headers.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry)iter.next();
            buffer.append(entry.getKey());
            buffer.append(":");
            if (forLogging && entry.getKey().toString().toLowerCase().contains(Stomp.Headers.Connect.PASSCODE)) {
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
        return buffer.toString();
    }
}
