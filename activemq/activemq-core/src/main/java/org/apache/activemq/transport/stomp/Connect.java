/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.stomp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.util.IntrospectionSupport;

class Connect implements StompCommand {
    private HeaderParser headerParser = new HeaderParser();
    private StompWireFormat format;

    Connect(StompWireFormat format) {
        this.format = format;
    }

    public CommandEnvelope build(String commandLine, DataInput in) throws IOException {
        
        Properties headers = headerParser.parse(in);
        
        
        // allow anyone to login for now
        String login = headers.getProperty(Stomp.Headers.Connect.LOGIN);
        String passcode = headers.getProperty(Stomp.Headers.Connect.PASSCODE);
        String clientId = headers.getProperty(Stomp.Headers.Connect.CLIENT_ID);
        
        final ConnectionInfo connectionInfo = new ConnectionInfo();
        
        IntrospectionSupport.setProperties(connectionInfo, headers, "activemq.");
        
        connectionInfo.setConnectionId(format.getConnectionId());
        if( clientId!=null )
            connectionInfo.setClientId(clientId);
        else
            connectionInfo.setClientId(""+connectionInfo.getConnectionId().toString());
        connectionInfo.setResponseRequired(true);
        connectionInfo.setUserName(login);
        connectionInfo.setPassword(passcode);

        while (in.readByte() != 0) {
        }
        
        return new CommandEnvelope(connectionInfo, headers, new ResponseListener() {
            public boolean onResponse(Response receipt, DataOutput out) throws IOException {
                
                if (receipt.getCorrelationId() != connectionInfo.getCommandId())
                    return false;
                
                final SessionInfo sessionInfo = new SessionInfo(format.getSessionId());
                sessionInfo.setCommandId(format.generateCommandId());
                sessionInfo.setResponseRequired(false);
                
                final ProducerInfo producerInfo = new ProducerInfo(format.getProducerId());
                producerInfo.setCommandId(format.generateCommandId());
                producerInfo.setResponseRequired(true);
                
                format.addResponseListener(new ResponseListener() {
                    public boolean onResponse(Response receipt, DataOutput out) throws IOException {
                        if (receipt.getCorrelationId() != producerInfo.getCommandId())
                            return false;
                        
                        format.onFullyConnected();
                        
                        StringBuffer buffer = new StringBuffer();
                        buffer.append(Stomp.Responses.CONNECTED);
                        buffer.append(Stomp.NEWLINE);
                        buffer.append(Stomp.Headers.Connected.SESSION);
                        buffer.append(Stomp.Headers.SEPERATOR);
                        buffer.append(connectionInfo.getClientId());
                        buffer.append(Stomp.NEWLINE);
                        buffer.append(Stomp.NEWLINE);
                        buffer.append(Stomp.NULL);
                        out.writeBytes(buffer.toString());
                        return true;
                    }
                });

                format.addToPendingReadCommands(sessionInfo);
                format.addToPendingReadCommands(producerInfo);
                return true;
            }
        });
    }
}
