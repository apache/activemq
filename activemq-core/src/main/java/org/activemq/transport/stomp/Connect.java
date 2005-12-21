/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.activemq.command.ConnectionInfo;
import org.activemq.command.ProducerInfo;
import org.activemq.command.Response;
import org.activemq.command.SessionInfo;

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
                        buffer.append(Stomp.Responses.CONNECTED).append(Stomp.NEWLINE);
                        buffer.append(Stomp.Headers.Connected.SESSION).append(Stomp.Headers.SEPERATOR).append(connectionInfo.getClientId()).append(Stomp.NEWLINE).append(
                                Stomp.NEWLINE);
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
