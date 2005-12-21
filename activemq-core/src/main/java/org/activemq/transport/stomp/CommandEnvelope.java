/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.Command;

import java.util.Properties;

public class CommandEnvelope {
    
    private final Command command;
    private final Properties headers;
    private final ResponseListener responseListener;
    
    public CommandEnvelope(Command command, Properties headers) {
        this(command, headers, null);
    }
    
    public CommandEnvelope(Command command, Properties headers, ResponseListener responseListener) {
        this.command = command;
        this.headers = headers;
        this.responseListener = responseListener;
    }

    public Properties getHeaders() {
        return headers;
    }

    public Command getCommand() {
        return command;
    }

    public ResponseListener getResponseListener() {
        return responseListener;
    }
    
}
