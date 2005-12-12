/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.Command;

import java.util.Properties;

public class CommandEnvelope {
    private final Command command;
    private final Properties headers;

    CommandEnvelope(Command command, Properties headers) {
        this.command = command;
        this.headers = headers;
    }

    Properties getHeaders() {
        return headers;
    }

    Command getCommand() {
        return command;
    }
}
