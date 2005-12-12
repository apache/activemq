/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.ShutdownInfo;

import java.io.DataInput;
import java.io.IOException;
import java.util.Properties;

class Disconnect implements StompCommand {

    public CommandEnvelope build(String line, DataInput in) throws IOException {
        while (in.readByte() != 0) {
        }
        return new CommandEnvelope(new ShutdownInfo(), new Properties());
    }
}
