/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import javax.jms.JMSException;

import java.io.DataInput;
import java.io.IOException;
import java.util.Properties;

interface Command {
    public CommandEnvelope build(String commandLine, DataInput in) throws IOException, JMSException;

    /**
     * Returns a command instance which always returns null for a packet
     */
    StompCommand NULL_COMMAND = new StompCommand() {
        public CommandEnvelope build(String commandLine, DataInput in) {
            return new CommandEnvelope(null, new Properties());
        }
    };
}
