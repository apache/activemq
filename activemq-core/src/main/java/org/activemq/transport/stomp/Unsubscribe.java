/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.ActiveMQDestination;

import java.io.DataInput;
import java.io.IOException;
import java.util.Properties;

public class Unsubscribe implements StompCommand {
    private static final HeaderParser parser = new HeaderParser();
    private final StompWireFormat format;

    Unsubscribe(StompWireFormat format) {
        this.format = format;
    }

    public CommandEnvelope build(String commandLine, DataInput in) throws IOException {
        Properties headers = parser.parse(in);
        while (in.readByte() == 0) {
        }

        String dest_name = headers.getProperty(Stomp.Headers.Unsubscribe.DESTINATION);
        ActiveMQDestination destination = DestinationNamer.convert(dest_name);

        Subscription s = format.getSubscriptionFor(destination);
        return new CommandEnvelope(s.close(), headers);
    }
}
