/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ConsumerId;
import org.activemq.command.ConsumerInfo;

import java.io.DataInput;
import java.io.IOException;
import java.util.Properties;

class Subscribe implements StompCommand {
    private HeaderParser headerParser = new HeaderParser();
    private StompWireFormat format;

    Subscribe(StompWireFormat format) {
        this.format = format;
    }

    public CommandEnvelope build(String commandLine, DataInput in) throws IOException {
        ConsumerInfo ci = new ConsumerInfo();
        Properties headers = headerParser.parse(in);
        String destination = headers.getProperty(Stomp.Headers.Subscribe.DESTINATION);
        ActiveMQDestination actual_dest = DestinationNamer.convert(destination);
        ci.setDestination(DestinationNamer.convert(destination));
        ConsumerId consumerId = format.createConsumerId();
        ci.setConsumerId(consumerId);
        ci.setResponseRequired(true);
        // ci.setSessionId(format.getSessionId());
        while (in.readByte() != 0) {
        }
        String subscriptionId = headers.getProperty(Stomp.Headers.Subscribe.ID, Subscription.NO_ID);
        Subscription s = new Subscription(format, consumerId, subscriptionId);
        s.setDestination(actual_dest);
        String ack_mode_key = headers.getProperty(Stomp.Headers.Subscribe.ACK_MODE);
        if (ack_mode_key != null && ack_mode_key.equals(Stomp.Headers.Subscribe.AckModeValues.CLIENT)) {
            s.setAckMode(Subscription.CLIENT_ACK);
        }

        format.addSubscription(s);
        return new CommandEnvelope(ci, headers);
    }
}
