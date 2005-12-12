/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.Command;
import org.activemq.command.Response;

import javax.jms.JMSException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.ProtocolException;

class CommandParser {
    private String clientId;
    private final StompWireFormat format;

    CommandParser(StompWireFormat wireFormat) {
        format = wireFormat;
    }

    Command parse(DataInput in) throws IOException, JMSException {
        String line;

        // skip white space to next real line
        try {
            while ((line = in.readLine()).trim().length() == 0) {
            }
        }
        catch (NullPointerException e) {
            throw new IOException("connection was closed");
        }

        // figure corrent command and return it
        StompCommand command = null;
        if (line.startsWith(Stomp.Commands.SUBSCRIBE))
            command = new Subscribe(format);
        if (line.startsWith(Stomp.Commands.SEND))
            command = new Send(format);
        if (line.startsWith(Stomp.Commands.DISCONNECT))
            command = new Disconnect();
        if (line.startsWith(Stomp.Commands.BEGIN))
            command = new Begin(format);
        if (line.startsWith(Stomp.Commands.COMMIT))
            command = new Commit(format);
        if (line.startsWith(Stomp.Commands.ABORT))
            command = new Abort(format);
        if (line.startsWith(Stomp.Commands.UNSUBSCRIBE))
            command = new Unsubscribe(format);
        if (line.startsWith(Stomp.Commands.ACK))
            command = new Ack(format);

        if (command == null) {
            while (in.readByte() == 0) {
            }
            throw new ProtocolException("Unknown command [" + line + "]");
        }

        CommandEnvelope envelope = command.build(line, in);
        if (envelope.getHeaders().containsKey(Stomp.Headers.RECEIPT_REQUESTED)) {
            final short id = StompWireFormat.generateCommandId();
            envelope.getCommand().setCommandId(id);
            envelope.getCommand().setResponseRequired(true);
            final String client_packet_key = envelope.getHeaders().getProperty(Stomp.Headers.RECEIPT_REQUESTED);
            format.addResponseListener(new ResponseListener() {
                public boolean onResponse(Response receipt, DataOutput out) throws IOException {
                    if (receipt.getCorrelationId() != id)
                        return false;

                    out.write(new FrameBuilder(Stomp.Responses.RECEIPT).addHeader(Stomp.Headers.Response.RECEIPT_ID, client_packet_key).toFrame());
                    return true;
                }
            });
        }

        return envelope.getCommand();
    }

    void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
