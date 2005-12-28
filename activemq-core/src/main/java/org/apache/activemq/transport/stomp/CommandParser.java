/**
 *
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.activemq.command.Command;
import org.apache.activemq.command.Response;

import javax.jms.JMSException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.ProtocolException;

class CommandParser {
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

        // figure correct command and return it
        StompCommand command = null;
        if (line.startsWith(Stomp.Commands.CONNECT))
            command = new Connect(format);
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

        final CommandEnvelope envelope = command.build(line, in);
        final short commandId = format.generateCommandId();
        final String client_packet_key = envelope.getHeaders().getProperty(Stomp.Headers.RECEIPT_REQUESTED);
        final boolean receiptRequested = client_packet_key!=null;
        
        envelope.getCommand().setCommandId(commandId);
        if (receiptRequested || envelope.getResponseListener()!=null ) {
            envelope.getCommand().setResponseRequired(true);
            if( envelope.getResponseListener()!=null ) {
                format.addResponseListener(envelope.getResponseListener());
            } else {
                format.addResponseListener(new ResponseListener() {
                    public boolean onResponse(Response receipt, DataOutput out) throws IOException {
                        if (receipt.getCorrelationId() != commandId)
                            return false;
                        out.write(new FrameBuilder(Stomp.Responses.RECEIPT).addHeader(Stomp.Headers.Response.RECEIPT_ID, client_packet_key).toFrame());
                        return true;
                    }
                });
            }
        }

        return envelope.getCommand();
    }
}
