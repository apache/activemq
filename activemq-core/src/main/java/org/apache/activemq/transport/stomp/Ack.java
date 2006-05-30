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
import java.io.IOException;
import java.net.ProtocolException;
import java.util.Properties;

import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;

class Ack implements StompCommand {
    private final StompWireFormat format;
    private static final HeaderParser parser = new HeaderParser();

    Ack(StompWireFormat format) {
        this.format = format;
    }

    public CommandEnvelope build(String commandLine, DataInput in) throws IOException {
        Properties headers = parser.parse(in);
        String message_id = headers.getProperty(Stomp.Headers.Ack.MESSAGE_ID);
        if (message_id == null)
            throw new ProtocolException("ACK received without a message-id to acknowledge!");

        Subscription sub = (Subscription) format.getDispachedMap().get(message_id);
        if( sub ==null ) 
            throw new ProtocolException("Unexpected ACK received for message-id [" + message_id + "]");
            
        MessageAck ack = sub.createMessageAck(message_id);
        
        if (headers.containsKey(Stomp.Headers.TRANSACTION)) {
            TransactionId tx_id = format.getTransactionId(headers.getProperty(Stomp.Headers.TRANSACTION));
            if (tx_id == null)
                throw new ProtocolException(headers.getProperty(Stomp.Headers.TRANSACTION) + " is an invalid transaction id");
            ack.setTransactionId(tx_id);
        }

        while ((in.readByte()) != 0) {
        }
        
        return new CommandEnvelope(ack, headers);
    }
}
