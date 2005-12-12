/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ActiveMQMessage;
import org.activemq.command.MessageAck;
import org.activemq.command.TransactionId;

import java.io.DataInput;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.List;
import java.util.Properties;

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

        List listeners = format.getAckListeners();
        for (int i = 0; i < listeners.size(); i++) {
            AckListener listener = (AckListener) listeners.get(i);
            if (listener.handle(message_id)) {
                listeners.remove(i);
                ActiveMQMessage msg = listener.getMessage();
                MessageAck ack = new MessageAck();
                ack.setDestination((ActiveMQDestination) msg.getJMSDestination());
                ack.setConsumerId(listener.getConsumerId());
                ack.setMessageID(msg.getMessageId());
                ack.setAckType(MessageAck.STANDARD_ACK_TYPE);

                /*
                 * ack.setMessageRead(true);
                 * ack.setProducerKey(msg.getProducerKey());
                 * ack.setSequenceNumber(msg.getSequenceNumber());
                 * ack.setPersistent(msg.getJMSDeliveryMode() ==
                 * DeliveryMode.PERSISTENT);
                 * ack.setSessionId(format.getSessionId());
                 */

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
        while ((in.readByte()) != 0) {
        }
        throw new ProtocolException("Unexepected ACK received for message-id [" + message_id + "]");
    }
}
