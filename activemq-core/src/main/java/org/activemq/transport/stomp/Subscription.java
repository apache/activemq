/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.ActiveMQBytesMessage;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ActiveMQTextMessage;
import org.activemq.command.ConsumerId;
import org.activemq.command.MessageAck;
import org.activemq.command.RemoveInfo;

import javax.jms.JMSException;

import java.io.DataOutput;
import java.io.IOException;

public class Subscription {
    private ActiveMQDestination destination;
    private int ackMode = 1;
    private StompWireFormat format;
    private final ConsumerId consumerId;
    private final String subscriptionId;
    public static final String NO_ID = "~~ NO SUCH THING ~~%%@#!Q";

    public Subscription(StompWireFormat format, ConsumerId consumerId, String subscriptionId) {
        this.format = format;
        this.consumerId = consumerId;
        this.subscriptionId = subscriptionId;
    }

    void setDestination(ActiveMQDestination actual_dest) {
        this.destination = actual_dest;
    }

    void receive(ActiveMQTextMessage msg, DataOutput out) throws IOException, JMSException {
        if (ackMode == CLIENT_ACK) {
            AckListener listener = new AckListener(msg, consumerId, subscriptionId);
            format.addAckListener(listener);
        }
        else if (ackMode == AUTO_ACK) {
            MessageAck ack = new MessageAck();
            // if (format.isInTransaction())
            // ack.setTransactionIDString(format.getTransactionId());
            ack.setDestination(msg.getDestination());
            ack.setConsumerId(consumerId);
            ack.setMessageID(msg.getMessageId());
            ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
            format.enqueueCommand(ack);
        }
        FrameBuilder builder = new FrameBuilder(Stomp.Responses.MESSAGE).addHeaders(msg).setBody(msg.getText().getBytes());
        if (!subscriptionId.equals(NO_ID)) {
            builder.addHeader(Stomp.Headers.Message.SUBSCRIPTION, subscriptionId);
        }
        out.write(builder.toFrame());
    }

    void receive(ActiveMQBytesMessage msg, DataOutput out) throws IOException, JMSException {
        // @todo refactor this and the other receive form to remoce duplication
        // -bmc
        if (ackMode == CLIENT_ACK) {
            AckListener listener = new AckListener(msg, consumerId, subscriptionId);
            format.addAckListener(listener);
        }
        else if (ackMode == AUTO_ACK) {
            MessageAck ack = new MessageAck();
            // if (format.isInTransaction())
            // ack.setTransactionIDString(format.getTransactionId());
            ack.setDestination(msg.getDestination());
            ack.setConsumerId(consumerId);
            ack.setMessageID(msg.getMessageId());
            ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
            format.enqueueCommand(ack);
        }
        FrameBuilder builder = new FrameBuilder(Stomp.Responses.MESSAGE).addHeaders(msg).setBody(msg.getContent().getData());
        if (!subscriptionId.equals(NO_ID)) {
            builder.addHeader(Stomp.Headers.Message.SUBSCRIPTION, subscriptionId);
        }
        out.write(builder.toFrame());
    }

    ActiveMQDestination getDestination() {
        return destination;
    }

    static final int AUTO_ACK = 1;
    static final int CLIENT_ACK = 2;

    public void setAckMode(int clientAck) {
        this.ackMode = clientAck;
    }

    public RemoveInfo close() {
        RemoveInfo unsub = new RemoveInfo();
        unsub.setObjectId(consumerId);
        return unsub;
    }
}
