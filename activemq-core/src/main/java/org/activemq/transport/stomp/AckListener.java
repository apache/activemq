/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.ActiveMQMessage;
import org.activemq.command.ConsumerId;

class AckListener {
    private final ActiveMQMessage msg;
    private final ConsumerId consumerId;
    private final String subscriptionId;

    public AckListener(ActiveMQMessage msg, ConsumerId consumerId, String subscriptionId) {
        this.msg = msg;
        this.consumerId = consumerId;
        this.subscriptionId = subscriptionId;
    }

    boolean handle(String messageId) {
        return msg.getJMSMessageID().equals(messageId);
    }

    public ActiveMQMessage getMessage() {
        return msg;
    }

    public ConsumerId getConsumerId() {
        return consumerId;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }
}
