/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.jms.pool;

import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

/**
 * A pooled {@link MessageProducer}
 */
public class PooledProducer implements MessageProducer {

    private final MessageProducer messageProducer;
    private final Destination destination;

    private int deliveryMode;
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int priority;
    private long timeToLive;
    private boolean anonymous = true;

    public PooledProducer(MessageProducer messageProducer, Destination destination) throws JMSException {
        this.messageProducer = messageProducer;
        this.destination = destination;
        this.anonymous = messageProducer.getDestination() == null;

        this.deliveryMode = messageProducer.getDeliveryMode();
        this.disableMessageID = messageProducer.getDisableMessageID();
        this.disableMessageTimestamp = messageProducer.getDisableMessageTimestamp();
        this.priority = messageProducer.getPriority();
        this.timeToLive = messageProducer.getTimeToLive();
    }

    @Override
    public void close() throws JMSException {
        if (!anonymous) {
            this.messageProducer.close();
        }
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, getDeliveryMode(), getPriority(), getTimeToLive());
    }

    @Override
    public void send(Message message) throws JMSException {
        send(destination, message, getDeliveryMode(), getPriority(), getTimeToLive());
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {

        if (destination == null) {
            if (messageProducer.getDestination() == null) {
                throw new UnsupportedOperationException("A destination must be specified.");
            }
            throw new InvalidDestinationException("Don't understand null destinations");
        }

        MessageProducer messageProducer = getMessageProducer();

        // just in case let only one thread send at once
        synchronized (messageProducer) {

            if (anonymous && this.destination != null && !this.destination.equals(destination)) {
                throw new UnsupportedOperationException("This producer can only send messages to: " + this.destination);
            }

            // Producer will do it's own Destination validation so always use the destination
            // based send method otherwise we might violate a JMS rule.
            messageProducer.send(destination, message, deliveryMode, priority, timeToLive);
        }
    }

    @Override
    public Destination getDestination() {
        return destination;
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public void setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    @Override
    public boolean getDisableMessageID() {
        return disableMessageID;
    }

    @Override
    public void setDisableMessageID(boolean disableMessageID) {
        this.disableMessageID = disableMessageID;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        return disableMessageTimestamp;
    }

    @Override
    public void setDisableMessageTimestamp(boolean disableMessageTimestamp) {
        this.disableMessageTimestamp = disableMessageTimestamp;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public void setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected MessageProducer getMessageProducer() {
        return messageProducer;
    }

    protected boolean isAnonymous() {
        return anonymous;
    }

    @Override
    public String toString() {
        return "PooledProducer { " + messageProducer + " }";
    }
}
