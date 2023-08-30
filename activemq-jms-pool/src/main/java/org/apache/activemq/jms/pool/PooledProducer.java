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

import jakarta.jms.CompletionListener;
import jakarta.jms.Destination;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;

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
    
    /** 
     *
     * @param message the message to send
     * @param CompletionListener to callback
     * @throws JMSException if the JMS provider fails to send the message due to
     *                 some internal error.
     * @throws UnsupportedOperationException if an invalid destination is
     *                 specified.
     * @throws InvalidDestinationException if a client uses this method with an
     *                 invalid destination.
     * @see jakarta.jms.Session#createProducer
     * @since 2.0
     */
    @Override
    public void send(Message message, CompletionListener completionListener) throws JMSException {
       throw new UnsupportedOperationException("send(Message, CompletionListener) is not supported");

    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive,
                     CompletionListener completionListener) throws JMSException {
        throw new UnsupportedOperationException("send(Message, deliveryMode, priority, timetoLive, CompletionListener) is not supported");
    }

    @Override
    public void send(Destination destination, Message message, CompletionListener completionListener)
                     throws JMSException {
        throw new UnsupportedOperationException("send(Destination, Message, CompletionListener) is not supported");
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive,
                     CompletionListener completionListener) throws JMSException {
        throw new UnsupportedOperationException("send(Destination, Message, deliveryMode, priority, timetoLive, CompletionListener) is not supported");
    }

    /**
     * Gets the delivery delay associated with this <CODE>MessageProducer</CODE>.
     *
     * @return this producer's <CODE>DeliveryDely/ <CODE>
     * @throws JMSException if the JMS provider fails to close the producer due to
     *                      some internal error.
     * @since 2.0
     */
    @Override
    public void setDeliveryDelay(long deliveryDelay) throws JMSException {
        throw new UnsupportedOperationException("setDeliveryDelay() is not supported");
    }

    /**
     * Gets the delivery delay value for this <CODE>MessageProducer</CODE>.
     *
     * @return the delivery delay for this messageProducer
     * @throws jakarta.jms.JMSException if the JMS provider fails to determine if deliver delay is
     *                      disabled due to some internal error.
     */
    @Override
    public long getDeliveryDelay() throws JMSException {
        throw new UnsupportedOperationException("getDeliveryDelay() is not supported");
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
