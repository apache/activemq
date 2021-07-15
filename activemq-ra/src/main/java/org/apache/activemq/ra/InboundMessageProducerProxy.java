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
package org.apache.activemq.ra;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

/**
 * An implementation of {@link MessageProducer} which uses the ActiveMQ JCA ResourceAdapter's
 * current thread's JMS {@link javax.jms.Session} to send messages.
 *
 * 
 */
public class InboundMessageProducerProxy implements MessageProducer, QueueSender, TopicPublisher {
    
    private MessageProducer messageProducer;
    private Destination destination;
    private int deliveryMode;
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int priority;
    private long timeToLive;

    public InboundMessageProducerProxy(MessageProducer messageProducer, Destination destination) throws JMSException {
        this.messageProducer = messageProducer;
        this.destination = destination;

        this.deliveryMode = messageProducer.getDeliveryMode();
        this.disableMessageID = messageProducer.getDisableMessageID();
        this.disableMessageTimestamp = messageProducer.getDisableMessageTimestamp();
        this.priority = messageProducer.getPriority();
        this.timeToLive = messageProducer.getTimeToLive();
    }

    public void close() throws JMSException {
        // do nothing as we just go back into the pool
        // though lets reset the various settings which may have been changed
        messageProducer.setDeliveryMode(deliveryMode);
        messageProducer.setDisableMessageID(disableMessageID);
        messageProducer.setDisableMessageTimestamp(disableMessageTimestamp);
        messageProducer.setPriority(priority);
        messageProducer.setTimeToLive(timeToLive);
    }

    public Destination getDestination() throws JMSException {
        return destination;
    }

    public int getDeliveryMode() throws JMSException {
        return messageProducer.getDeliveryMode();
    }

    public boolean getDisableMessageID() throws JMSException {
        return messageProducer.getDisableMessageID();
    }

    public boolean getDisableMessageTimestamp() throws JMSException {
        return messageProducer.getDisableMessageTimestamp();
    }

    public int getPriority() throws JMSException {
        return messageProducer.getPriority();
    }

    public long getTimeToLive() throws JMSException {
        return messageProducer.getTimeToLive();
    }

    public void send(Destination destination, Message message) throws JMSException {
        if (destination == null) {
            destination = this.destination;
        }
        messageProducer.send(destination, message);
    }

    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        if (destination == null) {
            destination = this.destination;
        }
        messageProducer.send(destination, message, deliveryMode, priority, timeToLive);
    }

    public void send(Message message) throws JMSException {
        messageProducer.send(destination, message);
    }

    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        messageProducer.send(destination, message, deliveryMode, priority, timeToLive);
    }

    public void setDeliveryMode(int i) throws JMSException {
        messageProducer.setDeliveryMode(i);
    }

    public void setDisableMessageID(boolean b) throws JMSException {
        messageProducer.setDisableMessageID(b);
    }

    public void setDisableMessageTimestamp(boolean b) throws JMSException {
        messageProducer.setDisableMessageTimestamp(b);
    }

    public void setPriority(int i) throws JMSException {
        messageProducer.setPriority(i);
    }

    public void setTimeToLive(long l) throws JMSException {
        messageProducer.setTimeToLive(l);
    }

    public Queue getQueue() throws JMSException {
        return (Queue) messageProducer.getDestination();
    }

    public void send(Queue arg0, Message arg1) throws JMSException {
        messageProducer.send(arg0, arg1);
    }

    public void send(Queue arg0, Message arg1, int arg2, int arg3, long arg4) throws JMSException {
        messageProducer.send(arg0, arg1, arg2, arg3, arg4);
    }

    public Topic getTopic() throws JMSException {
        return (Topic) messageProducer.getDestination();
    }

    public void publish(Message arg0) throws JMSException {
        messageProducer.send(arg0);
    }

    public void publish(Message arg0, int arg1, int arg2, long arg3) throws JMSException {
        messageProducer.send(arg0, arg1, arg2, arg3);
    }

    public void publish(Topic arg0, Message arg1) throws JMSException {
        messageProducer.send(arg0, arg1);
    }

    public void publish(Topic arg0, Message arg1, int arg2, int arg3, long arg4) throws JMSException {
        messageProducer.send(arg0, arg1, arg2, arg3, arg4);
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
     * @see javax.jms.Session#createProducer
     * @since 2.0
     */
    @Override
    public void send(Message message, CompletionListener completionListener) throws JMSException {
       throw new UnsupportedOperationException("send(Message, CompletionListener) is not supported");

    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
        throw new UnsupportedOperationException("send(Message, deliveryMode, priority, timetoLive, CompletionListener) is not supported");
    }

    @Override
    public void send(Destination destination, Message message, CompletionListener completionListener) throws JMSException {
        throw new UnsupportedOperationException("send(Destination, Message, CompletionListener) is not supported");
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
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
     * @throws javax.jms.JMSException if the JMS provider fails to determine if deliver delay is
     *                      disabled due to some internal error.
     */
    @Override
    public long getDeliveryDelay() throws JMSException {
        throw new UnsupportedOperationException("getDeliveryDelay() is not supported");
    }
}
