/*
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
package org.apache.activemq;

import javax.jms.*;
import javax.jms.IllegalStateException;
import javax.jms.Message;

/**
 * A useful base class for implementing a {@link MessageProducer}
 *
 * @version $Revision: $
 */
public abstract class ActiveMQMessageProducerSupport implements MessageProducer, Closeable {
    protected ActiveMQSession session;
    protected boolean disableMessageID;
    protected boolean disableMessageTimestamp;
    protected int defaultDeliveryMode;
    protected int defaultPriority;
    protected long defaultTimeToLive;

    public ActiveMQMessageProducerSupport(ActiveMQSession session) {
        this.session = session;
        disableMessageTimestamp = session.connection.isDisableTimeStampsByDefault();
    }

    /**
     * Sets whether message IDs are disabled.
     * <P>
     * Since message IDs take some effort to create and increase a message's
     * size, some JMS providers may be able to optimize message overhead if
     * they are given a hint that the message ID is not used by an application.
     * By calling the <CODE>setDisableMessageID</CODE> method on this message
     * producer, a JMS client enables this potential optimization for all
     * messages sent by this message producer. If the JMS provider accepts this
     * hint, these messages must have the message ID set to null; if the
     * provider ignores the hint, the message ID must be set to its normal
     * unique value.
     * <P>
     * Message IDs are enabled by default.
     *
     * @param value indicates if message IDs are disabled
     * @throws javax.jms.JMSException if the JMS provider fails to close the producer due to
     *                      some internal error.
     */
    public void setDisableMessageID(boolean value) throws JMSException {
        checkClosed();
        this.disableMessageID = value;
    }

    /**
     * Gets an indication of whether message IDs are disabled.
     *
     * @return an indication of whether message IDs are disabled
     * @throws javax.jms.JMSException if the JMS provider fails to determine if message IDs are
     *                      disabled due to some internal error.
     */
    public boolean getDisableMessageID() throws JMSException {
        checkClosed();
        return this.disableMessageID;
    }

    /**
     * Sets whether message timestamps are disabled.
     * <P>
     * Since timestamps take some effort to create and increase a message's
     * size, some JMS providers may be able to optimize message overhead if
     * they are given a hint that the timestamp is not used by an application.
     * By calling the <CODE>setDisableMessageTimestamp</CODE> method on this
     * message producer, a JMS client enables this potential optimization for
     * all messages sent by this message producer. If the JMS provider accepts
     * this hint, these messages must have the timestamp set to zero; if the
     * provider ignores the hint, the timestamp must be set to its normal
     * value.
     * <P>
     * Message timestamps are enabled by default.
     *
     * @param value indicates if message timestamps are disabled
     * @throws javax.jms.JMSException if the JMS provider fails to close the producer due to
     *                      some internal error.
     */
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        checkClosed();
        this.disableMessageTimestamp = value;
    }

    /**
     * Gets an indication of whether message timestamps are disabled.
     *
     * @return an indication of whether message timestamps are disabled
     * @throws javax.jms.JMSException if the JMS provider fails to close the producer due to
     *                      some internal error.
     */
    public boolean getDisableMessageTimestamp() throws JMSException {
        checkClosed();
        return this.disableMessageTimestamp;
    }

    /**
     * Sets the producer's default delivery mode.
     * <P>
     * Delivery mode is set to <CODE>PERSISTENT</CODE> by default.
     *
     * @param newDeliveryMode the message delivery mode for this message producer; legal
     *                        values are <code>DeliveryMode.NON_PERSISTENT</code> and
     *                        <code>DeliveryMode.PERSISTENT</code>
     * @throws javax.jms.JMSException if the JMS provider fails to set the delivery mode due to
     *                      some internal error.
     * @see javax.jms.MessageProducer#getDeliveryMode
     * @see javax.jms.DeliveryMode#NON_PERSISTENT
     * @see javax.jms.DeliveryMode#PERSISTENT
     * @see javax.jms.Message#DEFAULT_DELIVERY_MODE
     */
    public void setDeliveryMode(int newDeliveryMode) throws JMSException {
        if (newDeliveryMode != DeliveryMode.PERSISTENT && newDeliveryMode != DeliveryMode.NON_PERSISTENT) {
            throw new javax.jms.IllegalStateException("unkown delivery mode: " + newDeliveryMode);
        }
        checkClosed();
        this.defaultDeliveryMode = newDeliveryMode;
    }

    /**
     * Gets the producer's default delivery mode.
     *
     * @return the message delivery mode for this message producer
     * @throws javax.jms.JMSException if the JMS provider fails to close the producer due to
     *                      some internal error.
     */
    public int getDeliveryMode() throws JMSException {
        checkClosed();
        return this.defaultDeliveryMode;
    }

    /**
     * Sets the producer's default priority.
     * <P>
     * The JMS API defines ten levels of priority value, with 0 as the lowest
     * priority and 9 as the highest. Clients should consider priorities 0-4 as
     * gradations of normal priority and priorities 5-9 as gradations of
     * expedited priority. Priority is set to 4 by default.
     *
     * @param newDefaultPriority the message priority for this message producer; must be a
     *                           value between 0 and 9
     * @throws javax.jms.JMSException if the JMS provider fails to set the delivery mode due to
     *                      some internal error.
     * @see javax.jms.MessageProducer#getPriority
     * @see javax.jms.Message#DEFAULT_PRIORITY
     */
    public void setPriority(int newDefaultPriority) throws JMSException {
        if (newDefaultPriority < 0 || newDefaultPriority > 9) {
            throw new IllegalStateException("default priority must be a value between 0 and 9");
        }
        checkClosed();
        this.defaultPriority = newDefaultPriority;
    }

    /**
     * Gets the producer's default priority.
     *
     * @return the message priority for this message producer
     * @throws javax.jms.JMSException if the JMS provider fails to close the producer due to
     *                      some internal error.
     * @see javax.jms.MessageProducer#setPriority
     */
    public int getPriority() throws JMSException {
        checkClosed();
        return this.defaultPriority;
    }

    /**
     * Sets the default length of time in milliseconds from its dispatch time
     * that a produced message should be retained by the message system.
     * <P>
     * Time to live is set to zero by default.
     *
     * @param timeToLive the message time to live in milliseconds; zero is unlimited
     * @throws javax.jms.JMSException if the JMS provider fails to set the time to live due to
     *                      some internal error.
     * @see javax.jms.MessageProducer#getTimeToLive
     * @see javax.jms.Message#DEFAULT_TIME_TO_LIVE
     */
    public void setTimeToLive(long timeToLive) throws JMSException {
        if (timeToLive < 0l) {
            throw new IllegalStateException("cannot set a negative timeToLive");
        }
        checkClosed();
        this.defaultTimeToLive = timeToLive;
    }

    /**
     * Gets the default length of time in milliseconds from its dispatch time
     * that a produced message should be retained by the message system.
     *
     * @return the message time to live in milliseconds; zero is unlimited
     * @throws javax.jms.JMSException if the JMS provider fails to get the time to live due to
     *                      some internal error.
     * @see javax.jms.MessageProducer#setTimeToLive
     */
    public long getTimeToLive() throws JMSException {
        checkClosed();
        return this.defaultTimeToLive;
    }

    /**
     * Sends a message using the <CODE>MessageProducer</CODE>'s default
     * delivery mode, priority, and time to live.
     *
     * @param message the message to send
     * @throws javax.jms.JMSException                if the JMS provider fails to send the message due to some
     *                                     internal error.
     * @throws javax.jms.MessageFormatException      if an invalid message is specified.
     * @throws javax.jms.InvalidDestinationException if a client uses this method with a <CODE>
     *                                     MessageProducer</CODE> with an invalid destination.
     * @throws UnsupportedOperationException
     *                                     if a client uses this method with a <CODE>
     *                                     MessageProducer</CODE> that did not specify a
     *                                     destination at creation time.
     * @see javax.jms.Session#createProducer
     * @see javax.jms.MessageProducer
     * @since 1.1
     */
    public void send(Message message) throws JMSException {
        this.send(this.getDestination(),
                  message,
                  this.defaultDeliveryMode,
                  this.defaultPriority,
                  this.defaultTimeToLive);
    }

    /**
     * Sends a message to the destination, specifying delivery mode, priority,
     * and time to live.
     *
     * @param message      the message to send
     * @param deliveryMode the delivery mode to use
     * @param priority     the priority for this message
     * @param timeToLive   the message's lifetime (in milliseconds)
     * @throws javax.jms.JMSException                if the JMS provider fails to send the message due to some
     *                                     internal error.
     * @throws javax.jms.MessageFormatException      if an invalid message is specified.
     * @throws javax.jms.InvalidDestinationException if a client uses this method with a <CODE>
     *                                     MessageProducer</CODE> with an invalid destination.
     * @throws UnsupportedOperationException
     *                                     if a client uses this method with a <CODE>
     *                                     MessageProducer</CODE> that did not specify a
     *                                     destination at creation time.
     * @see javax.jms.Session#createProducer
     * @since 1.1
     */
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        this.send(this.getDestination(),
                  message,
                  deliveryMode,
                  priority,
                  timeToLive);
    }

    /**
     * Sends a message to a destination for an unidentified message producer.
     * Uses the <CODE>MessageProducer</CODE>'s default delivery mode,
     * priority, and time to live.
     * <P>
     * Typically, a message producer is assigned a destination at creation
     * time; however, the JMS API also supports unidentified message producers,
     * which require that the destination be supplied every time a message is
     * sent.
     *
     * @param destination the destination to send this message to
     * @param message     the message to send
     * @throws javax.jms.JMSException                if the JMS provider fails to send the message due to some
     *                                     internal error.
     * @throws javax.jms.MessageFormatException      if an invalid message is specified.
     * @throws javax.jms.InvalidDestinationException if a client uses this method with an invalid destination.
     * @throws UnsupportedOperationException
     *                                     if a client uses this method with a <CODE>
     *                                     MessageProducer</CODE> that specified a destination at
     *                                     creation time.
     * @see javax.jms.Session#createProducer
     * @see javax.jms.MessageProducer
     */
    public void send(Destination destination, Message message) throws JMSException {
        this.send(destination,
                  message,
                  this.defaultDeliveryMode,
                  this.defaultPriority,
                  this.defaultTimeToLive);
    }


    protected abstract void checkClosed() throws IllegalStateException;
}
