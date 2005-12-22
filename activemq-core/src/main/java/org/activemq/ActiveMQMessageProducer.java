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
package org.activemq;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;

import org.activeio.Disposable;
import org.activemq.command.ActiveMQDestination;
import org.activemq.command.ProducerId;
import org.activemq.command.ProducerInfo;
import org.activemq.management.JMSProducerStatsImpl;
import org.activemq.management.StatsCapable;
import org.activemq.management.StatsImpl;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicLong;

/**
 * A client uses a <CODE>MessageProducer</CODE> object to send messages to a
 * destination. A <CODE>MessageProducer</CODE> object is created by passing a
 * <CODE>Destination</CODE> object to a message-producer creation method
 * supplied by a session.
 * <P>
 * <CODE>MessageProducer</CODE> is the parent interface for all message
 * producers.
 * <P>
 * A client also has the option of creating a message producer without
 * supplying a destination. In this case, a destination must be provided with
 * every send operation. A typical use for this kind of message producer is to
 * send replies to requests using the request's <CODE>JMSReplyTo</CODE>
 * destination.
 * <P>
 * A client can specify a default delivery mode, priority, and time to live for
 * messages sent by a message producer. It can also specify the delivery mode,
 * priority, and time to live for an individual message.
 * <P>
 * A client can specify a time-to-live value in milliseconds for each message
 * it sends. This value defines a message expiration time that is the sum of
 * the message's time-to-live and the GMT when it is sent (for transacted
 * sends, this is the time the client sends the message, not the time the
 * transaction is committed).
 * <P>
 * A JMS provider should do its best to expire messages accurately; however,
 * the JMS API does not define the accuracy provided.
 *
 * @version $Revision: 1.14 $
 * @see javax.jms.TopicPublisher
 * @see javax.jms.QueueSender
 * @see javax.jms.Session#createProducer
 */
public class ActiveMQMessageProducer implements MessageProducer, StatsCapable, Closeable, Disposable {

    protected ActiveMQSession session;
    protected ProducerInfo info;
    private JMSProducerStatsImpl stats;
    private AtomicLong messageSequence;

    protected boolean closed;
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int defaultDeliveryMode;
    private int defaultPriority;
    private long defaultTimeToLive;
    private long startTime;

    protected ActiveMQMessageProducer(ActiveMQSession session, ProducerId producerId, ActiveMQDestination destination)
            throws JMSException {
        this.session = session;
        this.info = new ProducerInfo(producerId);
        this.info.setDestination(destination);
        this.disableMessageID = false;
        this.disableMessageTimestamp = session.connection.isDisableTimeStampsByDefault();
        this.defaultDeliveryMode = Message.DEFAULT_DELIVERY_MODE;
        this.defaultPriority = Message.DEFAULT_PRIORITY;
        this.defaultTimeToLive = Message.DEFAULT_TIME_TO_LIVE;
        this.startTime = System.currentTimeMillis();
        this.messageSequence = new AtomicLong(0);
        this.stats = new JMSProducerStatsImpl(session.getSessionStats(), destination);
        this.session.addProducer(this);        
        this.session.asyncSendPacket(info);
    }

    public StatsImpl getStats() {
        return stats;
    }

    public JMSProducerStatsImpl getProducerStats() {
        return stats;
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
     * @throws JMSException if the JMS provider fails to close the producer due to
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
     * @throws JMSException if the JMS provider fails to determine if message IDs are
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
     * @throws JMSException if the JMS provider fails to close the producer due to
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
     * @throws JMSException if the JMS provider fails to close the producer due to
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
     * @throws JMSException if the JMS provider fails to set the delivery mode due to
     *                      some internal error.
     * @see javax.jms.MessageProducer#getDeliveryMode
     * @see javax.jms.DeliveryMode#NON_PERSISTENT
     * @see javax.jms.DeliveryMode#PERSISTENT
     * @see javax.jms.Message#DEFAULT_DELIVERY_MODE
     */
    public void setDeliveryMode(int newDeliveryMode) throws JMSException {
        if (newDeliveryMode != DeliveryMode.PERSISTENT && newDeliveryMode != DeliveryMode.NON_PERSISTENT) {
            throw new IllegalStateException("unkown delivery mode: " + newDeliveryMode);
        }
        checkClosed();
        this.defaultDeliveryMode = newDeliveryMode;
    }

    /**
     * Gets the producer's default delivery mode.
     *
     * @return the message delivery mode for this message producer
     * @throws JMSException if the JMS provider fails to close the producer due to
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
     * @throws JMSException if the JMS provider fails to set the delivery mode due to
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
     * @throws JMSException if the JMS provider fails to close the producer due to
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
     * @throws JMSException if the JMS provider fails to set the time to live due to
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
     * @throws JMSException if the JMS provider fails to get the time to live due to
     *                      some internal error.
     * @see javax.jms.MessageProducer#setTimeToLive
     */
    public long getTimeToLive() throws JMSException {
        checkClosed();
        return this.defaultTimeToLive;
    }

    /**
     * Gets the destination associated with this <CODE>MessageProducer</CODE>.
     *
     * @return this producer's <CODE>Destination/ <CODE>
     * @throws JMSException if the JMS provider fails to close the producer due to
     *                      some internal error.
     * @since 1.1
     */
    public Destination getDestination() throws JMSException {
        checkClosed();
        return this.info.getDestination();
    }

    /**
     * Closes the message producer.
     * <P>
     * Since a provider may allocate some resources on behalf of a <CODE>
     * MessageProducer</CODE> outside the Java virtual machine, clients should
     * close them when they are not needed. Relying on garbage collection to
     * eventually reclaim these resources may not be timely enough.
     *
     * @throws JMSException if the JMS provider fails to close the producer due to
     *                      some internal error.
     */
    public void close() throws JMSException {
        if( closed==false ) {
            dispose();
            this.session.asyncSendPacket(info.createRemoveCommand());
        }
    }

    public void dispose() {
        if( closed==false ) {
            this.session.removeProducer(this);
            closed = true;
        }
    }


    /**
     * Check if the instance of this producer has been closed.
     * @throws IllegalStateException
     */
    protected void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("The producer is closed");
        }
    }

    /**
     * Sends a message using the <CODE>MessageProducer</CODE>'s default
     * delivery mode, priority, and time to live.
     *
     * @param message the message to send
     * @throws JMSException                if the JMS provider fails to send the message due to some
     *                                     internal error.
     * @throws MessageFormatException      if an invalid message is specified.
     * @throws InvalidDestinationException if a client uses this method with a <CODE>
     *                                     MessageProducer</CODE> with an invalid destination.
     * @throws java.lang.UnsupportedOperationException
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
     * @throws JMSException                if the JMS provider fails to send the message due to some
     *                                     internal error.
     * @throws MessageFormatException      if an invalid message is specified.
     * @throws InvalidDestinationException if a client uses this method with a <CODE>
     *                                     MessageProducer</CODE> with an invalid destination.
     * @throws java.lang.UnsupportedOperationException
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
     * @throws JMSException                if the JMS provider fails to send the message due to some
     *                                     internal error.
     * @throws MessageFormatException      if an invalid message is specified.
     * @throws InvalidDestinationException if a client uses this method with an invalid destination.
     * @throws java.lang.UnsupportedOperationException
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

    /**
     * Sends a message to a destination for an unidentified message producer,
     * specifying delivery mode, priority and time to live.
     * <P>
     * Typically, a message producer is assigned a destination at creation
     * time; however, the JMS API also supports unidentified message producers,
     * which require that the destination be supplied every time a message is
     * sent.
     *
     * @param destination  the destination to send this message to
     * @param message      the message to send
     * @param deliveryMode the delivery mode to use
     * @param priority     the priority for this message
     * @param timeToLive   the message's lifetime (in milliseconds)
     * @throws JMSException                if the JMS provider fails to send the message due to some
     *                                     internal error.
     * @throws UnsupportedOperationException   if an invalid destination is specified.
     * @throws InvalidDestinationException if a client uses this method with an invalid destination.
     * @see javax.jms.Session#createProducer
     * @since 1.1
     */
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        checkClosed();
        if (destination == null) {
            if( info.getDestination() == null ) {
                throw new UnsupportedOperationException("A destination must be specified.");
            }
            throw new InvalidDestinationException("Don't understand null destinations");
        }

        ActiveMQDestination dest; 
        if( destination == info.getDestination() ) {
            dest = (ActiveMQDestination) destination;
        } else  if ( info.getDestination() == null ) {
            dest = ActiveMQDestination.transform(destination);
        } else {
            throw new UnsupportedOperationException("This producer can only send messages to: " + this.info.getDestination().getPhysicalName());
        }
        
        this.session.send(this, dest, message, deliveryMode, priority, timeToLive);
        stats.onMessage();            
    }

    /**
     * @return the time in milli second when this object was created.
     */
    protected long getStartTime() {
        return this.startTime;
    }

    /**
     * @return Returns the messageSequence.
     */
    protected long getMessageSequence() {
        return messageSequence.incrementAndGet();
    }

    /**
     * @param messageSequence The messageSequence to set.
     */
    protected void setMessageSequence(AtomicLong messageSequence) {
        this.messageSequence = messageSequence;
    }

    /**
     * @return Returns the info.
     */
    protected ProducerInfo getProducerInfo(){
        return this.info!=null?this.info:null;
    }

    /**
     * @param info The info to set
     */
    protected  void setProducerInfo(ProducerInfo info){
        this.info = info;
    }

    public String toString() {
        return "ActiveMQMessageProducer { consumerId=" +info.getProducerId()+" }";
    }

}
