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
package org.apache.activemq;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.management.JMSProducerStatsImpl;
import org.apache.activemq.management.StatsCapable;
import org.apache.activemq.management.StatsImpl;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.util.IntrospectionSupport;

/**
 * A client uses a <CODE>MessageProducer</CODE> object to send messages to a
 * destination. A <CODE>MessageProducer</CODE> object is created by passing a
 * <CODE>Destination</CODE> object to a message-producer creation method
 * supplied by a session.
 * <P>
 * <CODE>MessageProducer</CODE> is the parent interface for all message
 * producers.
 * <P>
 * A client also has the option of creating a message producer without supplying
 * a destination. In this case, a destination must be provided with every send
 * operation. A typical use for this kind of message producer is to send replies
 * to requests using the request's <CODE>JMSReplyTo</CODE> destination.
 * <P>
 * A client can specify a default delivery mode, priority, and time to live for
 * messages sent by a message producer. It can also specify the delivery mode,
 * priority, and time to live for an individual message.
 * <P>
 * A client can specify a time-to-live value in milliseconds for each message it
 * sends. This value defines a message expiration time that is the sum of the
 * message's time-to-live and the GMT when it is sent (for transacted sends,
 * this is the time the client sends the message, not the time the transaction
 * is committed).
 * <P>
 * A JMS provider should do its best to expire messages accurately; however, the
 * JMS API does not define the accuracy provided.
 * 
 * @version $Revision: 1.14 $
 * @see javax.jms.TopicPublisher
 * @see javax.jms.QueueSender
 * @see javax.jms.Session#createProducer
 */
public class ActiveMQMessageProducer extends ActiveMQMessageProducerSupport implements StatsCapable, Disposable {

    protected ProducerInfo info;
    protected boolean closed;

    private final JMSProducerStatsImpl stats;
    private AtomicLong messageSequence;
    private final long startTime;
    private MessageTransformer transformer;
    private MemoryUsage producerWindow;

    protected ActiveMQMessageProducer(ActiveMQSession session, ProducerId producerId, ActiveMQDestination destination, int sendTimeout) throws JMSException {
        super(session);
        this.info = new ProducerInfo(producerId);
        this.info.setWindowSize(session.connection.getProducerWindowSize());
        if (destination != null && destination.getOptions() != null) {
            Map<String, String> options = new HashMap<String, String>(destination.getOptions());
            IntrospectionSupport.setProperties(this.info, options, "producer.");
        }
        this.info.setDestination(destination);

        // Enable producer window flow control if protocol > 3 and the window
        // size > 0
        if (session.connection.getProtocolVersion() >= 3 && this.info.getWindowSize() > 0) {
            producerWindow = new MemoryUsage("Producer Window: " + producerId);
            producerWindow.setExecutor(session.getConnectionExecutor());
            producerWindow.setLimit(this.info.getWindowSize());
            producerWindow.start();
        }

        this.defaultDeliveryMode = Message.DEFAULT_DELIVERY_MODE;
        this.defaultPriority = Message.DEFAULT_PRIORITY;
        this.defaultTimeToLive = Message.DEFAULT_TIME_TO_LIVE;
        this.startTime = System.currentTimeMillis();
        this.messageSequence = new AtomicLong(0);
        this.stats = new JMSProducerStatsImpl(session.getSessionStats(), destination);
        this.session.addProducer(this);
        this.session.asyncSendPacket(info);
        this.setSendTimeout(sendTimeout);
        setTransformer(session.getTransformer());
    }

    public StatsImpl getStats() {
        return stats;
    }

    public JMSProducerStatsImpl getProducerStats() {
        return stats;
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
     * MessageProducer</CODE>
     * outside the Java virtual machine, clients should close them when they are
     * not needed. Relying on garbage collection to eventually reclaim these
     * resources may not be timely enough.
     * 
     * @throws JMSException if the JMS provider fails to close the producer due
     *                 to some internal error.
     */
    public void close() throws JMSException {
        if (!closed) {
            dispose();
            this.session.asyncSendPacket(info.createRemoveCommand());
        }
    }

    public void dispose() {
        if (!closed) {
            this.session.removeProducer(this);
            if (producerWindow != null) {
                producerWindow.stop();
            }
            closed = true;
        }
    }

    /**
     * Check if the instance of this producer has been closed.
     * 
     * @throws IllegalStateException
     */
    @Override
    protected void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("The producer is closed");
        }
    }

    /**
     * Sends a message to a destination for an unidentified message producer,
     * specifying delivery mode, priority and time to live.
     * <P>
     * Typically, a message producer is assigned a destination at creation time;
     * however, the JMS API also supports unidentified message producers, which
     * require that the destination be supplied every time a message is sent.
     * 
     * @param destination the destination to send this message to
     * @param message the message to send
     * @param deliveryMode the delivery mode to use
     * @param priority the priority for this message
     * @param timeToLive the message's lifetime (in milliseconds)
     * @throws JMSException if the JMS provider fails to send the message due to
     *                 some internal error.
     * @throws UnsupportedOperationException if an invalid destination is
     *                 specified.
     * @throws InvalidDestinationException if a client uses this method with an
     *                 invalid destination.
     * @see javax.jms.Session#createProducer
     * @since 1.1
     */
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        checkClosed();
        if (destination == null) {
            if (info.getDestination() == null) {
                throw new UnsupportedOperationException("A destination must be specified.");
            }
            throw new InvalidDestinationException("Don't understand null destinations");
        }

        ActiveMQDestination dest;
        if (destination == info.getDestination()) {
            dest = (ActiveMQDestination)destination;
        } else if (info.getDestination() == null) {
            dest = ActiveMQDestination.transform(destination);
        } else {
            throw new UnsupportedOperationException("This producer can only send messages to: " + this.info.getDestination().getPhysicalName());
        }
        if (dest == null) {
            throw new JMSException("No destination specified");
        }

        if (transformer != null) {
            Message transformedMessage = transformer.producerTransform(session, this, message);
            if (transformedMessage != null) {
                message = transformedMessage;
            }
        }

        if (producerWindow != null) {
            try {
                producerWindow.waitForSpace();
            } catch (InterruptedException e) {
                throw new JMSException("Send aborted due to thread interrupt.");
            }
        }

        this.session.send(this, dest, message, deliveryMode, priority, timeToLive, producerWindow,sendTimeout);

        stats.onMessage();
    }

    public MessageTransformer getTransformer() {
        return transformer;
    }

    /**
     * Sets the transformer used to transform messages before they are sent on
     * to the JMS bus
     */
    public void setTransformer(MessageTransformer transformer) {
        this.transformer = transformer;
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
    protected ProducerInfo getProducerInfo() {
        return this.info != null ? this.info : null;
    }

    /**
     * @param info The info to set
     */
    protected void setProducerInfo(ProducerInfo info) {
        this.info = info;
    }

    @Override
    public String toString() {
        return "ActiveMQMessageProducer { value=" + info.getProducerId() + " }";
    }

    public void onProducerAck(ProducerAck pa) {
        if (this.producerWindow != null) {
            this.producerWindow.decreaseUsage(pa.getSize());
        }
    }

}
