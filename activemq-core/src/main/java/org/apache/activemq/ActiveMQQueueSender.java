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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;

import org.apache.activemq.command.ActiveMQDestination;

/**
 * A client uses a <CODE>QueueSender</CODE> object to send messages to a
 * queue. <p/>
 * <P>
 * Normally, the <CODE>Queue</CODE> is specified when a <CODE>QueueSender
 * </CODE>
 * is created. In this case, an attempt to use the <CODE>send</CODE> methods
 * for an unidentified <CODE>QueueSender</CODE> will throw a <CODE>
 * java.lang.UnsupportedOperationException</CODE>.
 * <p/>
 * <P>
 * If the <CODE>QueueSender</CODE> is created with an unidentified <CODE>
 * Queue</CODE>,
 * an attempt to use the <CODE>send</CODE> methods that assume that the
 * <CODE>Queue</CODE> has been identified will throw a <CODE>
 * java.lang.UnsupportedOperationException</CODE>.
 * <p/>
 * <P>
 * During the execution of its <CODE>send</CODE> method, a message must not be
 * changed by other threads within the client. If the message is modified, the
 * result of the <CODE>send</CODE> is undefined. <p/>
 * <P>
 * After sending a message, a client may retain and modify it without affecting
 * the message that has been sent. The same message object may be sent multiple
 * times. <p/>
 * <P>
 * The following message headers are set as part of sending a message:
 * <code>JMSDestination</code>, <code>JMSDeliveryMode</code>,<code>JMSExpiration</code>,<code>JMSPriority</code>,
 * <code>JMSMessageID</code> and <code>JMSTimeStamp</code>. When the
 * message is sent, the values of these headers are ignored. After the
 * completion of the <CODE>send</CODE>, the headers hold the values specified
 * by the method sending the message. It is possible for the <code>send</code>
 * method not to set <code>JMSMessageID</code> and <code>JMSTimeStamp</code>
 * if the setting of these headers is explicitly disabled by the
 * <code>MessageProducer.setDisableMessageID</code> or
 * <code>MessageProducer.setDisableMessageTimestamp</code> method. <p/>
 * <P>
 * Creating a <CODE>MessageProducer</CODE> provides the same features as
 * creating a <CODE>QueueSender</CODE>. A <CODE>MessageProducer</CODE>
 * object is recommended when creating new code. The <CODE>QueueSender</CODE>
 * is provided to support existing code.
 * 
 * @see javax.jms.MessageProducer
 * @see javax.jms.QueueSession#createSender(Queue)
 */

public class ActiveMQQueueSender extends ActiveMQMessageProducer implements QueueSender {

    protected ActiveMQQueueSender(ActiveMQSession session, ActiveMQDestination destination)
        throws JMSException {
        super(session, session.getNextProducerId(), destination);
    }

    /**
     * Gets the queue associated with this <CODE>QueueSender</CODE>.
     * 
     * @return this sender's queue
     * @throws JMSException if the JMS provider fails to get the queue for this
     *                 <CODE>QueueSender</CODE> due to some internal error.
     */

    public Queue getQueue() throws JMSException {
        return (Queue)super.getDestination();
    }

    /**
     * Sends a message to a queue for an unidentified message producer. Uses the
     * <CODE>QueueSender</CODE>'s default delivery mode, priority, and time
     * to live. <p/>
     * <P>
     * Typically, a message producer is assigned a queue at creation time;
     * however, the JMS API also supports unidentified message producers, which
     * require that the queue be supplied every time a message is sent.
     * 
     * @param queue the queue to send this message to
     * @param message the message to send
     * @throws JMSException if the JMS provider fails to send the message due to
     *                 some internal error.
     * @see javax.jms.MessageProducer#getDeliveryMode()
     * @see javax.jms.MessageProducer#getTimeToLive()
     * @see javax.jms.MessageProducer#getPriority()
     */

    public void send(Queue queue, Message message) throws JMSException {
        super.send(queue, message);
    }

    /**
     * Sends a message to a queue for an unidentified message producer,
     * specifying delivery mode, priority and time to live. <p/>
     * <P>
     * Typically, a message producer is assigned a queue at creation time;
     * however, the JMS API also supports unidentified message producers, which
     * require that the queue be supplied every time a message is sent.
     * 
     * @param queue the queue to send this message to
     * @param message the message to send
     * @param deliveryMode the delivery mode to use
     * @param priority the priority for this message
     * @param timeToLive the message's lifetime (in milliseconds)
     * @throws JMSException if the JMS provider fails to send the message due to
     *                 some internal error.
     */

    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive)
        throws JMSException {
        super.send(queue, message, deliveryMode, priority, timeToLive);
    }
}
