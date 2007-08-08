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

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;

/**
 * A client uses a <CODE>QueueReceiver</CODE> object to receive messages that
 * have been delivered to a queue.
 * <p/>
 * <P>
 * Although it is possible to have multiple <CODE>QueueReceiver</CODE> s for
 * the same queue, the JMS API does not define how messages are distributed
 * between the <CODE>QueueReceiver</CODE>s.
 * <p/>
 * <P>
 * If a <CODE>QueueReceiver</CODE> specifies a message selector, the messages
 * that are not selected remain on the queue. By definition, a message selector
 * allows a <CODE>QueueReceiver</CODE> to skip messages. This means that when
 * the skipped messages are eventually read, the total ordering of the reads
 * does not retain the partial order defined by each message producer. Only
 * <CODE>QueueReceiver</CODE> s without a message selector will read messages
 * in message producer order.
 * <p/>
 * <P>
 * Creating a <CODE>MessageConsumer</CODE> provides the same features as
 * creating a <CODE>QueueReceiver</CODE>. A <CODE>MessageConsumer</CODE>
 * object is recommended for creating new code. The <CODE>QueueReceiver
 * </CODE> is provided to support existing code.
 *
 * @see javax.jms.Session#createConsumer(javax.jms.Destination, String)
 * @see javax.jms.Session#createConsumer(javax.jms.Destination)
 * @see javax.jms.QueueSession#createReceiver(Queue, String)
 * @see javax.jms.QueueSession#createReceiver(Queue)
 * @see javax.jms.MessageConsumer
 */

public class ActiveMQQueueReceiver extends ActiveMQMessageConsumer implements
        QueueReceiver {

    /**
     * @param theSession
     * @param value
     * @param destination
     * @param messageSelector
     * @param prefetch
     * @param asyncDispatch 
     * @throws JMSException
     */
    protected ActiveMQQueueReceiver(ActiveMQSession theSession,
                                    ConsumerId consumerId, ActiveMQDestination destination, String selector, int prefetch, int maximumPendingMessageCount, boolean asyncDispatch)
            throws JMSException {
        super(theSession, consumerId, destination, null, selector, prefetch, maximumPendingMessageCount, false, false, asyncDispatch);
    }

    /**
     * Gets the <CODE>Queue</CODE> associated with this queue receiver.
     *
     * @return this receiver's <CODE>Queue</CODE>
     * @throws JMSException if the JMS provider fails to get the queue for this queue
     *                      receiver due to some internal error.
     */

    public Queue getQueue() throws JMSException {
        checkClosed();
        return (Queue) super.getDestination();
    }
    
}
