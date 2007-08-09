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
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;

/**
 * A client uses a <CODE>TopicSubscriber</CODE> object to receive messages
 * that have been published to a topic. A <CODE>TopicSubscriber</CODE> object
 * is the publish/subscribe form of a message consumer. A <CODE>
 * MessageConsumer</CODE> can be created by using <CODE>
 * Session.createConsumer</CODE>.
 * <p/>
 * <P>
 * A <CODE>TopicSession</CODE> allows the creation of multiple <CODE>
 * TopicSubscriber</CODE> objects per topic. It will deliver each message for
 * a topic to each subscriber eligible to receive it. Each copy of the message
 * is treated as a completely separate message. Work done on one copy has no
 * effect on the others; acknowledging one does not acknowledge the others; one
 * message may be delivered immediately, while another waits for its subscriber
 * to process messages ahead of it.
 * <p/>
 * <P>
 * Regular <CODE>TopicSubscriber</CODE> objects are not durable. They receive
 * only messages that are published while they are active.
 * <p/>
 * <P>
 * Messages filtered out by a subscriber's message selector will never be
 * delivered to the subscriber. From the subscriber's perspective, they do not
 * exist.
 * <p/>
 * <P>
 * In some cases, a connection may both publish and subscribe to a topic. The
 * subscriber <CODE>NoLocal</CODE> attribute allows a subscriber to inhibit
 * the delivery of messages published by its own connection.
 * <p/>
 * <P>
 * If a client needs to receive all the messages published on a topic,
 * including the ones published while the subscriber is inactive, it uses a
 * durable <CODE>TopicSubscriber</CODE>. The JMS provider retains a record
 * of this durable subscription and insures that all messages from the topic's
 * publishers are retained until they are acknowledged by this durable
 * subscriber or they have expired.
 * <p/>
 * <P>
 * Sessions with durable subscribers must always provide the same client
 * identifier. In addition, each client must specify a name that uniquely
 * identifies (within client identifier) each durable subscription it creates.
 * Only one session at a time can have a <CODE>TopicSubscriber</CODE> for a
 * particular durable subscription.
 * <p/>
 * <P>
 * A client can change an existing durable subscription by creating a durable
 * <CODE>TopicSubscriber</CODE> with the same name and a new topic and/or
 * message selector. Changing a durable subscription is equivalent to
 * unsubscribing (deleting) the old one and creating a new one.
 * <p/>
 * <P>
 * The <CODE>unsubscribe</CODE> method is used to delete a durable
 * subscription. The <CODE>unsubscribe</CODE> method can be used at the
 * <CODE>Session</CODE> or <CODE>TopicSession</CODE> level. This method
 * deletes the state being maintained on behalf of the subscriber by its
 * provider.
 * <p/>
 * <P>
 * Creating a <CODE>MessageConsumer</CODE> provides the same features as
 * creating a <CODE>TopicSubscriber</CODE>. To create a durable subscriber,
 * use of <CODE>Session.CreateDurableSubscriber</CODE> is recommended. The
 * <CODE>TopicSubscriber</CODE> is provided to support existing code.
 *
 * @see javax.jms.Session#createConsumer
 * @see javax.jms.Session#createDurableSubscriber
 * @see javax.jms.TopicSession
 * @see javax.jms.TopicSession#createSubscriber
 * @see javax.jms.TopicSubscriber
 * @see javax.jms.MessageConsumer
 */

public class ActiveMQTopicSubscriber extends ActiveMQMessageConsumer implements
        TopicSubscriber {

    /**
     * @param theSession
     * @param value 
     * @param dest
     * @param name
     * @param selector
     * @param cnum
     * @param noLocalValue
     * @param browserValue
     * @param asyncDispatch 
     * @throws JMSException
     */
    protected ActiveMQTopicSubscriber(ActiveMQSession theSession,
                                      ConsumerId consumerId, ActiveMQDestination dest, String name, String selector, int prefetch, int maximumPendingMessageCount,
                                      boolean noLocalValue, boolean browserValue, boolean asyncDispatch) throws JMSException {
        super(theSession, consumerId, dest, name, selector, prefetch, maximumPendingMessageCount, noLocalValue, browserValue, asyncDispatch);
    }

    /**
     * Gets the <CODE>Topic</CODE> associated with this subscriber.
     *
     * @return this subscriber's <CODE>Topic</CODE>
     * @throws JMSException if the JMS provider fails to get the topic for this topic
     *                      subscriber due to some internal error.
     */

    public Topic getTopic() throws JMSException {
        checkClosed();
        return (Topic) super.getDestination();
    }

    /**
     * Gets the <CODE>NoLocal</CODE> attribute for this subscriber. The
     * default value for this attribute is false.
     *
     * @return true if locally published messages are being inhibited
     * @throws JMSException if the JMS provider fails to get the <CODE>NoLocal
     *                      </CODE> attribute for this topic subscriber due to some
     *                      internal error.
     */

    public boolean getNoLocal() throws JMSException {
        checkClosed();
        return super.isNoLocal();
    }
}
