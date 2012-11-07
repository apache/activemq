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
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.apache.activemq.command.ActiveMQDestination;

/**
 * A client uses a <CODE>TopicPublisher</CODE> object to publish messages on
 * a topic. A <CODE>TopicPublisher</CODE> object is the publish-subscribe
 * form of a message producer.
 * <p/>
 * <P>
 * Normally, the <CODE>Topic</CODE> is specified when a <CODE>TopicPublisher
 * </CODE> is created. In this case, an attempt to use the <CODE>publish
 * </CODE> methods for an unidentified <CODE>TopicPublisher</CODE> will throw
 * a <CODE>java.lang.UnsupportedOperationException</CODE>.
 * <p/>
 * <P>
 * If the <CODE>TopicPublisher</CODE> is created with an unidentified <CODE>
 * Topic</CODE>, an attempt to use the <CODE>publish</CODE> methods that
 * assume that the <CODE>Topic</CODE> has been identified will throw a <CODE>
 * java.lang.UnsupportedOperationException</CODE>.
 * <p/>
 * <P>
 * During the execution of its <CODE>publish</CODE> method, a message must
 * not be changed by other threads within the client. If the message is
 * modified, the result of the <CODE>publish</CODE> is undefined.
 * <p/>
 * <P>
 * After publishing a message, a client may retain and modify it without
 * affecting the message that has been published. The same message object may
 * be published multiple times.
 * <p/>
 * <P>
 * The following message headers are set as part of publishing a message:
 * <code>JMSDestination</code>,<code>JMSDeliveryMode</code>,<code>JMSExpiration</code>,
 * <code>JMSPriority</code>,<code>JMSMessageID</code> and <code>JMSTimeStamp</code>.
 * When the message is published, the values of these headers are ignored.
 * After completion of the <CODE>publish</CODE>, the headers hold the values
 * specified by the method publishing the message. It is possible for the
 * <CODE>publish</CODE> method not to set <code>JMSMessageID</code> and
 * <code>JMSTimeStamp</code> if the setting of these headers is explicitly
 * disabled by the <code>MessageProducer.setDisableMessageID</code> or <code>MessageProducer.setDisableMessageTimestamp</code>
 * method.
 * <p/>
 * <P>
 * Creating a <CODE>MessageProducer</CODE> provides the same features as
 * creating a <CODE>TopicPublisher</CODE>. A <CODE>MessageProducer</CODE>
 * object is recommended when creating new code. The <CODE>TopicPublisher
 * </CODE> is provided to support existing code.
 * <p/>
 * <p/>
 * <P>
 * Because <CODE>TopicPublisher</CODE> inherits from <CODE>MessageProducer
 * </CODE>, it inherits the <CODE>send</CODE> methods that are a part of the
 * <CODE>MessageProducer</CODE> interface. Using the <CODE>send</CODE>
 * methods will have the same effect as using the <CODE>publish</CODE>
 * methods: they are functionally the same.
 *
 * @see Session#createProducer(Destination)
 * @see TopicSession#createPublisher(Topic)
 */

public class ActiveMQTopicPublisher extends ActiveMQMessageProducer implements
        TopicPublisher {

    protected ActiveMQTopicPublisher(ActiveMQSession session,
                                     ActiveMQDestination destination, int sendTimeout) throws JMSException {
        super(session, session.getNextProducerId(), destination,sendTimeout);
    }

    /**
     * Gets the topic associated with this <CODE>TopicPublisher</CODE>.
     *
     * @return this publisher's topic
     * @throws JMSException if the JMS provider fails to get the topic for this
     *                      <CODE>TopicPublisher</CODE> due to some internal error.
     */

    public Topic getTopic() throws JMSException {
        return (Topic) super.getDestination();
    }

    /**
     * Publishes a message to the topic. Uses the <CODE>TopicPublisher</CODE>'s
     * default delivery mode, priority, and time to live.
     *
     * @param message the message to publish
     * @throws JMSException                if the JMS provider fails to publish the message due to
     *                                     some internal error.
     * @throws MessageFormatException      if an invalid message is specified.
     * @throws InvalidDestinationException if a client uses this method with a <CODE>TopicPublisher
     *                                     </CODE> with an invalid topic.
     * @throws java.lang.UnsupportedOperationException
     *                                     if a client uses this method with a <CODE>TopicPublisher
     *                                     </CODE> that did not specify a topic at creation time.
     * @see javax.jms.MessageProducer#getDeliveryMode()
     * @see javax.jms.MessageProducer#getTimeToLive()
     * @see javax.jms.MessageProducer#getPriority()
     */

    public void publish(Message message) throws JMSException {
        super.send(message);
    }

    /**
     * Publishes a message to the topic, specifying delivery mode, priority,
     * and time to live.
     *
     * @param message      the message to publish
     * @param deliveryMode the delivery mode to use
     * @param priority     the priority for this message
     * @param timeToLive   the message's lifetime (in milliseconds)
     * @throws JMSException                if the JMS provider fails to publish the message due to
     *                                     some internal error.
     * @throws MessageFormatException      if an invalid message is specified.
     * @throws InvalidDestinationException if a client uses this method with a <CODE>TopicPublisher
     *                                     </CODE> with an invalid topic.
     * @throws java.lang.UnsupportedOperationException
     *                                     if a client uses this method with a <CODE>TopicPublisher
     *                                     </CODE> that did not specify a topic at creation time.
     */

    public void publish(Message message, int deliveryMode, int priority,
                        long timeToLive) throws JMSException {
        super.send(message, deliveryMode, priority, timeToLive);
    }

    /**
     * Publishes a message to a topic for an unidentified message producer.
     * Uses the <CODE>TopicPublisher</CODE>'s default delivery mode,
     * priority, and time to live.
     * <p/>
     * <P>
     * Typically, a message producer is assigned a topic at creation time;
     * however, the JMS API also supports unidentified message producers, which
     * require that the topic be supplied every time a message is published.
     *
     * @param topic   the topic to publish this message to
     * @param message the message to publish
     * @throws JMSException                if the JMS provider fails to publish the message due to
     *                                     some internal error.
     * @throws MessageFormatException      if an invalid message is specified.
     * @throws InvalidDestinationException if a client uses this method with an invalid topic.
     * @see javax.jms.MessageProducer#getDeliveryMode()
     * @see javax.jms.MessageProducer#getTimeToLive()
     * @see javax.jms.MessageProducer#getPriority()
     */

    public void publish(Topic topic, Message message) throws JMSException {
        super.send(topic, message);
    }

    /**
     * Publishes a message to a topic for an unidentified message producer,
     * specifying delivery mode, priority and time to live.
     * <p/>
     * <P>
     * Typically, a message producer is assigned a topic at creation time;
     * however, the JMS API also supports unidentified message producers, which
     * require that the topic be supplied every time a message is published.
     *
     * @param topic        the topic to publish this message to
     * @param message      the message to publish
     * @param deliveryMode the delivery mode to use
     * @param priority     the priority for this message
     * @param timeToLive   the message's lifetime (in milliseconds)
     * @throws JMSException                if the JMS provider fails to publish the message due to
     *                                     some internal error.
     * @throws MessageFormatException      if an invalid message is specified.
     * @throws InvalidDestinationException if a client uses this method with an invalid topic.
     */

    public void publish(Topic topic, Message message, int deliveryMode,
                        int priority, long timeToLive) throws JMSException {
        super.send(topic, message, deliveryMode, priority, timeToLive);
    }
}
