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

import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

/**
 * A {@link Session} implementation which can be used with the ActiveMQ JCA
 * Resource Adapter to publish messages using the same JMS session that is used
 * to dispatch messages.
 * 
 * @version $Revision$
 */
public class InboundSessionProxy implements Session, QueueSession, TopicSession {

    private InboundContext sessionAndProducer;

    public Session getSession() throws JMSException {
        return getSessionAndProducer().getSession();
    }

    public QueueSession getQueueSession() throws JMSException {
        Session session = getSession();
        if (session instanceof QueueSession) {
            return (QueueSession)session;
        } else {
            throw new JMSException("The underlying JMS Session does not support QueueSession semantics: " + session);
        }
    }

    public TopicSession getTopicSession() throws JMSException {
        Session session = getSession();
        if (session instanceof TopicSession) {
            return (TopicSession)session;
        } else {
            throw new JMSException("The underlying JMS Session does not support TopicSession semantics: " + session);
        }
    }

    public InboundContext getSessionAndProducer() throws JMSException {
        if (sessionAndProducer == null) {
            sessionAndProducer = InboundContextSupport.getActiveSessionAndProducer();
            if (sessionAndProducer == null) {
                throw new JMSException("No currently active Session. This JMS provider cannot be used outside a MessageListener.onMessage() invocation");
            }
        }
        return sessionAndProducer;
    }

    public MessageProducer createProducer(Destination destination) throws JMSException {
        return new InboundMessageProducerProxy(getSessionAndProducer().getMessageProducer(), destination);
    }

    public void close() throws JMSException {
        // we don't allow users to close this session
        // as its used by the JCA container
    }

    public void commit() throws JMSException {
        // the JCA container will handle transactions
    }

    public void rollback() throws JMSException {
        // the JCA container will handle transactions
    }

    public void recover() throws JMSException {
        // the JCA container will handle recovery
    }

    public void run() {
        try {
            getSession().run();
        } catch (JMSException e) {
            throw new RuntimeException("Failed to run() on session due to: " + e, e);
        }
    }

    // Straightforward delegation methods
    // -------------------------------------------------------------------------

    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return getSession().createBrowser(queue);
    }

    public QueueBrowser createBrowser(Queue queue, String s) throws JMSException {
        return getSession().createBrowser(queue, s);
    }

    public BytesMessage createBytesMessage() throws JMSException {
        return getSession().createBytesMessage();
    }

    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return getSession().createConsumer(destination);
    }

    public MessageConsumer createConsumer(Destination destination, String s) throws JMSException {
        return getSession().createConsumer(destination, s);
    }

    public MessageConsumer createConsumer(Destination destination, String s, boolean b) throws JMSException {
        return getSession().createConsumer(destination, s, b);
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String s) throws JMSException {
        return getSession().createDurableSubscriber(topic, s);
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String s, String s1, boolean b) throws JMSException {
        return getSession().createDurableSubscriber(topic, s, s1, b);
    }

    public MapMessage createMapMessage() throws JMSException {
        return getSession().createMapMessage();
    }

    public Message createMessage() throws JMSException {
        return getSession().createMessage();
    }

    public ObjectMessage createObjectMessage() throws JMSException {
        return getSession().createObjectMessage();
    }

    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException {
        return getSession().createObjectMessage(serializable);
    }

    public Queue createQueue(String s) throws JMSException {
        return getSession().createQueue(s);
    }

    public StreamMessage createStreamMessage() throws JMSException {
        return getSession().createStreamMessage();
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return getSession().createTemporaryQueue();
    }

    public TemporaryTopic createTemporaryTopic() throws JMSException {
        return getSession().createTemporaryTopic();
    }

    public TextMessage createTextMessage() throws JMSException {
        return getSession().createTextMessage();
    }

    public TextMessage createTextMessage(String s) throws JMSException {
        return getSession().createTextMessage(s);
    }

    public Topic createTopic(String s) throws JMSException {
        return getSession().createTopic(s);
    }

    public int getAcknowledgeMode() throws JMSException {
        return getSession().getAcknowledgeMode();
    }

    public MessageListener getMessageListener() throws JMSException {
        return getSession().getMessageListener();
    }

    public boolean getTransacted() throws JMSException {
        return getSession().getTransacted();
    }

    public void setMessageListener(MessageListener messageListener) throws JMSException {
        getSession().setMessageListener(messageListener);
    }

    public void unsubscribe(String s) throws JMSException {
        getSession().unsubscribe(s);
    }

    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return getQueueSession().createReceiver(queue);
    }

    public QueueReceiver createReceiver(Queue queue, String s) throws JMSException {
        return getQueueSession().createReceiver(queue, s);
    }

    public QueueSender createSender(Queue queue) throws JMSException {
        return new InboundMessageProducerProxy(getSessionAndProducer().getMessageProducer(), queue);
    }

    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return getTopicSession().createSubscriber(topic);
    }

    public TopicSubscriber createSubscriber(Topic topic, String s, boolean b) throws JMSException {
        return getTopicSession().createSubscriber(topic, s, b);
    }

    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return getTopicSession().createPublisher(topic);
    }

    public String toString() {
        try {
            return "InboundSessionProxy { " + getSession() + " }";
        } catch (JMSException e) {
            return "InboundSessionProxy { null }";
        }
    }

}
