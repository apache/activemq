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
import javax.jms.IllegalStateException;
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

import org.apache.activemq.ActiveMQSession;

/**
 * Acts as a pass through proxy for a JMS Session object. It intercepts events
 * that are of interest of the ActiveMQManagedConnection.
 * 
 * @version $Revision$
 */
public class ManagedSessionProxy implements Session, QueueSession, TopicSession {

    private final ActiveMQSession session;
    private boolean closed;

    public ManagedSessionProxy(ActiveMQSession session) {
        this.session = session;
    }

    public void setUseSharedTxContext(boolean enable) throws JMSException {
        if (session.getTransactionContext() != null) {
            ((ManagedTransactionContext)session.getTransactionContext()).setUseSharedTxContext(enable);
        }
    }

    /**
     * @throws JMSException
     */
    public void close() throws JMSException {
        cleanup();
    }

    /**
     * Called by the ActiveMQManagedConnection to invalidate this proxy.
     * 
     * @throws JMSException
     * @throws JMSException
     */
    public void cleanup() throws JMSException {
        closed = true;
        session.close();
    }

    /**
     * 
     */
    private Session getSession() throws JMSException {
        if (closed) {
            throw new IllegalStateException("The Session is closed");
        }
        return session;
    }

    /**
     * @throws JMSException
     */
    public void commit() throws JMSException {
        getSession().commit();
    }

    /**
     * @param queue
     * @return
     * @throws JMSException
     */
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return getSession().createBrowser(queue);
    }

    /**
     * @param queue
     * @param messageSelector
     * @return
     * @throws JMSException
     */
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        return getSession().createBrowser(queue, messageSelector);
    }

    /**
     * @return
     * @throws JMSException
     */
    public BytesMessage createBytesMessage() throws JMSException {
        return getSession().createBytesMessage();
    }

    /**
     * @param destination
     * @return
     * @throws JMSException
     */
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return getSession().createConsumer(destination);
    }

    /**
     * @param destination
     * @param messageSelector
     * @return
     * @throws JMSException
     */
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        return getSession().createConsumer(destination, messageSelector);
    }

    /**
     * @param destination
     * @param messageSelector
     * @param noLocal
     * @return
     * @throws JMSException
     */
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        return getSession().createConsumer(destination, messageSelector, noLocal);
    }

    /**
     * @param topic
     * @param name
     * @return
     * @throws JMSException
     */
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        return getSession().createDurableSubscriber(topic, name);
    }

    /**
     * @param topic
     * @param name
     * @param messageSelector
     * @param noLocal
     * @return
     * @throws JMSException
     */
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        return getSession().createDurableSubscriber(topic, name, messageSelector, noLocal);
    }

    /**
     * @return
     * @throws JMSException
     */
    public MapMessage createMapMessage() throws JMSException {
        return getSession().createMapMessage();
    }

    /**
     * @return
     * @throws JMSException
     */
    public Message createMessage() throws JMSException {
        return getSession().createMessage();
    }

    /**
     * @return
     * @throws JMSException
     */
    public ObjectMessage createObjectMessage() throws JMSException {
        return getSession().createObjectMessage();
    }

    /**
     * @param object
     * @return
     * @throws JMSException
     */
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        return getSession().createObjectMessage(object);
    }

    /**
     * @param destination
     * @return
     * @throws JMSException
     */
    public MessageProducer createProducer(Destination destination) throws JMSException {
        return getSession().createProducer(destination);
    }

    /**
     * @param queueName
     * @return
     * @throws JMSException
     */
    public Queue createQueue(String queueName) throws JMSException {
        return getSession().createQueue(queueName);
    }

    /**
     * @return
     * @throws JMSException
     */
    public StreamMessage createStreamMessage() throws JMSException {
        return getSession().createStreamMessage();
    }

    /**
     * @return
     * @throws JMSException
     */
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return getSession().createTemporaryQueue();
    }

    /**
     * @return
     * @throws JMSException
     */
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        return getSession().createTemporaryTopic();
    }

    /**
     * @return
     * @throws JMSException
     */
    public TextMessage createTextMessage() throws JMSException {
        return getSession().createTextMessage();
    }

    /**
     * @param text
     * @return
     * @throws JMSException
     */
    public TextMessage createTextMessage(String text) throws JMSException {
        return getSession().createTextMessage(text);
    }

    /**
     * @param topicName
     * @return
     * @throws JMSException
     */
    public Topic createTopic(String topicName) throws JMSException {
        return getSession().createTopic(topicName);
    }

    /**
     * @return
     * @throws JMSException
     */
    public int getAcknowledgeMode() throws JMSException {
        return getSession().getAcknowledgeMode();
    }

    /**
     * @return
     * @throws JMSException
     */
    public MessageListener getMessageListener() throws JMSException {
        return getSession().getMessageListener();
    }

    /**
     * @return
     * @throws JMSException
     */
    public boolean getTransacted() throws JMSException {
        return getSession().getTransacted();
    }

    /**
     * @throws JMSException
     */
    public void recover() throws JMSException {
        getSession().recover();
    }

    /**
     * @throws JMSException
     */
    public void rollback() throws JMSException {
        getSession().rollback();
    }

    /**
     * @param listener
     * @throws JMSException
     */
    public void setMessageListener(MessageListener listener) throws JMSException {
        getSession(); // .setMessageListener(listener);
    }

    /**
     * @param name
     * @throws JMSException
     */
    public void unsubscribe(String name) throws JMSException {
        getSession().unsubscribe(name);
    }

    /**
     * @param queue
     * @return
     * @throws JMSException
     */
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return ((QueueSession)getSession()).createReceiver(queue);
    }

    /**
     * @param queue
     * @param messageSelector
     * @return
     * @throws JMSException
     */
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        return ((QueueSession)getSession()).createReceiver(queue, messageSelector);
    }

    /**
     * @param queue
     * @return
     * @throws JMSException
     */
    public QueueSender createSender(Queue queue) throws JMSException {
        return ((QueueSession)getSession()).createSender(queue);
    }

    /**
     * @param topic
     * @return
     * @throws JMSException
     */
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return ((TopicSession)getSession()).createPublisher(topic);
    }

    /**
     * @param topic
     * @return
     * @throws JMSException
     */
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return ((TopicSession)getSession()).createSubscriber(topic);
    }

    /**
     * @param topic
     * @param messageSelector
     * @param noLocal
     * @return
     * @throws JMSException
     */
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        return ((TopicSession)getSession()).createSubscriber(topic, messageSelector, noLocal);
    }

    /**
     * @see javax.jms.Session#run()
     */
    public void run() {
        throw new RuntimeException("Operation not supported.");
    }

    public String toString() {
        return "ManagedSessionProxy { " + session + " }";
    }

}
