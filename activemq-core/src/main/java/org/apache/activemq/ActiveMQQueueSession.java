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

import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
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
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

/**
 * A QueueSession implementation that throws IllegalStateExceptions when Topic
 * operations are attempted but which delegates to another QueueSession for all
 * other operations. The ActiveMQSessions implement both Topic and Queue
 * Sessions methods but the spec states that Queue session should throw
 * Exceptions if topic operations are attempted on it.
 *
 *
 */
public class ActiveMQQueueSession implements QueueSession {

    private final QueueSession next;

    public ActiveMQQueueSession(QueueSession next) {
        this.next = next;
    }

    /**
     * @throws JMSException
     */
    public void close() throws JMSException {
        next.close();
    }

    /**
     * @throws JMSException
     */
    public void commit() throws JMSException {
        next.commit();
    }

    /**
     * @param queue
     * @return
     * @throws JMSException
     */
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return next.createBrowser(queue);
    }

    /**
     * @param queue
     * @param messageSelector
     * @return
     * @throws JMSException
     */
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        return next.createBrowser(queue, messageSelector);
    }

    /**
     * @return
     * @throws JMSException
     */
    public BytesMessage createBytesMessage() throws JMSException {
        return next.createBytesMessage();
    }

    /**
     * @param destination
     * @return
     * @throws JMSException
     */
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        if (destination instanceof Topic) {
            throw new InvalidDestinationException("Topics are not supported by a QueueSession");
        }
        return next.createConsumer(destination);
    }

    /**
     * @param destination
     * @param messageSelector
     * @return
     * @throws JMSException
     */
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        if (destination instanceof Topic) {
            throw new InvalidDestinationException("Topics are not supported by a QueueSession");
        }
        return next.createConsumer(destination, messageSelector);
    }

    /**
     * @param destination
     * @param messageSelector
     * @param noLocal
     * @return
     * @throws JMSException
     */
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        if (destination instanceof Topic) {
            throw new InvalidDestinationException("Topics are not supported by a QueueSession");
        }
        return next.createConsumer(destination, messageSelector, noLocal);
    }

    /**
     * @param topic
     * @param name
     * @return
     * @throws JMSException
     */
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
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
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @return
     * @throws JMSException
     */
    public MapMessage createMapMessage() throws JMSException {
        return next.createMapMessage();
    }

    /**
     * @return
     * @throws JMSException
     */
    public Message createMessage() throws JMSException {
        return next.createMessage();
    }

    /**
     * @return
     * @throws JMSException
     */
    public ObjectMessage createObjectMessage() throws JMSException {
        return next.createObjectMessage();
    }

    /**
     * @param object
     * @return
     * @throws JMSException
     */
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        return next.createObjectMessage(object);
    }

    /**
     * @param destination
     * @return
     * @throws JMSException
     */
    public MessageProducer createProducer(Destination destination) throws JMSException {
        if (destination instanceof Topic) {
            throw new InvalidDestinationException("Topics are not supported by a QueueSession");
        }
        return next.createProducer(destination);
    }

    /**
     * @param queueName
     * @return
     * @throws JMSException
     */
    public Queue createQueue(String queueName) throws JMSException {
        return next.createQueue(queueName);
    }

    /**
     * @param queue
     * @return
     * @throws JMSException
     */
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return next.createReceiver(queue);
    }

    /**
     * @param queue
     * @param messageSelector
     * @return
     * @throws JMSException
     */
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        return next.createReceiver(queue, messageSelector);
    }

    /**
     * @param queue
     * @return
     * @throws JMSException
     */
    public QueueSender createSender(Queue queue) throws JMSException {
        return next.createSender(queue);
    }

    /**
     * @return
     * @throws JMSException
     */
    public StreamMessage createStreamMessage() throws JMSException {
        return next.createStreamMessage();
    }

    /**
     * @return
     * @throws JMSException
     */
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return next.createTemporaryQueue();
    }

    /**
     * @return
     * @throws JMSException
     */
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /**
     * @return
     * @throws JMSException
     */
    public TextMessage createTextMessage() throws JMSException {
        return next.createTextMessage();
    }

    /**
     * @param text
     * @return
     * @throws JMSException
     */
    public TextMessage createTextMessage(String text) throws JMSException {
        return next.createTextMessage(text);
    }

    /**
     * @param topicName
     * @return
     * @throws JMSException
     */
    public Topic createTopic(String topicName) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object arg0) {
        if(this != arg0) {
            return next.equals(arg0);
        }

        return true;
    }

    /**
     * @return
     * @throws JMSException
     */
    public int getAcknowledgeMode() throws JMSException {
        return next.getAcknowledgeMode();
    }

    /**
     * @return
     * @throws JMSException
     */
    public MessageListener getMessageListener() throws JMSException {
        return next.getMessageListener();
    }

    /**
     * @return
     * @throws JMSException
     */
    public boolean getTransacted() throws JMSException {
        return next.getTransacted();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return next.hashCode();
    }

    /**
     * @throws JMSException
     */
    public void recover() throws JMSException {
        next.recover();
    }

    /**
     * @throws JMSException
     */
    public void rollback() throws JMSException {
        next.rollback();
    }

    /**
     *
     */
    public void run() {
        next.run();
    }

    /**
     * @param listener
     * @throws JMSException
     */
    public void setMessageListener(MessageListener listener) throws JMSException {
        next.setMessageListener(listener);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return next.toString();
    }

    /**
     * @param name
     * @throws JMSException
     */
    public void unsubscribe(String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    public QueueSession getNext() {
        return next;
    }

}
