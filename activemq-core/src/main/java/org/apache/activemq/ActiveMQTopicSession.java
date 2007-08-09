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
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

/**
 * A TopicSession implementation that throws IllegalStateExceptions when Queue
 * operations are attempted but which delegates to another TopicSession for all
 * other operations. The ActiveMQSessions implement both Topic and Queue
 * Sessions methods but the spec states that TopicSession should throw
 * Exceptions if queue operations are attempted on it.
 * 
 * @version $Revision: 1.2 $
 */
public class ActiveMQTopicSession implements TopicSession {

    private final TopicSession next;

    public ActiveMQTopicSession(TopicSession next) {
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
        throw new IllegalStateException("Operation not supported by a TopicSession");
    }

    /**
     * @param queue
     * @param messageSelector
     * @return
     * @throws JMSException
     */
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        throw new IllegalStateException("Operation not supported by a TopicSession");
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
        if (destination instanceof Queue) {
            throw new InvalidDestinationException("Queues are not supported by a TopicSession");
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
        if (destination instanceof Queue) {
            throw new InvalidDestinationException("Queues are not supported by a TopicSession");
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
        if (destination instanceof Queue) {
            throw new InvalidDestinationException("Queues are not supported by a TopicSession");
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
        return next.createDurableSubscriber(topic, name);
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
        return next.createDurableSubscriber(topic, name, messageSelector, noLocal);
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
        if (destination instanceof Queue) {
            throw new InvalidDestinationException("Queues are not supported by a TopicSession");
        }
        return next.createProducer(destination);
    }

    /**
     * @param topic
     * @return
     * @throws JMSException
     */
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return next.createPublisher(topic);
    }

    /**
     * @param queueName
     * @return
     * @throws JMSException
     */
    public Queue createQueue(String queueName) throws JMSException {
        throw new IllegalStateException("Operation not supported by a TopicSession");
    }

    /**
     * @return
     * @throws JMSException
     */
    public StreamMessage createStreamMessage() throws JMSException {
        return next.createStreamMessage();
    }

    /**
     * @param topic
     * @return
     * @throws JMSException
     */
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return next.createSubscriber(topic);
    }

    /**
     * @param topic
     * @param messageSelector
     * @param noLocal
     * @return
     * @throws JMSException
     */
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        return next.createSubscriber(topic, messageSelector, noLocal);
    }

    /**
     * @return
     * @throws JMSException
     */
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new IllegalStateException("Operation not supported by a TopicSession");
    }

    /**
     * @return
     * @throws JMSException
     */
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        return next.createTemporaryTopic();
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
        return next.createTopic(topicName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object arg0) {
        return next.equals(arg0);
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
        next.unsubscribe(name);
    }

    public TopicSession getNext() {
        return next;
    }
}
