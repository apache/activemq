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
 * Sessions methods but the specification states that Queue session should throw
 * Exceptions if topic operations are attempted on it.
 */
public class ActiveMQQueueSession implements QueueSession {

    private final QueueSession next;

    public ActiveMQQueueSession(QueueSession next) {
        this.next = next;
    }

    @Override
    public void close() throws JMSException {
        next.close();
    }

    @Override
    public void commit() throws JMSException {
        next.commit();
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return next.createBrowser(queue);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        return next.createBrowser(queue, messageSelector);
    }

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        return next.createBytesMessage();
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        if (destination instanceof Topic) {
            throw new InvalidDestinationException("Topics are not supported by a QueueSession");
        }
        return next.createConsumer(destination);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        if (destination instanceof Topic) {
            throw new InvalidDestinationException("Topics are not supported by a QueueSession");
        }
        return next.createConsumer(destination, messageSelector);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        if (destination instanceof Topic) {
            throw new InvalidDestinationException("Topics are not supported by a QueueSession");
        }
        return next.createConsumer(destination, messageSelector, noLocal);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        return next.createMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        return next.createMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        return next.createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        return next.createObjectMessage(object);
    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        if (destination instanceof Topic) {
            throw new InvalidDestinationException("Topics are not supported by a QueueSession");
        }
        return next.createProducer(destination);
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        return next.createQueue(queueName);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return next.createReceiver(queue);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        return next.createReceiver(queue, messageSelector);
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        return next.createSender(queue);
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        return next.createStreamMessage();
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return next.createTemporaryQueue();
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        return next.createTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        return next.createTextMessage(text);
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    @Override
    public boolean equals(Object arg0) {
        if(this != arg0) {
            return next.equals(arg0);
        }

        return true;
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        return next.getAcknowledgeMode();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return next.getMessageListener();
    }

    @Override
    public boolean getTransacted() throws JMSException {
        return next.getTransacted();
    }

    @Override
    public int hashCode() {
        return next.hashCode();
    }

    @Override
    public void recover() throws JMSException {
        next.recover();
    }

    @Override
    public void rollback() throws JMSException {
        next.rollback();
    }

    @Override
    public void run() {
        next.run();
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        next.setMessageListener(listener);
    }

    @Override
    public String toString() {
        return next.toString();
    }

    @Override
    public void unsubscribe(String name) throws JMSException {
        throw new IllegalStateException("Operation not supported by a QueueSession");
    }

    public QueueSession getNext() {
        return next;
    }
}
