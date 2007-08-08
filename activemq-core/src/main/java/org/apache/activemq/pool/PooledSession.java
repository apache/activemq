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
package org.apache.activemq.pool;

import java.io.Serializable;
import java.util.Iterator;

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
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQQueueSender;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQTopicPublisher;
import org.apache.activemq.AlreadyClosedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @version $Revision: 1.1 $
 */
public class PooledSession implements TopicSession, QueueSession {
    private static final transient Log log = LogFactory.getLog(PooledSession.class);

    private ActiveMQSession session;
    private SessionPool sessionPool;
    private ActiveMQMessageProducer messageProducer;
    private ActiveMQQueueSender queueSender;
    private ActiveMQTopicPublisher topicPublisher;
    private boolean transactional = true;
    private boolean ignoreClose = false;
    
    private final CopyOnWriteArrayList consumers = new CopyOnWriteArrayList();
    private final CopyOnWriteArrayList browsers = new CopyOnWriteArrayList();


    public PooledSession(ActiveMQSession aSession, SessionPool sessionPool) {
        this.session = aSession;
        this.sessionPool = sessionPool;
        this.transactional = session.isTransacted();
    }

    protected boolean isIgnoreClose() {
        return ignoreClose;
    }

    protected void setIgnoreClose(boolean ignoreClose) {
        this.ignoreClose = ignoreClose;
    }

    public void close() throws JMSException {
        if (!ignoreClose) {
            // TODO a cleaner way to reset??
    
            // lets reset the session
            getSession().setMessageListener(null);
            
            // Close any consumers and browsers that may have been created.
            for (Iterator iter = consumers.iterator(); iter.hasNext();) {
                MessageConsumer consumer = (MessageConsumer) iter.next();
                consumer.close();
            }
            consumers.clear();
            
            for (Iterator iter = browsers.iterator(); iter.hasNext();) {
                QueueBrowser browser = (QueueBrowser) iter.next();
                browser.close();
            }
            browsers.clear();
    
            // maybe do a rollback?
            if (transactional) {
                try {
                    getSession().rollback();
                }
                catch (JMSException e) {
                    log.warn("Caught exception trying rollback() when putting session back into the pool: " + e, e);
    
                    // lets close the session and not put the session back into the pool
                    try {
                        session.close();
                    }
                    catch (JMSException e1) {
                        log.trace("Ignoring exception as discarding session: " + e1, e1);
                    }
                    session = null;
                    return;
                }
            }
    
            sessionPool.returnSession(this);
        }
    }

    public void commit() throws JMSException {
        getSession().commit();
    }

    public BytesMessage createBytesMessage() throws JMSException {
        return getSession().createBytesMessage();
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

    public void unsubscribe(String s) throws JMSException {
        getSession().unsubscribe(s);
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

    public boolean getTransacted() throws JMSException {
        return getSession().getTransacted();
    }

    public void recover() throws JMSException {
        getSession().recover();
    }

    public void rollback() throws JMSException {
        getSession().rollback();
    }

    public void run() {
        if (session != null) {
            session.run();
        }
    }


    // Consumer related methods
    //-------------------------------------------------------------------------
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return addQueueBrowser(getSession().createBrowser(queue));
    }

    public QueueBrowser createBrowser(Queue queue, String selector) throws JMSException {
        return addQueueBrowser(getSession().createBrowser(queue, selector));
    }

    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return addConsumer(getSession().createConsumer(destination));
    }

    public MessageConsumer createConsumer(Destination destination, String selector) throws JMSException {
        return addConsumer(getSession().createConsumer(destination, selector));
    }

    public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException {
        return addConsumer(getSession().createConsumer(destination, selector, noLocal));
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String selector) throws JMSException {
        return addTopicSubscriber(getSession().createDurableSubscriber(topic, selector));
    }


    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String selector, boolean noLocal) throws JMSException {
        return addTopicSubscriber(getSession().createDurableSubscriber(topic, name, selector, noLocal));
    }

    public MessageListener getMessageListener() throws JMSException {
        return getSession().getMessageListener();
    }

    public void setMessageListener(MessageListener messageListener) throws JMSException {
        getSession().setMessageListener(messageListener);
    }

    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return addTopicSubscriber(getSession().createSubscriber(topic));
    }

    public TopicSubscriber createSubscriber(Topic topic, String selector, boolean local) throws JMSException {
        return addTopicSubscriber(getSession().createSubscriber(topic, selector, local));
    }

    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return addQueueReceiver(getSession().createReceiver(queue));
    }

    public QueueReceiver createReceiver(Queue queue, String selector) throws JMSException {
        return addQueueReceiver(getSession().createReceiver(queue, selector));
    }


    // Producer related methods
    //-------------------------------------------------------------------------
    public MessageProducer createProducer(Destination destination) throws JMSException {
        return new PooledProducer(getMessageProducer(), destination);
    }

    public QueueSender createSender(Queue queue) throws JMSException {
        return new PooledQueueSender(getQueueSender(), queue);
    }

    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return new PooledTopicPublisher(getTopicPublisher(), topic);
    }

    // Implementation methods
    //-------------------------------------------------------------------------
    protected ActiveMQSession getSession() throws AlreadyClosedException {
        if (session == null) {
            throw new AlreadyClosedException("The session has already been closed");
        }
        return session;
    }

    public ActiveMQMessageProducer getMessageProducer() throws JMSException {
        if (messageProducer == null) {
            messageProducer = (ActiveMQMessageProducer) getSession().createProducer(null);
        }
        return messageProducer;
    }

    public ActiveMQQueueSender getQueueSender() throws JMSException {
        if (queueSender == null) {
            queueSender = (ActiveMQQueueSender) getSession().createSender(null);
        }
        return queueSender;
    }

    public ActiveMQTopicPublisher getTopicPublisher() throws JMSException {
        if (topicPublisher == null) {
            topicPublisher = (ActiveMQTopicPublisher) getSession().createPublisher(null);
        }
        return topicPublisher;
    }

    private QueueBrowser addQueueBrowser(QueueBrowser browser) {
        browsers.add(browser);
        return browser;
    }
    private MessageConsumer addConsumer(MessageConsumer consumer) {
        consumers.add(consumer);
        return consumer;
    }    
    private TopicSubscriber addTopicSubscriber(TopicSubscriber subscriber) {
        consumers.add(subscriber);
        return subscriber;
    }
    private QueueReceiver addQueueReceiver(QueueReceiver receiver) {
        consumers.add(receiver);
        return receiver;
    }

    public String toString() {
        return "PooledSession { "+session+" }";
    }
}
