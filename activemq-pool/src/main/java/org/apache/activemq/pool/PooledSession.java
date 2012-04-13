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
import java.util.concurrent.CopyOnWriteArrayList;

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
import javax.jms.XASession;
import javax.transaction.xa.XAResource;

import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.ActiveMQQueueSender;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQTopicPublisher;
import org.apache.activemq.AlreadyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PooledSession implements Session, TopicSession, QueueSession, XASession {
    private static final transient Logger LOG = LoggerFactory.getLogger(PooledSession.class);

    private ActiveMQSession session;
    private SessionPool sessionPool;
    private ActiveMQMessageProducer messageProducer;
    private ActiveMQQueueSender queueSender;
    private ActiveMQTopicPublisher topicPublisher;
    private boolean transactional = true;
    private boolean ignoreClose;

    private final CopyOnWriteArrayList<MessageConsumer> consumers = new CopyOnWriteArrayList<MessageConsumer>();
    private final CopyOnWriteArrayList<QueueBrowser> browsers = new CopyOnWriteArrayList<QueueBrowser>();
    private final CopyOnWriteArrayList<PooledSessionEventListener> tempDestEventListeners =
        new CopyOnWriteArrayList<PooledSessionEventListener>();
    private boolean isXa;

    public PooledSession(ActiveMQSession aSession, SessionPool sessionPool) {
        this.session = aSession;
        this.sessionPool = sessionPool;
        this.transactional = session.isTransacted();
    }

    public void addTempDestEventListener(PooledSessionEventListener listener) {
        this.tempDestEventListeners.add(listener);
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

            boolean invalidate = false;
            try {
                // lets reset the session
                getInternalSession().setMessageListener(null);

                // Close any consumers and browsers that may have been created.
                for (Iterator<MessageConsumer> iter = consumers.iterator(); iter.hasNext();) {
                    MessageConsumer consumer = iter.next();
                    consumer.close();
                }

                for (Iterator<QueueBrowser> iter = browsers.iterator(); iter.hasNext();) {
                    QueueBrowser browser = iter.next();
                    browser.close();
                }

                if (transactional && !isXa) {
                    try {
                        getInternalSession().rollback();
                    } catch (JMSException e) {
                        invalidate = true;
                        LOG.warn("Caught exception trying rollback() when putting session back into the pool, will invalidate. " + e, e);
                    }
                }
            } catch (JMSException ex) {
                invalidate = true;
                LOG.warn("Caught exception trying close() when putting session back into the pool, will invalidate. " + ex, ex);
            } finally {
                consumers.clear();
                browsers.clear();
            }

            if (invalidate) {
                // lets close the session and not put the session back into
                // the pool
                if (session != null) {
                    try {
                        session.close();
                    } catch (JMSException e1) {
                        LOG.trace("Ignoring exception on close as discarding session: " + e1, e1);
                    }
                    session = null;
                }
                sessionPool.invalidateSession(this);
            } else {
                sessionPool.returnSession(this);
            }
        }
    }

    public void commit() throws JMSException {
        getInternalSession().commit();
    }

    public BytesMessage createBytesMessage() throws JMSException {
        return getInternalSession().createBytesMessage();
    }

    public MapMessage createMapMessage() throws JMSException {
        return getInternalSession().createMapMessage();
    }

    public Message createMessage() throws JMSException {
        return getInternalSession().createMessage();
    }

    public ObjectMessage createObjectMessage() throws JMSException {
        return getInternalSession().createObjectMessage();
    }

    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException {
        return getInternalSession().createObjectMessage(serializable);
    }

    public Queue createQueue(String s) throws JMSException {
        return getInternalSession().createQueue(s);
    }

    public StreamMessage createStreamMessage() throws JMSException {
        return getInternalSession().createStreamMessage();
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException {
        TemporaryQueue result;

        result = getInternalSession().createTemporaryQueue();

        // Notify all of the listeners of the created temporary Queue.
        for (PooledSessionEventListener listener : this.tempDestEventListeners) {
            listener.onTemporaryQueueCreate(result);
        }

        return result;
    }

    public TemporaryTopic createTemporaryTopic() throws JMSException {
        TemporaryTopic result;

        result = getInternalSession().createTemporaryTopic();

        // Notify all of the listeners of the created temporary Topic.
        for (PooledSessionEventListener listener : this.tempDestEventListeners) {
            listener.onTemporaryTopicCreate(result);
        }

        return result;
    }

    public void unsubscribe(String s) throws JMSException {
        getInternalSession().unsubscribe(s);
    }

    public TextMessage createTextMessage() throws JMSException {
        return getInternalSession().createTextMessage();
    }

    public TextMessage createTextMessage(String s) throws JMSException {
        return getInternalSession().createTextMessage(s);
    }

    public Topic createTopic(String s) throws JMSException {
        return getInternalSession().createTopic(s);
    }

    public int getAcknowledgeMode() throws JMSException {
        return getInternalSession().getAcknowledgeMode();
    }

    public boolean getTransacted() throws JMSException {
        return getInternalSession().getTransacted();
    }

    public void recover() throws JMSException {
        getInternalSession().recover();
    }

    public void rollback() throws JMSException {
        getInternalSession().rollback();
    }

    public XAResource getXAResource() {
        if (session == null) {
            throw new IllegalStateException("Session is closed");
        }
        return session.getTransactionContext();
    }

    public Session getSession() {
        return this;
    }

    public void run() {
        if (session != null) {
            session.run();
        }
    }

    // Consumer related methods
    // -------------------------------------------------------------------------
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return addQueueBrowser(getInternalSession().createBrowser(queue));
    }

    public QueueBrowser createBrowser(Queue queue, String selector) throws JMSException {
        return addQueueBrowser(getInternalSession().createBrowser(queue, selector));
    }

    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return addConsumer(getInternalSession().createConsumer(destination));
    }

    public MessageConsumer createConsumer(Destination destination, String selector) throws JMSException {
        return addConsumer(getInternalSession().createConsumer(destination, selector));
    }

    public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException {
        return addConsumer(getInternalSession().createConsumer(destination, selector, noLocal));
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String selector) throws JMSException {
        return addTopicSubscriber(getInternalSession().createDurableSubscriber(topic, selector));
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String selector, boolean noLocal) throws JMSException {
        return addTopicSubscriber(getInternalSession().createDurableSubscriber(topic, name, selector, noLocal));
    }

    public MessageListener getMessageListener() throws JMSException {
        return getInternalSession().getMessageListener();
    }

    public void setMessageListener(MessageListener messageListener) throws JMSException {
        getInternalSession().setMessageListener(messageListener);
    }

    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return addTopicSubscriber(getInternalSession().createSubscriber(topic));
    }

    public TopicSubscriber createSubscriber(Topic topic, String selector, boolean local) throws JMSException {
        return addTopicSubscriber(getInternalSession().createSubscriber(topic, selector, local));
    }

    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return addQueueReceiver(getInternalSession().createReceiver(queue));
    }

    public QueueReceiver createReceiver(Queue queue, String selector) throws JMSException {
        return addQueueReceiver(getInternalSession().createReceiver(queue, selector));
    }

    // Producer related methods
    // -------------------------------------------------------------------------
    public MessageProducer createProducer(Destination destination) throws JMSException {
        return new PooledProducer(getMessageProducer(), destination);
    }

    public QueueSender createSender(Queue queue) throws JMSException {
        return new PooledQueueSender(getQueueSender(), queue);
    }

    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return new PooledTopicPublisher(getTopicPublisher(), topic);
    }

    /**
     * Callback invoked when the consumer is closed.
     * <p/>
     * This is used to keep track of an explicit closed consumer created by this
     * session, by which we know do not need to keep track of the consumer, as
     * its already closed.
     *
     * @param consumer
     *            the consumer which is being closed
     */
    protected void onConsumerClose(MessageConsumer consumer) {
        consumers.remove(consumer);
    }

    public ActiveMQSession getInternalSession() throws AlreadyClosedException {
        if (session == null) {
            throw new AlreadyClosedException("The session has already been closed");
        }
        return session;
    }

    public ActiveMQMessageProducer getMessageProducer() throws JMSException {
        if (messageProducer == null) {
            messageProducer = (ActiveMQMessageProducer) getInternalSession().createProducer(null);
        }
        return messageProducer;
    }

    public ActiveMQQueueSender getQueueSender() throws JMSException {
        if (queueSender == null) {
            queueSender = (ActiveMQQueueSender) getInternalSession().createSender(null);
        }
        return queueSender;
    }

    public ActiveMQTopicPublisher getTopicPublisher() throws JMSException {
        if (topicPublisher == null) {
            topicPublisher = (ActiveMQTopicPublisher) getInternalSession().createPublisher(null);
        }
        return topicPublisher;
    }

    private QueueBrowser addQueueBrowser(QueueBrowser browser) {
        browsers.add(browser);
        return browser;
    }

    private MessageConsumer addConsumer(MessageConsumer consumer) {
        consumers.add(consumer);
        // must wrap in PooledMessageConsumer to ensure the onConsumerClose
        // method is invoked
        // when the returned consumer is closed, to avoid memory leak in this
        // session class
        // in case many consumers is created
        return new PooledMessageConsumer(this, consumer);
    }

    private TopicSubscriber addTopicSubscriber(TopicSubscriber subscriber) {
        consumers.add(subscriber);
        return subscriber;
    }

    private QueueReceiver addQueueReceiver(QueueReceiver receiver) {
        consumers.add(receiver);
        return receiver;
    }

    public void setIsXa(boolean isXa) {
        this.isXa = isXa;
    }

    public String toString() {
        return "PooledSession { " + session + " }";
    }
}
