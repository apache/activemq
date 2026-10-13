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
package org.apache.activemq.jms.pool;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.jms.BytesMessage;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

/**
 * JMS 2.0 {@link JMSContext} backed by a {@link PooledConnection} and a
 * lazily-created {@link Session}. Child contexts created via
 * {@link #createContext(int)} share the same underlying connection and use
 * an {@link AtomicLong} reference counter so the connection is returned to
 * the pool only when the last context closes.
 */
public class PooledJMSContext implements JMSContext {

    private final PooledConnection connection;
    private final int sessionMode;
    private final AtomicLong connectionCounter;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean autoStart = true;
    private volatile Session session;
    private volatile MessageProducer contextProducer;
    private volatile Message lastReceivedMessage;

    PooledJMSContext(PooledConnection connection, int sessionMode) {
        this(connection, sessionMode, new AtomicLong(1));
    }

    private PooledJMSContext(PooledConnection connection, int sessionMode, AtomicLong connectionCounter) {
        this.connection = connection;
        this.sessionMode = sessionMode;
        this.connectionCounter = connectionCounter;
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        checkClosed();
        // Increment-if-positive so a concurrent last-reference close() cannot
        // release the connection while we attach a new context to it.
        long current;
        do {
            current = connectionCounter.get();
            if (current <= 0) {
                throw new IllegalStateRuntimeException("Context is closed");
            }
        } while (!connectionCounter.compareAndSet(current, current + 1));
        return new PooledJMSContext(connection, sessionMode, connectionCounter);
    }

    @Override
    public JMSProducer createProducer() {
        checkClosed();
        try {
            var s = getSession();
            var producer = contextProducer;
            if (producer == null) {
                synchronized (this) {
                    checkClosed();
                    if (contextProducer == null) {
                        contextProducer = s.createProducer(null);
                    }
                    producer = contextProducer;
                }
            }
            return new PooledJMSProducer(s, producer);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public String getClientID() {
        checkClosed();
        try {
            return connection.getClientID();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void setClientID(String clientID) {
        checkClosed();
        try {
            connection.setClientID(clientID);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public ConnectionMetaData getMetaData() {
        checkClosed();
        try {
            return connection.getMetaData();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public ExceptionListener getExceptionListener() {
        checkClosed();
        try {
            return connection.getExceptionListener();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        checkClosed();
        try {
            connection.setExceptionListener(listener);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void start() {
        checkClosed();
        try {
            connection.start();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void stop() {
        checkClosed();
        try {
            connection.stop();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        checkClosed();
        this.autoStart = autoStart;
    }

    @Override
    public boolean getAutoStart() {
        checkClosed();
        return autoStart;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        JMSRuntimeException failure = null;
        try {
            synchronized (this) {
                var producerFailure = resetContextProducer();
                if (producerFailure != null) {
                    failure = JmsPoolExceptionSupport.toRuntimeException(producerFailure);
                }
                if (session != null) {
                    try {
                        session.close();
                    } catch (JMSException e) {
                        if (failure == null) {
                            failure = JmsPoolExceptionSupport.toRuntimeException(e);
                        }
                    } finally {
                        session = null;
                    }
                }
            }
        } finally {
            lastReceivedMessage = null;
            if (connectionCounter.decrementAndGet() == 0) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    if (failure == null) {
                        failure = JmsPoolExceptionSupport.toRuntimeException(e);
                    }
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public BytesMessage createBytesMessage() {
        checkClosed();
        try {
            return getSession().createBytesMessage();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public MapMessage createMapMessage() {
        checkClosed();
        try {
            return getSession().createMapMessage();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public Message createMessage() {
        checkClosed();
        try {
            return getSession().createMessage();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public ObjectMessage createObjectMessage() {
        checkClosed();
        try {
            return getSession().createObjectMessage();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        checkClosed();
        try {
            return getSession().createObjectMessage(object);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public StreamMessage createStreamMessage() {
        checkClosed();
        try {
            return getSession().createStreamMessage();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public TextMessage createTextMessage() {
        checkClosed();
        try {
            return getSession().createTextMessage();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public TextMessage createTextMessage(String text) {
        checkClosed();
        try {
            return getSession().createTextMessage(text);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public boolean getTransacted() {
        checkClosed();
        return sessionMode == Session.SESSION_TRANSACTED;
    }

    @Override
    public int getSessionMode() {
        checkClosed();
        return sessionMode;
    }

    @Override
    public void commit() {
        checkClosed();
        try {
            getSession().commit();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void rollback() {
        checkClosed();
        try {
            getSession().rollback();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void recover() {
        checkClosed();
        try {
            getSession().recover();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return new PooledJMSConsumer(this, getSession().createConsumer(destination));
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return new PooledJMSConsumer(this, getSession().createConsumer(destination, messageSelector));
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return new PooledJMSConsumer(this, getSession().createConsumer(destination, messageSelector, noLocal));
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public Queue createQueue(String queueName) {
        checkClosed();
        try {
            return getSession().createQueue(queueName);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public Topic createTopic(String topicName) {
        checkClosed();
        try {
            return getSession().createTopic(topicName);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return new PooledJMSConsumer(this, getSession().createDurableConsumer(topic, name));
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return new PooledJMSConsumer(this, getSession().createDurableConsumer(topic, name, messageSelector, noLocal));
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return new PooledJMSConsumer(this, getSession().createSharedDurableConsumer(topic, name));
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return new PooledJMSConsumer(this, getSession().createSharedDurableConsumer(topic, name, messageSelector));
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return new PooledJMSConsumer(this, getSession().createSharedConsumer(topic, sharedSubscriptionName));
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return new PooledJMSConsumer(this, getSession().createSharedConsumer(topic, sharedSubscriptionName, messageSelector));
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return getSession().createBrowser(queue);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        checkClosed();
        try {
            if (autoStart) {
                connection.start();
            }
            return getSession().createBrowser(queue, messageSelector);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        checkClosed();
        try {
            return getSession().createTemporaryQueue();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        checkClosed();
        try {
            return getSession().createTemporaryTopic();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void unsubscribe(String name) {
        checkClosed();
        try {
            getSession().unsubscribe(name);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void acknowledge() {
        checkClosed();
        try {
            // Acknowledging any consumed message acknowledges all messages the
            // session has consumed; track the most recent one received through
            // this context's consumers. Messages delivered to MessageListeners
            // are not tracked (see the module's known-limitations doc).
            var last = lastReceivedMessage;
            if (last != null) {
                last.acknowledge();
            }
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    void onMessageReceived(Message message) {
        lastReceivedMessage = message;
    }

    private Session getSession() {
        var result = session;
        if (result == null || isStale(result)) {
            synchronized (this) {
                checkClosed();
                if (session != null && isStale(session)) {
                    // The pooled session was returned behind our back -- typically an
                    // XA transaction completed and its Synchronization closed the
                    // session. Drop the stale references and renew so the next
                    // operation enlists in the current transaction.
                    resetContextProducer();
                    session = null;
                }
                if (session == null) {
                    try {
                        var transacted = sessionMode == Session.SESSION_TRANSACTED;
                        session = connection.createSession(transacted,
                            transacted ? Session.SESSION_TRANSACTED : sessionMode);
                    } catch (JMSException e) {
                        throw JmsPoolExceptionSupport.toRuntimeException(e);
                    }
                }
                result = session;
            }
        }
        return result;
    }

    private static boolean isStale(Session session) {
        return session instanceof PooledSession && ((PooledSession) session).isClosed();
    }

    /**
     * Drops the cached producer, closing the underlying MessageProducer when it
     * was created for this context (non-anonymous mode); in anonymous mode the
     * producer is owned by the pooled session and must survive. Callers must
     * hold the context monitor; any close failure is returned for the caller
     * to handle.
     */
    private JMSException resetContextProducer() {
        var producer = contextProducer;
        contextProducer = null;
        if (producer instanceof PooledProducer && session instanceof PooledSession
            && !((PooledSession) session).isUseAnonymousProducers()) {
            try {
                ((PooledProducer) producer).getMessageProducer().close();
            } catch (JMSException e) {
                return e;
            }
        }
        return null;
    }

    private void checkClosed() {
        if (closed.get()) {
            throw new IllegalStateRuntimeException("Context is closed");
        }
    }
}
