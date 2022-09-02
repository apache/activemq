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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.util.JMSExceptionSupport;

/**
 * In terms of the JMS 1.1 API a JMSContext should be thought of as 
 * representing both a Connection and a Session. Although the simplified 
 * API removes the need for applications to use those objects, the concepts 
 * of connection and session remain important. A connection represents a 
 * physical link to the JMS server and a session represents a 
 * single-threaded context for sending and receiving messages. 
 *
 *
 * @see javax.jms.JMSContext
 */

public class ActiveMQContext implements JMSContext {

    private static final boolean DEFAULT_AUTO_START = true;

    private final ActiveMQConnection activemqConnection;
    private final AtomicLong connectionCounter;
    private ActiveMQSession activemqSession = null;

    // Configuration
    private boolean autoStart = DEFAULT_AUTO_START;
    private final int sessionMode;

    // State
    private boolean closeInvoked = false;
    private final AtomicBoolean startInvoked = new AtomicBoolean(false);
    private ActiveMQMessageProducer activemqMessageProducer = null;

    ActiveMQContext(final ActiveMQConnection activemqConnection) {
        this.activemqConnection = activemqConnection;
        this.sessionMode = AUTO_ACKNOWLEDGE;
        this.connectionCounter = new AtomicLong(1l);
    }

    ActiveMQContext(final ActiveMQConnection activemqConnection, final int sessionMode) {
        this.activemqConnection = activemqConnection;
        this.sessionMode = sessionMode;
        this.connectionCounter = new AtomicLong(1l);
    }

    private ActiveMQContext(final ActiveMQConnection activemqConnection, final int sessionMode, final AtomicLong connectionCounter) {
        this.activemqConnection = activemqConnection;
        this.sessionMode = sessionMode;
        this.connectionCounter = connectionCounter;
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        if(connectionCounter.get() == 0l) {
            throw new JMSRuntimeException("Context already closed");
        }

        connectionCounter.incrementAndGet();
        return new ActiveMQContext(activemqConnection, sessionMode, connectionCounter);
    }

    @Override
    public JMSProducer createProducer() {
        return new ActiveMQProducer(this, getCreatedActiveMQMessageProducer());
    }

    @Override
    public String getClientID() {
        try {
            return this.activemqConnection.getClientID();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void setClientID(String clientID) {
        try {
            this.activemqConnection.setClientID(clientID);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public ConnectionMetaData getMetaData() {
        checkContextState();
        try {
            return this.activemqConnection.getMetaData();
        } catch (JMSException e) {
                throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public ExceptionListener getExceptionListener() {
        checkContextState();
        try {
            return this.activemqConnection.getExceptionListener();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        checkContextState();
        try {
            this.activemqConnection.setExceptionListener(listener);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void start() {
        checkContextState();
        try {
            if(startInvoked.compareAndSet(false, true)) {
                this.activemqConnection.start();
            }
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void stop() {
        checkContextState();
        try {
            if(startInvoked.compareAndSet(true, false)) {
                this.activemqConnection.stop();
            }
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void setAutoStart(boolean autoStart) {
       this.autoStart = autoStart;
    }

    @Override
    public boolean getAutoStart() {
        return this.autoStart;
    }

    @Override
    public synchronized void close() {
        JMSRuntimeException firstException = null;

        if(this.activemqMessageProducer != null) {
            try {
                this.activemqMessageProducer.close();
            } catch (JMSException e) {
                if(firstException == null) { 
                    firstException = JMSExceptionSupport.convertToJMSRuntimeException(e);
                }
            }
        }

        if(this.activemqSession != null) {
            try {
               this.activemqSession.close();
            } catch (JMSException e) {
               if(firstException == null) {
                   firstException = JMSExceptionSupport.convertToJMSRuntimeException(e);
               }
            }
        }

        if(connectionCounter.decrementAndGet() == 0) {
            if(this.activemqConnection != null) {
                try {
                    closeInvoked = true;
                    this.activemqConnection.close();
                } catch (JMSException e) {
                    if(firstException == null) {
                        firstException = JMSExceptionSupport.convertToJMSRuntimeException(e);
                    }
                }
            }
        }

        if(firstException != null) {
            throw firstException;
        }
    }

    @Override
    public BytesMessage createBytesMessage() {
        checkContextState();
        try {
            return activemqSession.createBytesMessage();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
     }

    @Override
    public MapMessage createMapMessage() {
        checkContextState();
        try {
            return activemqSession.createMapMessage();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public Message createMessage() {
        checkContextState();
        try {
            return activemqSession.createMessage();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public ObjectMessage createObjectMessage() {
        checkContextState();
        try {
            return activemqSession.createObjectMessage();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        checkContextState();
        try {
            return activemqSession.createObjectMessage(object);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public StreamMessage createStreamMessage() {
        checkContextState();
        try {
            return activemqSession.createStreamMessage();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public TextMessage createTextMessage() {
        checkContextState();
        try {
            return activemqSession.createTextMessage();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public TextMessage createTextMessage(String text) {
        checkContextState();
        try {
            return activemqSession.createTextMessage(text);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public boolean getTransacted() {
        checkContextState();
        try {
            return activemqSession.getTransacted();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public int getSessionMode() {
        return this.sessionMode;
    }

    @Override
    public void commit() {
        checkContextState();
        try {
            activemqSession.commit();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void rollback() {
        checkContextState();
        try {
            activemqSession.rollback();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void recover() {
        checkContextState();
        try {
            activemqSession.recover();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        checkContextState();
        try {
            if(getAutoStart()) {
                start();
            }
            return new ActiveMQConsumer(this, activemqSession.createConsumer(destination));
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        checkContextState();
        try {
            if(getAutoStart()) {
                start();
            }
            return new ActiveMQConsumer(this, activemqSession.createConsumer(destination, messageSelector));
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        checkContextState();
        try {
            if(getAutoStart()) {
                start();
            }
            return new ActiveMQConsumer(this, activemqSession.createConsumer(destination, messageSelector, noLocal));
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public Queue createQueue(String queueName) {
        checkContextState();
        try {
            return activemqSession.createQueue(queueName);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public Topic createTopic(String topicName) {
        checkContextState();
        try {
            return activemqSession.createTopic(topicName);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        checkContextState();
        try {
            if(getAutoStart()) {
                start();
            }
            return new ActiveMQConsumer(this, activemqSession.createDurableConsumer(topic, name));
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        checkContextState();
        try {
            if(getAutoStart()) {
                start();
            }
            return new ActiveMQConsumer(this, activemqSession.createDurableConsumer(topic, name, messageSelector, noLocal));
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        throw new UnsupportedOperationException("createSharedDurableConsumer(topic, name) is not supported");
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        throw new UnsupportedOperationException("createDurableConsumer(topic, name, messageSelector) is not supported");
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        throw new UnsupportedOperationException("createSharedConsumer(topic, sharedSubscriptionName) is not supported");
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        throw new UnsupportedOperationException("createSharedConsumer(topic, sharedSubscriptionName, messageSelector) is not supported");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        checkContextState();
        try {
            if(getAutoStart()) {
                start();
            }
            return activemqSession.createBrowser(queue);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        checkContextState();
        try {
            if(getAutoStart()) {
                start();
            }
            return activemqSession.createBrowser(queue, messageSelector);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        checkContextState();
        try {
            return activemqSession.createTemporaryQueue();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        checkContextState();
        try {
            return activemqSession.createTemporaryTopic();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void unsubscribe(String name) {
        checkContextState();
        try {
            activemqSession.unsubscribe(name);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public void acknowledge() {
        checkContextState();
        try {
            activemqSession.acknowledge();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    private void checkContextState() {
        if (activemqConnection == null) {
            throw new JMSRuntimeException("Connection not available");
        }

        if (activemqSession == null) {
            if (closeInvoked) {
                throw new IllegalStateRuntimeException("Context is closed");
            }
            try {
            	Session jmsSession = activemqConnection.createSession(SESSION_TRANSACTED == sessionMode, sessionMode);
            	activemqSession = ActiveMQSession.class.cast(jmsSession);
            } catch (JMSException e) {
                throw JMSExceptionSupport.convertToJMSRuntimeException(e);
            }
         }
    }

    private ActiveMQMessageProducer getCreatedActiveMQMessageProducer() {
        checkContextState();
        
        if (this.activemqMessageProducer == null) {
            try {
                this.activemqMessageProducer = new ActiveMQMessageProducer(activemqSession, activemqSession.getNextProducerId(), null, activemqConnection.getSendTimeout());
            } catch (JMSException e) {
                throw JMSExceptionSupport.convertToJMSRuntimeException(e);
            }
         }
        return this.activemqMessageProducer;
    }

}
