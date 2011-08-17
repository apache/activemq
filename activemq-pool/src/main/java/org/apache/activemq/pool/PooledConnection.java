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

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.AlreadyClosedException;
import org.apache.activemq.EnhancedConnection;
import org.apache.activemq.advisory.DestinationSource;

/**
 * Represents a proxy {@link Connection} which is-a {@link TopicConnection} and
 * {@link QueueConnection} which is pooled and on {@link #close()} will return
 * itself to the sessionPool.
 *
 * <b>NOTE</b> this implementation is only intended for use when sending
 * messages. It does not deal with pooling of consumers; for that look at a
 * library like <a href="http://jencks.org/">Jencks</a> such as in <a
 * href="http://jencks.org/Message+Driven+POJOs">this example</a>
 *
 *
 */
public class PooledConnection implements TopicConnection, QueueConnection, EnhancedConnection {

    private ConnectionPool pool;
    private boolean stopped;

    public PooledConnection(ConnectionPool pool) {
        this.pool = pool;
        this.pool.incrementReferenceCount();
    }

    /**
     * Factory method to create a new instance.
     */
    public PooledConnection newInstance() {
        return new PooledConnection(pool);
    }

    public void close() throws JMSException {
        if (this.pool != null) {
            this.pool.decrementReferenceCount();
            this.pool = null;
        }
    }

    public void start() throws JMSException {
        assertNotClosed();
        pool.start();
    }

    public void stop() throws JMSException {
        stopped = true;
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String selector,
                                                       ServerSessionPool serverSessionPool, int maxMessages)
        throws JMSException {
        return getConnection()
            .createConnectionConsumer(destination, selector, serverSessionPool, maxMessages);
    }

    public ConnectionConsumer createConnectionConsumer(Topic topic, String s,
                                                       ServerSessionPool serverSessionPool, int maxMessages)
        throws JMSException {
        return getConnection().createConnectionConsumer(topic, s, serverSessionPool, maxMessages);
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String selector, String s1,
                                                              ServerSessionPool serverSessionPool, int i)
        throws JMSException {
        return getConnection().createDurableConnectionConsumer(topic, selector, s1, serverSessionPool, i);
    }

    public String getClientID() throws JMSException {
        return getConnection().getClientID();
    }

    public ExceptionListener getExceptionListener() throws JMSException {
        return getConnection().getExceptionListener();
    }

    public ConnectionMetaData getMetaData() throws JMSException {
        return getConnection().getMetaData();
    }

    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException {
        getConnection().setExceptionListener(exceptionListener);
    }

    public void setClientID(String clientID) throws JMSException {
        getConnection().setClientID(clientID);
    }

    public ConnectionConsumer createConnectionConsumer(Queue queue, String selector,
                                                       ServerSessionPool serverSessionPool, int maxMessages)
        throws JMSException {
        return getConnection().createConnectionConsumer(queue, selector, serverSessionPool, maxMessages);
    }

    // Session factory methods
    // -------------------------------------------------------------------------
    public QueueSession createQueueSession(boolean transacted, int ackMode) throws JMSException {
        return (QueueSession)createSession(transacted, ackMode);
    }

    public TopicSession createTopicSession(boolean transacted, int ackMode) throws JMSException {
        return (TopicSession)createSession(transacted, ackMode);
    }

    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        return pool.createSession(transacted, ackMode);
    }

    // EnhancedCollection API
    // -------------------------------------------------------------------------

    public DestinationSource getDestinationSource() throws JMSException {
        return getConnection().getDestinationSource();
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    public ActiveMQConnection getConnection() throws JMSException {
        assertNotClosed();
        return pool.getConnection();
    }

    protected void assertNotClosed() throws AlreadyClosedException {
        if (stopped || pool == null) {
            throw new AlreadyClosedException();
        }
    }

    protected ActiveMQSession createSession(SessionKey key) throws JMSException {
        return (ActiveMQSession)getConnection().createSession(key.isTransacted(), key.getAckMode());
    }

    public String toString() {
        return "PooledConnection { " + pool + " }";
    }
}
