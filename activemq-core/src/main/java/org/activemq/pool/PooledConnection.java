/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
package org.activemq.pool;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

import org.activemq.ActiveMQConnection;
import org.activemq.ActiveMQSession;
import org.activemq.AlreadyClosedException;
import org.activemq.util.JMSExceptionSupport;

/**
 * Represents a proxy {@link Connection} which is-a {@link TopicConnection} and
 * {@link QueueConnection} which is pooled and on {@link #close()} will return
 * itself to the sessionPool.
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class PooledConnection implements TopicConnection, QueueConnection {

    private ActiveMQConnection connection;
    private Map cache;
    private boolean stopped;

    public PooledConnection(ActiveMQConnection connection) {
        this(connection, new HashMap());
    }

    public PooledConnection(ActiveMQConnection connection, Map cache) {
        this.connection = connection;
        this.cache = cache;
    }

    /**
     * Factory method to create a new instance.
     */
    public PooledConnection newInstance() {
        return new PooledConnection(connection, cache);
    }

    public void close() throws JMSException {
        connection = null;
        Iterator i = cache.values().iterator();
        while (i.hasNext()) {
            SessionPool pool = (SessionPool) i.next();
            i.remove();
            try {
                pool.close();
            }
            catch (Exception e) {
                throw JMSExceptionSupport.create(e);
            }
        }
    }

    public void start() throws JMSException {
        // TODO should we start connections first before pooling them?
        getConnection().start();
    }

    public void stop() throws JMSException {
        stopped = true;
    }

    public ConnectionConsumer createConnectionConsumer(Destination destination, String selector, ServerSessionPool serverSessionPool, int maxMessages)
            throws JMSException {
        return getConnection().createConnectionConsumer(destination, selector, serverSessionPool, maxMessages);
    }

    public ConnectionConsumer createConnectionConsumer(Topic topic, String s, ServerSessionPool serverSessionPool, int maxMessages) throws JMSException {
        return getConnection().createConnectionConsumer(topic, s, serverSessionPool, maxMessages);
    }

    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String selector, String s1, ServerSessionPool serverSessionPool, int i)
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

    public ConnectionConsumer createConnectionConsumer(Queue queue, String selector, ServerSessionPool serverSessionPool, int maxMessages) throws JMSException {
        return getConnection().createConnectionConsumer(queue, selector, serverSessionPool, maxMessages);
    }

    // Session factory methods
    // -------------------------------------------------------------------------
    public QueueSession createQueueSession(boolean transacted, int ackMode) throws JMSException {
        return (QueueSession) createSession(transacted, ackMode);
    }

    public TopicSession createTopicSession(boolean transacted, int ackMode) throws JMSException {
        return (TopicSession) createSession(transacted, ackMode);
    }

    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        SessionKey key = new SessionKey(transacted, ackMode);
        SessionPool pool = (SessionPool) cache.get(key);
        if (pool == null) {
            pool = new SessionPool(getConnection(), key);
            cache.put(key, pool);
        }
        return pool.borrowSession();
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected ActiveMQConnection getConnection() throws JMSException {
        if (stopped || connection == null) {
            throw new AlreadyClosedException();
        }
        return connection;
    }

    protected ActiveMQSession createSession(SessionKey key) throws JMSException {
        return (ActiveMQSession) getConnection().createSession(key.isTransacted(), key.getAckMode());
    }

}