/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.pool;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.Service;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.pool.ObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPool.Config;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A JMS provider which pools Connection, Session and MessageProducer instances
 * so it can be used with tools like Spring's <a
 * href="http://activemq.org/Spring+Support">JmsTemplate</a>.
 * 
 * <b>NOTE</b> this implementation is only intended for use when sending
 * messages. It does not deal with pooling of consumers; for that look at a
 * library like <a href="http://jencks.org/">Jencks</a> such as in <a
 * href="http://jencks.org/Message+Driven+POJOs">this example</a>
 * 
 * @version $Revision: 1.1 $
 */
public class PooledConnectionFactory implements ConnectionFactory, Service {
    private ActiveMQConnectionFactory connectionFactory;
    private Map cache = new HashMap();
    private ObjectPoolFactory poolFactory;
    private int maximumActive = 5000;

    public PooledConnectionFactory() {
        this(new ActiveMQConnectionFactory());
    }

    public PooledConnectionFactory(String brokerURL) {
        this(new ActiveMQConnectionFactory(brokerURL));
    }

    public PooledConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public ActiveMQConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    public synchronized Connection createConnection(String userName, String password) throws JMSException {
        ConnectionKey key = new ConnectionKey(userName, password);
        ConnectionPool connection = (ConnectionPool) cache.get(key);
        if (connection == null) {
            ActiveMQConnection delegate = createConnection(key);
            connection = new ConnectionPool(delegate, getPoolFactory());
            cache.put(key, connection);
        }
        return new PooledConnection(connection);
    }

    protected ActiveMQConnection createConnection(ConnectionKey key) throws JMSException {
        if (key.getUserName() == null && key.getPassword() == null) {
            return (ActiveMQConnection) connectionFactory.createConnection();
        }
        else {
            return (ActiveMQConnection) connectionFactory.createConnection(key.getUserName(), key.getPassword());
        }
    }

    /**
     * @see org.apache.activemq.service.Service#start()
     */
    public void start() {
        try {
            createConnection();
        }
        catch (JMSException e) {
            IOExceptionSupport.create(e);
        }
    }

    public void stop() throws Exception {
        ServiceStopper stopper = new ServiceStopper();
        for (Iterator iter = cache.values().iterator(); iter.hasNext();) {
            ConnectionPool connection = (ConnectionPool) iter.next();
            try {
                connection.close();
            }
            catch (JMSException e) {
                stopper.onException(this, e);
            }
        }
        stopper.throwFirstException();
    }

    public ObjectPoolFactory getPoolFactory() {
        if (poolFactory == null) {
            poolFactory = createPoolFactory();
        }
        return poolFactory;
    }

    /**
     * Sets the object pool factory used to create individual session pools for
     * each connection
     */
    public void setPoolFactory(ObjectPoolFactory poolFactory) {
        this.poolFactory = poolFactory;
    }

    public int getMaximumActive() {
        return maximumActive;
    }

    /**
     * Sets the maximum number of active sessions per connection
     */
    public void setMaximumActive(int maximumActive) {
        this.maximumActive = maximumActive;
    }

    protected ObjectPoolFactory createPoolFactory() {
        return new GenericObjectPoolFactory(null, maximumActive);
    }
}
