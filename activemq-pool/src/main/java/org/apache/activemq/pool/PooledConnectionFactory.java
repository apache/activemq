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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.Service;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.ObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPoolFactory;

/**
 * A JMS provider which pools Connection, Session and MessageProducer instances
 * so it can be used with tools like <a href="http://camel.apache.org/activemq.html">Camel</a> and Spring's <a
 * href="http://activemq.apache.org/spring-support.html">JmsTemplate and MessagListenerContainer</a>.
 * Connections, sessions and producers are returned to a pool after use so that they can be reused later
 * without having to undergo the cost of creating them again.
 * 
 * b>NOTE:</b> while this implementation does allow the creation of a collection of active consumers,
 * it does not 'pool' consumers. Pooling makes sense for connections, sessions and producers, which 
 * are expensive to create and can remain idle a minimal cost. Consumers, on the other hand, are usually
 * just created at startup and left active, handling incoming messages as they come. When a consumer is
 * complete, it is best to close it rather than return it to a pool for later reuse: this is because, 
 * even if a consumer is idle, ActiveMQ will keep delivering messages to the consumer's prefetch buffer,
 * where they'll get held until the consumer is active again.
 * 
 * If you are creating a collection of consumers (for example, for multi-threaded message consumption), you
 * might want to consider using a lower prefetch value for each consumer (e.g. 10 or 20), to ensure that 
 * all messages don't end up going to just one of the consumers. See this FAQ entry for more detail: 
 * http://activemq.apache.org/i-do-not-receive-messages-in-my-second-consumer.html
 * 
 * @org.apache.xbean.XBean element="pooledConnectionFactory"
 * 
 * @version $Revision: 1.1 $
 */
public class PooledConnectionFactory implements ConnectionFactory, Service {
    private static final transient Log LOG = LogFactory.getLog(PooledConnectionFactory.class);
    private ConnectionFactory connectionFactory;
    private Map<ConnectionKey, LinkedList<ConnectionPool>> cache = new HashMap<ConnectionKey, LinkedList<ConnectionPool>>();
    private ObjectPoolFactory poolFactory;
    private int maximumActive = 500;
    private int maxConnections = 1;
    private int idleTimeout = 30 * 1000;
    private AtomicBoolean stopped = new AtomicBoolean(false);
    private long expiryTimeout = 0l;

    public PooledConnectionFactory() {
        this(new ActiveMQConnectionFactory());
    }

    public PooledConnectionFactory(String brokerURL) {
        this(new ActiveMQConnectionFactory(brokerURL));
    }

    public PooledConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    public synchronized Connection createConnection(String userName, String password) throws JMSException {
        if (stopped.get()) {
            LOG.debug("PooledConnectionFactory is stopped, skip create new connection.");
            return null;
        }
        
        ConnectionKey key = new ConnectionKey(userName, password);
        LinkedList<ConnectionPool> pools = cache.get(key);

        if (pools == null) {
            pools = new LinkedList<ConnectionPool>();
            cache.put(key, pools);
        }

        ConnectionPool connection = null;
        if (pools.size() == maxConnections) {
            connection = pools.removeFirst();
        }

        // Now.. we might get a connection, but it might be that we need to
        // dump it..
        if (connection != null && connection.expiredCheck()) {
            connection = null;
        }

        if (connection == null) {
            ActiveMQConnection delegate = createConnection(key);
            connection = createConnectionPool(delegate);
        }
        pools.add(connection);
        return new PooledConnection(connection);
    }

    protected ConnectionPool createConnectionPool(ActiveMQConnection connection) {
        ConnectionPool result =  new ConnectionPool(connection, getPoolFactory());
        result.setIdleTimeout(getIdleTimeout());
        result.setExpiryTimeout(getExpiryTimeout());
        return result;
    }

    protected ActiveMQConnection createConnection(ConnectionKey key) throws JMSException {
        if (key.getUserName() == null && key.getPassword() == null) {
            return (ActiveMQConnection)connectionFactory.createConnection();
        } else {
            return (ActiveMQConnection)connectionFactory.createConnection(key.getUserName(), key.getPassword());
        }
    }

    /**
     * @see org.apache.activemq.service.Service#start()
     */
    public void start() {
        try {
            stopped.set(false);
            createConnection();
        } catch (JMSException e) {
            LOG.warn("Create pooled connection during start failed.", e);
            IOExceptionSupport.create(e);
        }
    }

    public void stop() {
        LOG.debug("Stop the PooledConnectionFactory, number of connections in cache: "+cache.size());
        stopped.set(true);
        for (Iterator<LinkedList<ConnectionPool>> iter = cache.values().iterator(); iter.hasNext();) {
            for (ConnectionPool connection : iter.next()) {
                try {
                    connection.close();
                }catch(Exception e) {
                    LOG.warn("Close connection failed",e);
                }
            }
        }
        cache.clear();
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

    /**
     * @return the maxConnections
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * @param maxConnections the maxConnections to set
     */
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    protected ObjectPoolFactory createPoolFactory() {
        return new GenericObjectPoolFactory(null, maximumActive);
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * allow connections to expire, irrespective of load or idle time. This is useful with failover
     * to force a reconnect from the pool, to reestablish load balancing or use of the master post recovery
     * 
     * @param expiryTimeout non zero in milliseconds
     */
    public void setExpiryTimeout(long expiryTimeout) {
        this.expiryTimeout = expiryTimeout;   
    }
    
    public long getExpiryTimeout() {
        return expiryTimeout;
    }
}
