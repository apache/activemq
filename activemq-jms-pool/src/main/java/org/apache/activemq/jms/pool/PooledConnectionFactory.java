/*
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

import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JMS provider which pools Connection, Session and MessageProducer instances
 * so it can be used with tools like <a href="http://camel.apache.org/activemq.html">Camel</a> and Spring's
 * <a href="http://activemq.apache.org/spring-support.html">JmsTemplate and MessagListenerContainer</a>.
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
 * Optionally, one may configure the pool to examine and possibly evict objects as they sit idle in the
 * pool. This is performed by an "idle object eviction" thread, which runs asynchronously. Caution should
 * be used when configuring this optional feature. Eviction runs contend with client threads for access
 * to objects in the pool, so if they run too frequently performance issues may result. The idle object
 * eviction thread may be configured using the {@link org.apache.activemq.jms.pool.PooledConnectionFactory#setTimeBetweenExpirationCheckMillis} method.  By
 * default the value is -1 which means no eviction thread will be run.  Set to a non-negative value to
 * configure the idle eviction thread to run.
 */
public class PooledConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory {
    private static final transient Logger LOG = LoggerFactory.getLogger(PooledConnectionFactory.class);

    protected final AtomicBoolean stopped = new AtomicBoolean(false);
    private GenericKeyedObjectPool<ConnectionKey, ConnectionPool> connectionsPool;

    protected Object connectionFactory;

    private int maximumActiveSessionPerConnection = 500;
    private int idleTimeout = 30 * 1000;
    private int connectionTimeout = 30 * 1000;
    private boolean blockIfSessionPoolIsFull = true;
    private long blockIfSessionPoolIsFullTimeout = -1L;
    private long expiryTimeout = 0l;
    private boolean createConnectionOnStartup = true;
    private boolean useAnonymousProducers = true;
    private boolean reconnectOnException = true;

    // Temporary value used to always fetch the result of makeObject.
    private final AtomicReference<ConnectionPool> mostRecentlyCreated = new AtomicReference<ConnectionPool>(null);

    public void initConnectionsPool() {
        if (this.connectionsPool == null) {
            final GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
            poolConfig.setJmxEnabled(false);
            this.connectionsPool = new GenericKeyedObjectPool<ConnectionKey, ConnectionPool>(
                new KeyedPooledObjectFactory<ConnectionKey, ConnectionPool>() {
                    @Override
                    public PooledObject<ConnectionPool> makeObject(ConnectionKey connectionKey) throws Exception {
                        Connection delegate = createConnection(connectionKey);

                        ConnectionPool connection = createConnectionPool(delegate);
                        connection.setIdleTimeout(getIdleTimeout());
                        connection.setExpiryTimeout(getExpiryTimeout());
                        connection.setMaximumActiveSessionPerConnection(getMaximumActiveSessionPerConnection());
                        connection.setBlockIfSessionPoolIsFull(isBlockIfSessionPoolIsFull());
                        if (isBlockIfSessionPoolIsFull() && getBlockIfSessionPoolIsFullTimeout() > 0) {
                            connection.setBlockIfSessionPoolIsFullTimeout(getBlockIfSessionPoolIsFullTimeout());
                        }
                        connection.setUseAnonymousProducers(isUseAnonymousProducers());
                        connection.setReconnectOnException(isReconnectOnException());

                        LOG.trace("Created new connection: {}", connection);

                        PooledConnectionFactory.this.mostRecentlyCreated.set(connection);

                        return new DefaultPooledObject<ConnectionPool>(connection);
                    }

                    @Override
                    public void destroyObject(ConnectionKey connectionKey, PooledObject<ConnectionPool> pooledObject) throws Exception {
                        ConnectionPool connection = pooledObject.getObject();
                        try {
                            LOG.trace("Destroying connection: {}", connection);
                            connection.close();
                        } catch (Exception e) {
                            LOG.warn("Close connection failed for connection: " + connection + ". This exception will be ignored.",e);
                        }
                    }

                    @Override
                    public boolean validateObject(ConnectionKey connectionKey, PooledObject<ConnectionPool> pooledObject) {
                        ConnectionPool connection = pooledObject.getObject();
                        if (connection != null && connection.expiredCheck()) {
                            LOG.trace("Connection has expired: {} and will be destroyed", connection);
                            return false;
                        }

                        return true;
                    }

                    @Override
                    public void activateObject(ConnectionKey connectionKey, PooledObject<ConnectionPool> pooledObject) throws Exception {
                    }

                    @Override
                    public void passivateObject(ConnectionKey connectionKey, PooledObject<ConnectionPool> pooledObject) throws Exception {
                    }

                }, poolConfig);

            // Set max wait time to control borrow from pool.
            this.connectionsPool.setMaxWaitMillis(getConnectionTimeout());

            // Set max idle (not max active) since our connections always idle in the pool.
            this.connectionsPool.setMaxIdlePerKey(1);
            this.connectionsPool.setLifo(false);

            // We always want our validate method to control when idle objects are evicted.
            this.connectionsPool.setTestOnBorrow(true);
            this.connectionsPool.setTestWhileIdle(true);
        }
    }

    /**
     * @return the currently configured ConnectionFactory used to create the pooled Connections.
     */
    public Object getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * Sets the ConnectionFactory used to create new pooled Connections.
     * <p/>
     * Updates to this value do not affect Connections that were previously created and placed
     * into the pool.  In order to allocate new Connections based off this new ConnectionFactory
     * it is first necessary to {@link #clear} the pooled Connections.
     *
     * @param toUse
     *      The factory to use to create pooled Connections.
     */
    public void setConnectionFactory(final Object toUse) {
        if (toUse instanceof ConnectionFactory) {
            this.connectionFactory = toUse;
        } else {
            throw new IllegalArgumentException("connectionFactory should implement javax.jms.ConnectionFactory");
        }
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return (QueueConnection) createConnection();
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return (QueueConnection) createConnection(userName, password);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return (TopicConnection) createConnection();
    }

    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return (TopicConnection) createConnection(userName, password);
    }

    @Override
    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    @Override
    public synchronized Connection createConnection(String userName, String password) throws JMSException {
        if (stopped.get()) {
            LOG.debug("PooledConnectionFactory is stopped, skip create new connection.");
            return null;
        }

        ConnectionPool connection = null;
        ConnectionKey key = new ConnectionKey(userName, password);

        // This will either return an existing non-expired ConnectionPool or it
        // will create a new one to meet the demand.
        if (getConnectionsPool().getNumIdle(key) < getMaxConnections()) {
            try {
                connectionsPool.addObject(key);
                connection = mostRecentlyCreated.getAndSet(null);
                connection.incrementReferenceCount();
            } catch (Exception e) {
                throw createJmsException("Error while attempting to add new Connection to the pool", e);
            }
        } else {
            try {
                // We can race against other threads returning the connection when there is an
                // expiration or idle timeout.  We keep pulling out ConnectionPool instances until
                // we win and get a non-closed instance and then increment the reference count
                // under lock to prevent another thread from triggering an expiration check and
                // pulling the rug out from under us.
                while (connection == null) {
                    try {
                        connection = connectionsPool.borrowObject(key);
                    } catch (NoSuchElementException ex) {
                        if (!"Timeout waiting for idle object".equals(ex.getMessage())) {
                            throw ex;
                        }
                    }
                    if (connection != null) {
                        synchronized (connection) {
                            if (connection.getConnection() != null) {
                                connection.incrementReferenceCount();
                                break;
                            }

                            // Return the bad one to the pool and let if get destroyed as normal.
                            connectionsPool.returnObject(key, connection);
                            connection = null;
                        }
                    }
                }
            } catch (Exception e) {
                throw createJmsException("Error while attempting to retrieve a connection from the pool", e);
            }

            try {
                connectionsPool.returnObject(key, connection);
            } catch (Exception e) {
                throw createJmsException("Error when returning connection to the pool", e);
            }
        }

        return newPooledConnection(connection);
    }

    protected Connection newPooledConnection(ConnectionPool connection) {
        return new PooledConnection(connection);
    }

    private JMSException createJmsException(String msg, Exception cause) {
        JMSException exception = new JMSException(msg);
        exception.setLinkedException(cause);
        exception.initCause(cause);
        return exception;
    }

    protected Connection createConnection(ConnectionKey key) throws JMSException {
        if (connectionFactory instanceof ConnectionFactory) {
            if (key.getUserName() == null && key.getPassword() == null) {
                return ((ConnectionFactory) connectionFactory).createConnection();
            } else {
                return ((ConnectionFactory) connectionFactory).createConnection(key.getUserName(), key.getPassword());
            }
        } else {
            throw new IllegalStateException("connectionFactory should implement javax.jms.ConnectionFactory");
        }
    }

    public void start() {
        LOG.debug("Staring the PooledConnectionFactory: create on start = {}", isCreateConnectionOnStartup());
        stopped.set(false);
        if (isCreateConnectionOnStartup()) {
            try {
                // warm the pool by creating a connection during startup
                createConnection().close();
            } catch (JMSException e) {
                LOG.warn("Create pooled connection during start failed. This exception will be ignored.", e);
            }
        }
    }

    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            LOG.debug("Stopping the PooledConnectionFactory, number of connections in cache: {}",
                      connectionsPool != null ? connectionsPool.getNumActive() : 0);
            try {
                if (connectionsPool != null) {
                    connectionsPool.close();
                    connectionsPool = null;
                }
            } catch (Exception e) {
            }
        }
    }

    /**
     * Clears all connections from the pool.  Each connection that is currently in the pool is
     * closed and removed from the pool.  A new connection will be created on the next call to
     * {@link #createConnection}.  Care should be taken when using this method as Connections that
     * are in use be client's will be closed.
     */
    public void clear() {
        if (stopped.get()) {
            return;
        }

        getConnectionsPool().clear();
    }

    /**
     * Returns the currently configured maximum number of sessions a pooled Connection will
     * create before it either blocks or throws an exception when a new session is requested,
     * depending on configuration.
     *
     * @return the number of session instances that can be taken from a pooled connection.
     */
    public int getMaximumActiveSessionPerConnection() {
        return maximumActiveSessionPerConnection;
    }

    /**
     * Sets the maximum number of active sessions per connection
     *
     * @param maximumActiveSessionPerConnection
     *      The maximum number of active session per connection in the pool.
     */
    public void setMaximumActiveSessionPerConnection(int maximumActiveSessionPerConnection) {
        this.maximumActiveSessionPerConnection = maximumActiveSessionPerConnection;
    }

    /**
     * Controls the behavior of the internal session pool. By default the call to
     * Connection.getSession() will block if the session pool is full.  If the
     * argument false is given, it will change the default behavior and instead the
     * call to getSession() will throw a JMSException.
     *
     * The size of the session pool is controlled by the @see #maximumActive
     * property.
     *
     * @param block - if true, the call to getSession() blocks if the pool is full
     * until a session object is available.  defaults to true.
     */
    public void setBlockIfSessionPoolIsFull(boolean block) {
        this.blockIfSessionPoolIsFull = block;
    }

    /**
     * Returns whether a pooled Connection will enter a blocked state or will throw an Exception
     * once the maximum number of sessions has been borrowed from the the Session Pool.
     *
     * @return true if the pooled Connection createSession method will block when the limit is hit.
     * @see #setBlockIfSessionPoolIsFull(boolean)
     */
    public boolean isBlockIfSessionPoolIsFull() {
        return this.blockIfSessionPoolIsFull;
    }

    /**
     * Returns the maximum number to pooled Connections that this factory will allow before it
     * begins to return connections from the pool on calls to ({@link #createConnection}.
     *
     * @return the maxConnections that will be created for this pool.
     */
    public int getMaxConnections() {
        return getConnectionsPool().getMaxIdlePerKey();
    }

    /**
     * Sets the maximum number of pooled Connections (defaults to one).  Each call to
     * {@link #createConnection} will result in a new Connection being create up to the max
     * connections value.
     *
     * @param maxConnections the maxConnections to set
     */
    public void setMaxConnections(int maxConnections) {
        getConnectionsPool().setMaxIdlePerKey(maxConnections);
        getConnectionsPool().setMaxTotalPerKey(maxConnections);
    }

    /**
     * Gets the Idle timeout value applied to new Connection's that are created by this pool.
     * <p/>
     * The idle timeout is used determine if a Connection instance has sat to long in the pool unused
     * and if so is closed and removed from the pool.  The default value is 30 seconds.
     *
     * @return idle timeout value (milliseconds)
     */
    public int getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * Sets the idle timeout  value for Connection's that are created by this pool in Milliseconds,
     * defaults to 30 seconds.
     * <p/>
     * For a Connection that is in the pool but has no current users the idle timeout determines how
     * long the Connection can live before it is eligible for removal from the pool.  Normally the
     * connections are tested when an attempt to check one out occurs so a Connection instance can sit
     * in the pool much longer than its idle timeout if connections are used infrequently.
     *
     * @param idleTimeout
     *      The maximum time a pooled Connection can sit unused before it is eligible for removal.
     */
    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * Gets the connection timeout value. The maximum time waited to get a Connection from the pool.
     * The default value is 30 seconds.
     *
     * @return connection timeout value (milliseconds)
     */
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * Sets the connection timeout value for getting Connections from this pool in Milliseconds,
     * defaults to 30 seconds.
     *
     * @param connectionTimeout
     *      The maximum time to wait for getting a pooled Connection.
     */
    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
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

    /**
     * @return the configured expiration timeout for connections in the pool.
     */
    public long getExpiryTimeout() {
        return expiryTimeout;
    }

    /**
     * @return true if a Connection is created immediately on a call to {@link start}.
     */
    public boolean isCreateConnectionOnStartup() {
        return createConnectionOnStartup;
    }

    /**
     * Whether to create a connection on starting this {@link PooledConnectionFactory}.
     * <p/>
     * This can be used to warm-up the pool on startup. Notice that any kind of exception
     * happens during startup is logged at WARN level and ignored.
     *
     * @param createConnectionOnStartup <tt>true</tt> to create a connection on startup
     */
    public void setCreateConnectionOnStartup(boolean createConnectionOnStartup) {
        this.createConnectionOnStartup = createConnectionOnStartup;
    }

    /**
     * Should Sessions use one anonymous producer for all producer requests or should a new
     * MessageProducer be created for each request to create a producer object, default is true.
     *
     * When enabled the session only needs to allocate one MessageProducer for all requests and
     * the MessageProducer#send(destination, message) method can be used.  Normally this is the
     * right thing to do however it does result in the Broker not showing the producers per
     * destination.
     *
     * @return true if a PooledSession will use only a single anonymous message producer instance.
     */
    public boolean isUseAnonymousProducers() {
        return this.useAnonymousProducers;
    }

    /**
     * Sets whether a PooledSession uses only one anonymous MessageProducer instance or creates
     * a new MessageProducer for each call the create a MessageProducer.
     *
     * @param value
     *      Boolean value that configures whether anonymous producers are used.
     */
    public void setUseAnonymousProducers(boolean value) {
        this.useAnonymousProducers = value;
    }

    /**
     * Gets the Pool of ConnectionPool instances which are keyed by different ConnectionKeys.
     *
     * @return this factories pool of ConnectionPool instances.
     */
    protected GenericKeyedObjectPool<ConnectionKey, ConnectionPool> getConnectionsPool() {
        initConnectionsPool();
        return this.connectionsPool;
    }

    /**
     * Sets the number of milliseconds to sleep between runs of the idle Connection eviction thread.
     * When non-positive, no idle object eviction thread will be run, and Connections will only be
     * checked on borrow to determine if they have sat idle for too long or have failed for some
     * other reason.
     * <p/>
     * By default this value is set to -1 and no expiration thread ever runs.
     *
     * @param timeBetweenExpirationCheckMillis
     *      The time to wait between runs of the idle Connection eviction thread.
     */
    public void setTimeBetweenExpirationCheckMillis(long timeBetweenExpirationCheckMillis) {
        getConnectionsPool().setTimeBetweenEvictionRunsMillis(timeBetweenExpirationCheckMillis);
    }

    /**
     * @return the number of milliseconds to sleep between runs of the idle connection eviction thread.
     */
    public long getTimeBetweenExpirationCheckMillis() {
        return getConnectionsPool().getTimeBetweenEvictionRunsMillis();
    }

    /**
     * @return the number of Connections currently in the Pool
     */
    public int getNumConnections() {
        return getConnectionsPool().getNumIdle();
    }

    /**
     * Delegate that creates each instance of an ConnectionPool object.  Subclasses can override
     * this method to customize the type of connection pool returned.
     *
     * @param connection
     *
     * @return instance of a new ConnectionPool.
     */
    protected ConnectionPool createConnectionPool(Connection connection) {
        return new ConnectionPool(connection);
    }

    /**
     * Returns the timeout to use for blocking creating new sessions
     *
     * @return true if the pooled Connection createSession method will block when the limit is hit.
     * @see #setBlockIfSessionPoolIsFull(boolean)
     */
    public long getBlockIfSessionPoolIsFullTimeout() {
        return blockIfSessionPoolIsFullTimeout;
    }

    /**
     * Controls the behavior of the internal session pool. By default the call to
     * Connection.getSession() will block if the session pool is full.  This setting
     * will affect how long it blocks and throws an exception after the timeout.
     *
     * The size of the session pool is controlled by the @see #maximumActive
     * property.
     *
     * Whether or not the call to create session blocks is controlled by the @see #blockIfSessionPoolIsFull
     * property
     *
     * @param blockIfSessionPoolIsFullTimeout - if blockIfSessionPoolIsFullTimeout is true,
     *                                        then use this setting to configure how long to block before retry
     */
    public void setBlockIfSessionPoolIsFullTimeout(long blockIfSessionPoolIsFullTimeout) {
        this.blockIfSessionPoolIsFullTimeout = blockIfSessionPoolIsFullTimeout;
    }

    /**
     * @return true if the underlying connection will be renewed on JMSException, false otherwise
     */
    public boolean isReconnectOnException() {
        return reconnectOnException;
    }

    /**
     * Controls weather the underlying connection should be reset (and renewed) on JMSException
     *
     * @param reconnectOnException
     *          Boolean value that configures whether reconnect on exception should happen
     */
    public void setReconnectOnException(boolean reconnectOnException) {
        this.reconnectOnException = reconnectOnException;
    }

    /**
     * Called by any superclass that implements a JNDIReferencable or similar that needs to collect
     * the properties of this class for storage etc.
     *
     * This method should be updated any time there is a new property added.
     *
     * @param props
     *        a properties object that should be filled in with this objects property values.
     */
    protected void populateProperties(Properties props) {
        props.setProperty("maximumActiveSessionPerConnection", Integer.toString(getMaximumActiveSessionPerConnection()));
        props.setProperty("maxConnections", Integer.toString(getMaxConnections()));
        props.setProperty("idleTimeout", Integer.toString(getIdleTimeout()));
        props.setProperty("expiryTimeout", Long.toString(getExpiryTimeout()));
        props.setProperty("timeBetweenExpirationCheckMillis", Long.toString(getTimeBetweenExpirationCheckMillis()));
        props.setProperty("createConnectionOnStartup", Boolean.toString(isCreateConnectionOnStartup()));
        props.setProperty("useAnonymousProducers", Boolean.toString(isUseAnonymousProducers()));
        props.setProperty("blockIfSessionPoolIsFullTimeout", Long.toString(getBlockIfSessionPoolIsFullTimeout()));
        props.setProperty("reconnectOnException", Boolean.toString(isReconnectOnException()));
    }
}
