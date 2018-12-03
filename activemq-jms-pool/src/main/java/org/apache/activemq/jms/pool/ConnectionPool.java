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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds a real JMS connection along with the session pools associated with it.
 * <p/>
 * Instances of this class are shared amongst one or more PooledConnection object and must
 * track the session objects that are loaned out for cleanup on close as well as ensuring
 * that the temporary destinations of the managed Connection are purged when all references
 * to this ConnectionPool are released.
 */
public class ConnectionPool implements ExceptionListener {
    private static final transient Logger LOG = LoggerFactory.getLogger(ConnectionPool.class);

    protected Connection connection;
    private int referenceCount;
    private long lastUsed = System.currentTimeMillis();
    private final long firstUsed = lastUsed;
    private boolean hasExpired;
    private int idleTimeout = 30 * 1000;
    private long expiryTimeout = 0l;
    private boolean useAnonymousProducers = true;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final GenericKeyedObjectPool<SessionKey, SessionHolder> sessionPool;
    private final List<PooledSession> loanedSessions = new CopyOnWriteArrayList<PooledSession>();
    private boolean reconnectOnException;
    private ExceptionListener parentExceptionListener;

    public ConnectionPool(Connection connection) {
        final GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
        poolConfig.setJmxEnabled(false);
        this.connection = wrap(connection);
        try {
            this.connection.setExceptionListener(this);
        } catch (JMSException ex) {
            LOG.warn("Could not set exception listener on create of ConnectionPool");
        }

        // Create our internal Pool of session instances.
        this.sessionPool = new GenericKeyedObjectPool<SessionKey, SessionHolder>(
            new KeyedPooledObjectFactory<SessionKey, SessionHolder>() {
                @Override
                public PooledObject<SessionHolder> makeObject(SessionKey sessionKey) throws Exception {

                    return new DefaultPooledObject<SessionHolder>(new SessionHolder(makeSession(sessionKey)));
                }

                @Override
                public void destroyObject(SessionKey sessionKey, PooledObject<SessionHolder> pooledObject) throws Exception {
                    pooledObject.getObject().close();
                }

                @Override
                public boolean validateObject(SessionKey sessionKey, PooledObject<SessionHolder> pooledObject) {
                    return true;
                }

                @Override
                public void activateObject(SessionKey sessionKey, PooledObject<SessionHolder> pooledObject) throws Exception {
                }

                @Override
                public void passivateObject(SessionKey sessionKey, PooledObject<SessionHolder> pooledObject) throws Exception {
                }
            }, poolConfig
        );
    }

    // useful when external failure needs to force expiry
    public void setHasExpired(boolean val) {
        hasExpired = val;
    }

    protected Session makeSession(SessionKey key) throws JMSException {
        return connection.createSession(key.isTransacted(), key.getAckMode());
    }

    protected Connection wrap(Connection connection) {
        return connection;
    }

    protected void unWrap(Connection connection) {
    }

    public void start() throws JMSException {
        if (started.compareAndSet(false, true)) {
            try {
                connection.start();
            } catch (JMSException e) {
                started.set(false);
                if (isReconnectOnException()) {
                    close();
                }
                throw(e);
            }
        }
    }

    public synchronized Connection getConnection() {
        return connection;
    }

    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        SessionKey key = new SessionKey(transacted, ackMode);
        PooledSession session;
        try {
            session = new PooledSession(key, sessionPool.borrowObject(key), sessionPool, key.isTransacted(), useAnonymousProducers);
            session.addSessionEventListener(new PooledSessionEventListener() {

                @Override
                public void onTemporaryTopicCreate(TemporaryTopic tempTopic) {
                }

                @Override
                public void onTemporaryQueueCreate(TemporaryQueue tempQueue) {
                }

                @Override
                public void onSessionClosed(PooledSession session) {
                    ConnectionPool.this.loanedSessions.remove(session);
                }
            });
            this.loanedSessions.add(session);
        } catch (Exception e) {
            IllegalStateException illegalStateException = new IllegalStateException(e.toString());
            illegalStateException.initCause(e);
            throw illegalStateException;
        }
        return session;
    }

    public synchronized void close() {
        if (connection != null) {
            try {
                sessionPool.close();
            } catch (Exception e) {
            } finally {
                try {
                    connection.close();
                } catch (Exception e) {
                } finally {
                    connection = null;
                }
            }
        }
    }

    public synchronized void incrementReferenceCount() {
        referenceCount++;
        lastUsed = System.currentTimeMillis();
    }

    public synchronized void decrementReferenceCount() {
        referenceCount--;
        lastUsed = System.currentTimeMillis();
        if (referenceCount == 0) {
            // Loaned sessions are those that are active in the sessionPool and
            // have not been closed by the client before closing the connection.
            // These need to be closed so that all session's reflect the fact
            // that the parent Connection is closed.
            for (PooledSession session : this.loanedSessions) {
                try {
                    session.close();
                } catch (Exception e) {
                }
            }
            this.loanedSessions.clear();

            unWrap(getConnection());

            expiredCheck();
        }
    }

    /**
     * Determines if this Connection has expired.
     * <p/>
     * A ConnectionPool is considered expired when all references to it are released AND either
     * the configured idleTimeout has elapsed OR the configured expiryTimeout has elapsed.
     * Once a ConnectionPool is determined to have expired its underlying Connection is closed.
     *
     * @return true if this connection has expired.
     */
    public synchronized boolean expiredCheck() {

        boolean expired = false;

        if (connection == null) {
            return true;
        }

        if (hasExpired) {
            if (referenceCount == 0) {
                close();
                expired = true;
            }
        }

        if (expiryTimeout > 0 && (firstUsed + expiryTimeout) - System.currentTimeMillis() < 0) {
            hasExpired = true;
            if (referenceCount == 0) {
                close();
                expired = true;
            }
        }

        // Only set hasExpired here is no references, as a Connection with references is by
        // definition not idle at this time.
        if (referenceCount == 0 && idleTimeout > 0 && (lastUsed + idleTimeout) - System.currentTimeMillis() < 0) {
            hasExpired = true;
            close();
            expired = true;
        }

        return expired;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public void setExpiryTimeout(long expiryTimeout) {
        this.expiryTimeout = expiryTimeout;
    }

    public long getExpiryTimeout() {
        return expiryTimeout;
    }

    public int getMaximumActiveSessionPerConnection() {
        return this.sessionPool.getMaxTotalPerKey();
    }

    public void setMaximumActiveSessionPerConnection(int maximumActiveSessionPerConnection) {
        this.sessionPool.setMaxTotalPerKey(maximumActiveSessionPerConnection);
    }

    public boolean isUseAnonymousProducers() {
        return this.useAnonymousProducers;
    }

    public void setUseAnonymousProducers(boolean value) {
        this.useAnonymousProducers = value;
    }

    /**
     * @return the total number of Pooled session including idle sessions that are not
     *          currently loaned out to any client.
     */
    public int getNumSessions() {
        return this.sessionPool.getNumIdle() + this.sessionPool.getNumActive();
    }

    /**
     * @return the total number of Sessions that are in the Session pool but not loaned out.
     */
    public int getNumIdleSessions() {
        return this.sessionPool.getNumIdle();
    }

    /**
     * @return the total number of Session's that have been loaned to PooledConnection instances.
     */
    public int getNumActiveSessions() {
        return this.sessionPool.getNumActive();
    }

    /**
     * Configure whether the createSession method should block when there are no more idle sessions and the
     * pool already contains the maximum number of active sessions.  If false the create method will fail
     * and throw an exception.
     *
     * @param block
     * 		Indicates whether blocking should be used to wait for more space to create a session.
     */
    public void setBlockIfSessionPoolIsFull(boolean block) {
        this.sessionPool.setBlockWhenExhausted(block);
    }

    public boolean isBlockIfSessionPoolIsFull() {
        return this.sessionPool.getBlockWhenExhausted();
    }

    /**
     * Returns the timeout to use for blocking creating new sessions
     *
     * @return true if the pooled Connection createSession method will block when the limit is hit.
     * @see #setBlockIfSessionPoolIsFull(boolean)
     */
    public long getBlockIfSessionPoolIsFullTimeout() {
        return this.sessionPool.getMaxWaitMillis();
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
        this.sessionPool.setMaxWaitMillis(blockIfSessionPoolIsFullTimeout);
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

    ExceptionListener getParentExceptionListener() {
        return parentExceptionListener;
    }

    void setParentExceptionListener(ExceptionListener parentExceptionListener) {
        this.parentExceptionListener = parentExceptionListener;
    }

    @Override
    public void onException(JMSException exception) {
        if (isReconnectOnException()) {
            close();
        }
        if (parentExceptionListener != null) {
            parentExceptionListener.onException(exception);
        }
    }

    @Override
    public String toString() {
        return "ConnectionPool[" + connection + "]";
    }
}
