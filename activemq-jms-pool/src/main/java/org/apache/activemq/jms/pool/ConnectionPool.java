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
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
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
public class ConnectionPool {

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
    private final GenericKeyedObjectPool<SessionKey, PooledSession> sessionPool;
    private final List<PooledSession> loanedSessions = new CopyOnWriteArrayList<PooledSession>();

    public ConnectionPool(Connection connection) {

        this.connection = wrap(connection);

        // Create our internal Pool of session instances.
        this.sessionPool = new GenericKeyedObjectPool<SessionKey, PooledSession>(
            new KeyedPoolableObjectFactory<SessionKey, PooledSession>() {

                @Override
                public void activateObject(SessionKey key, PooledSession session) throws Exception {
                    ConnectionPool.this.loanedSessions.add(session);
                }

                @Override
                public void destroyObject(SessionKey key, PooledSession session) throws Exception {
                    ConnectionPool.this.loanedSessions.remove(session);
                    session.getInternalSession().close();
                }

                @Override
                public PooledSession makeObject(SessionKey key) throws Exception {
                    Session session = makeSession(key);
                    return new PooledSession(key, session, sessionPool, key.isTransacted(), useAnonymousProducers);
                }

                @Override
                public void passivateObject(SessionKey key, PooledSession session) throws Exception {
                    ConnectionPool.this.loanedSessions.remove(session);
                }

                @Override
                public boolean validateObject(SessionKey key, PooledSession session) {
                    return true;
                }
            }
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
            session = sessionPool.borrowObject(key);
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

        if (expiryTimeout > 0 && System.currentTimeMillis() > firstUsed + expiryTimeout) {
            hasExpired = true;
            if (referenceCount == 0) {
                close();
                expired = true;
            }
        }

        // Only set hasExpired here is no references, as a Connection with references is by
        // definition not idle at this time.
        if (referenceCount == 0 && idleTimeout > 0 && System.currentTimeMillis() > lastUsed + idleTimeout) {
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
        return this.sessionPool.getMaxActive();
    }

    public void setMaximumActiveSessionPerConnection(int maximumActiveSessionPerConnection) {
        this.sessionPool.setMaxActive(maximumActiveSessionPerConnection);
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
        this.sessionPool.setWhenExhaustedAction(
                (block ? GenericObjectPool.WHEN_EXHAUSTED_BLOCK : GenericObjectPool.WHEN_EXHAUSTED_FAIL));
    }

    public boolean isBlockIfSessionPoolIsFull() {
        return this.sessionPool.getWhenExhaustedAction() == GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    }

    @Override
    public String toString() {
        return "ConnectionPool[" + connection + "]";
    }
}
