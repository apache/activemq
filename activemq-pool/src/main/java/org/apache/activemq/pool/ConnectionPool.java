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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.transport.TransportListener;
import org.apache.commons.pool.ObjectPoolFactory;

/**
 * Holds a real JMS connection along with the session pools associated with it.
 * 
 * @version $Revision$
 */
public class ConnectionPool {

    private ActiveMQConnection connection;
    private Map<SessionKey, SessionPool> cache;
    private AtomicBoolean started = new AtomicBoolean(false);
    private int referenceCount;
    private ObjectPoolFactory poolFactory;
    private long lastUsed = System.currentTimeMillis();
    private long firstUsed = lastUsed;
    private boolean hasFailed;
    private boolean hasExpired;
    private int idleTimeout = 30 * 1000;
    private long expiryTimeout = 0l;

    public ConnectionPool(ActiveMQConnection connection, ObjectPoolFactory poolFactory) {
        this(connection, new HashMap<SessionKey, SessionPool>(), poolFactory);
        // Add a transport Listener so that we can notice if this connection
        // should be expired due to
        // a connection failure.
        connection.addTransportListener(new TransportListener() {
            public void onCommand(Object command) {
            }

            public void onException(IOException error) {
                synchronized (ConnectionPool.this) {
                    hasFailed = true;
                }
            }

            public void transportInterupted() {
            }

            public void transportResumed() {
            }
        });       
        //
        // make sure that we set the hasFailed flag, in case the transport already failed
        // prior to the addition of our new TransportListener
        //
        if(connection.isTransportFailed()) {
            hasFailed = true;
        }
    }

    public ConnectionPool(ActiveMQConnection connection, Map<SessionKey, SessionPool> cache, ObjectPoolFactory poolFactory) {
        this.connection = connection;
        this.cache = cache;
        this.poolFactory = poolFactory;
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

    public synchronized ActiveMQConnection getConnection() {
        return connection;
    }

    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        SessionKey key = new SessionKey(transacted, ackMode);
        SessionPool pool = cache.get(key);
        if (pool == null) {
            pool = createSessionPool(key);
            cache.put(key, pool);
        }
        PooledSession session = pool.borrowSession();
        return session;
    }

    public synchronized void close() {
        if (connection != null) {
            try {
                Iterator<SessionPool> i = cache.values().iterator();
                while (i.hasNext()) {
                    SessionPool pool = i.next();
                    i.remove();
                    try {
                        pool.close();
                    } catch (Exception e) {
                    }
                }
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
            expiredCheck();
        }
    }

    /**
     * @return true if this connection has expired.
     */
    public synchronized boolean expiredCheck() {
        if (connection == null) {
            return true;
        }
        if (hasExpired) {
            if (referenceCount == 0) {
                close();
            }
            return true;
        }
        if (hasFailed 
                || (idleTimeout > 0 && System.currentTimeMillis() > lastUsed + idleTimeout)
                || expiryTimeout > 0 && System.currentTimeMillis() > firstUsed + expiryTimeout) {
            hasExpired = true;
            if (referenceCount == 0) {
                close();
            }
            return true;
        }
        return false;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    protected SessionPool createSessionPool(SessionKey key) {
        return new SessionPool(this, key, poolFactory.createPool());
    }

    public void setExpiryTimeout(long expiryTimeout) {
        this.expiryTimeout  = expiryTimeout;
    }
    
    public long getExpiryTimeout() {
        return expiryTimeout;
    }

}
