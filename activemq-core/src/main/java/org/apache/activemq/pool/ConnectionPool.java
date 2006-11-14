/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.transport.TransportListener;
import org.apache.commons.pool.ObjectPoolFactory;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * Holds a real JMS connection along with the session pools associated with it.
 * 
 * @version $Revision$
 */
public class ConnectionPool {
	
    private ActiveMQConnection connection;
    private Map cache;
    private AtomicBoolean started = new AtomicBoolean(false);
    private int referenceCount;
    private ObjectPoolFactory poolFactory;
	private long lastUsed;
	private boolean hasFailed;
	private int idleTimeout = 30*1000;

    public ConnectionPool(ActiveMQConnection connection, ObjectPoolFactory poolFactory) {
        this(connection, new HashMap(), poolFactory);
        // Add a transport Listener so that we can notice if this connection should be expired due to 
        // a connection failure.
        connection.addTransportListener(new TransportListener(){
			public void onCommand(Object command) {
			}
			public void onException(IOException error) {
				synchronized(ConnectionPool.this) {
					hasFailed = true;
				}
			}
			public void transportInterupted() {
			}
			public void transportResumed() {
			}
		});
    }

    public ConnectionPool(ActiveMQConnection connection, Map cache, ObjectPoolFactory poolFactory) {
        this.connection = connection;
        this.cache = cache;
        this.poolFactory = poolFactory;
    }

    public void start() throws JMSException {
        if (started.compareAndSet(false, true)) {
            connection.start();
        }
    }

    synchronized public ActiveMQConnection getConnection() {
        return connection;
    }

    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        SessionKey key = new SessionKey(transacted, ackMode);
        SessionPool pool = (SessionPool) cache.get(key);
        if (pool == null) {
            pool = new SessionPool(this, key, poolFactory.createPool());
            cache.put(key, pool);
        }
        return pool.borrowSession();
    }

    synchronized public void close() {
    	if( connection!=null ) {
	        Iterator i = cache.values().iterator();
	        while (i.hasNext()) {
	            SessionPool pool = (SessionPool) i.next();
	            i.remove();
	            try {
	                pool.close();
	            } catch (Exception e) {
	            }
	        }
            try {
            	connection.close();
            } catch (Exception e) {
            }
	        connection = null;
    	}
    }

    synchronized public void incrementReferenceCount() {
		referenceCount++;
	}

	synchronized public void decrementReferenceCount() {
		referenceCount--;
		if( referenceCount == 0 ) {
			lastUsed = System.currentTimeMillis();
			expiredCheck();
		}
	}

	/**
	 * @return true if this connection has expired.
	 */
	synchronized public boolean expiredCheck() {
		if( connection == null )
			return true;
		if( hasFailed || idleTimeout> 0 && System.currentTimeMillis() > lastUsed+idleTimeout ) {
			if( referenceCount == 0 ) {
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

}
