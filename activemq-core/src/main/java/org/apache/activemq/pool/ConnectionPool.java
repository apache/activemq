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

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.commons.pool.ObjectPoolFactory;

import javax.jms.JMSException;
import javax.jms.Session;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Holds a real JMS connection along with the session pools associated with it.
 * 
 * @version $Revision$
 */
public class ConnectionPool {
    private ActiveMQConnection connection;
    private Map cache;
    private AtomicBoolean started = new AtomicBoolean(false);
    private ObjectPoolFactory poolFactory;

    public ConnectionPool(ActiveMQConnection connection, ObjectPoolFactory poolFactory) {
        this(connection, new HashMap(), poolFactory);
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

    public ActiveMQConnection getConnection() {
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

    public void close() throws JMSException {
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
        connection.close();
        connection = null;
    }

}
