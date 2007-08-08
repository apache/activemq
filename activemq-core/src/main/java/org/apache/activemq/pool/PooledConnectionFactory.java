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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.transaction.TransactionManager;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.Service;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.commons.pool.ObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPoolFactory;

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
    private ConnectionFactory connectionFactory;
    private Map cache = new HashMap();
    private ObjectPoolFactory poolFactory;
    private int maximumActive = 500;
    private int maxConnections = 1;
    private TransactionManager transactionManager;

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

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    public synchronized Connection createConnection(String userName, String password) throws JMSException {
        ConnectionKey key = new ConnectionKey(userName, password);
        LinkedList pools = (LinkedList) cache.get(key);
        
        if (pools ==  null) {
            pools = new LinkedList();
            cache.put(key, pools);
        }

        ConnectionPool connection = null;
        if (pools.size() == maxConnections) {
            connection = (ConnectionPool) pools.removeFirst();
        }
        
        // Now.. we might get a connection, but it might be that we need to 
        // dump it..
        if( connection!=null && connection.expiredCheck() ) {
        	connection=null;
        }
        
        if (connection == null) {
            ActiveMQConnection delegate = createConnection(key);
            connection = createConnectionPool(delegate);
        }
        pools.add(connection);
        return new PooledConnection(connection);
    }
    
    protected ConnectionPool createConnectionPool(ActiveMQConnection connection) {
        return new ConnectionPool(connection, getPoolFactory(), transactionManager);
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

    public void stop() throws Exception{
        for(Iterator iter=cache.values().iterator();iter.hasNext();){
            LinkedList list=(LinkedList)iter.next();
            for(Iterator i=list.iterator();i.hasNext();){
                ConnectionPool connection=(ConnectionPool)i.next();
                connection.close();
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
}
