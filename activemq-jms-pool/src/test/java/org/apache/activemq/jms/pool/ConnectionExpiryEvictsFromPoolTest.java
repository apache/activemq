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

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.test.TestSupport;

public class ConnectionExpiryEvictsFromPoolTest extends TestSupport {

    private BrokerService broker;
    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;

    @Override
    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistent(false);
        TransportConnector connector = broker.addConnector("tcp://localhost:0");
        broker.start();
        factory = new ActiveMQConnectionFactory("mock:" + connector.getConnectUri());
        pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);
        pooledFactory.setMaxConnections(1);
    }

    public void testEvictionOfIdle() throws Exception {
        pooledFactory.setIdleTimeout(10);
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        Connection amq1 = connection.getConnection();

        connection.close();
        // let it idle timeout
        TimeUnit.SECONDS.sleep(1);

        PooledConnection connection2 = (PooledConnection) pooledFactory.createConnection();
        Connection amq2 = connection2.getConnection();
        assertTrue("not equal", !amq1.equals(amq2));
    }

    public void testEvictionOfExpired() throws Exception {
        pooledFactory.setExpiryTimeout(10);
        Connection connection = pooledFactory.createConnection();
        Connection amq1 = ((PooledConnection) connection).getConnection();

        // let it expire while in use
        TimeUnit.SECONDS.sleep(1);
        connection.close();

        Connection connection2 = pooledFactory.createConnection();
        Connection amq2 = ((PooledConnection) connection2).getConnection();
        assertTrue("not equal", !amq1.equals(amq2));
    }

    public void testNotIdledWhenInUse() throws Exception {
        pooledFactory.setIdleTimeout(10);
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // let connection to get idle
        TimeUnit.SECONDS.sleep(1);

        // get a connection from pool again, it should be the same underlying connection
        // as before and should not be idled out since an open session exists.
        PooledConnection connection2 = (PooledConnection) pooledFactory.createConnection();
        assertSame(connection.getConnection(), connection2.getConnection());

        // now the session is closed even when it should not be
        try {
            // any operation on session first checks whether session is closed
            s.getTransacted();
        } catch (javax.jms.IllegalStateException e) {
            assertTrue("Session should be fine, instead: " + e.getMessage(), false);
        }

        Connection original = connection.getConnection();

        connection.close();
        connection2.close();

        // let connection to get idle
        TimeUnit.SECONDS.sleep(1);

        // get a connection from pool again, it should be a new Connection instance as the
        // old one should have been inactive and idled out.
        PooledConnection connection3 = (PooledConnection) pooledFactory.createConnection();
        assertNotSame(original, connection3.getConnection());
    }

    @Override
    protected void tearDown() throws Exception {
        broker.stop();
    }
}
