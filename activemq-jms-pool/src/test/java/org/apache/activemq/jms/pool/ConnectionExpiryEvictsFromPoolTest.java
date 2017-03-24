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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConnectionExpiryEvictsFromPoolTest extends JmsPoolTestSupport {

    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        brokerService = new BrokerService();
        brokerService.setUseJmx(false);
        brokerService.setPersistent(false);
        brokerService.setSchedulerSupport(false);
        brokerService.setAdvisorySupport(false);
        TransportConnector connector = brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
        factory = new ActiveMQConnectionFactory("mock:" + connector.getConnectUri());
        pooledFactory = new PooledConnectionFactory();
        pooledFactory.setConnectionFactory(factory);
        pooledFactory.setMaxConnections(1);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            pooledFactory.stop();
        } catch (Exception ex) {
            // ignored
        }

        super.tearDown();
    }

    @Test(timeout = 60000)
    public void testEvictionOfIdle() throws Exception {
        pooledFactory.setIdleTimeout(10);
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        Connection amq1 = connection.getConnection();

        connection.close();

        // let it idle timeout
        TimeUnit.MILLISECONDS.sleep(500);

        PooledConnection connection2 = (PooledConnection) pooledFactory.createConnection();
        Connection amq2 = connection2.getConnection();
        assertTrue("not equal", !amq1.equals(amq2));
    }

    @Test(timeout = 60000)
    public void testEvictionOfExpired() throws Exception {
        pooledFactory.setExpiryTimeout(10);
        Connection connection = pooledFactory.createConnection();
        Connection amq1 = ((PooledConnection) connection).getConnection();

        // let it expire while in use
        TimeUnit.MILLISECONDS.sleep(500);
        connection.close();

        Connection connection2 = pooledFactory.createConnection();
        Connection amq2 = ((PooledConnection) connection2).getConnection();
        assertTrue("not equal", !amq1.equals(amq2));
    }

    @Test(timeout = 60000)
    public void testNotIdledWhenInUse() throws Exception {
        pooledFactory.setIdleTimeout(10);
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // let connection to get idle
        TimeUnit.MILLISECONDS.sleep(500);

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
        TimeUnit.MILLISECONDS.sleep(500);

        // get a connection from pool again, it should be a new Connection instance as the
        // old one should have been inactive and idled out.
        PooledConnection connection3 = (PooledConnection) pooledFactory.createConnection();
        assertNotSame(original, connection3.getConnection());
    }
}
