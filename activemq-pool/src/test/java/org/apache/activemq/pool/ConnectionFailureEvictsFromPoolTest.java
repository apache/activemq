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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.EnhancedConnection;
import org.apache.activemq.advisory.DestinationSource;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.jms.pool.PooledConnection;
import org.apache.activemq.test.TestSupport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.mock.MockTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionFailureEvictsFromPoolTest extends TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionFailureEvictsFromPoolTest.class);
    private BrokerService broker;
    TransportConnector connector;

    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistent(false);
        connector = broker.addConnector("tcp://localhost:0");
        broker.start();
    }

    public void testEnhancedConnection() throws Exception {
        XaPooledConnectionFactory pooledFactory =
                new XaPooledConnectionFactory(new ActiveMQXAConnectionFactory("mock:" + connector.getConnectUri() + "?closeAsync=false"));

        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        EnhancedConnection enhancedConnection = (EnhancedConnection)connection.getConnection();
        DestinationSource destinationSource = enhancedConnection.getDestinationSource();
        assertNotNull(destinationSource);

    }

    public void testEvictionXA() throws Exception {
        XaPooledConnectionFactory pooledFactory =
                new XaPooledConnectionFactory(new ActiveMQXAConnectionFactory("mock:(" + connector.getConnectUri() + "?closeAsync=false)?jms.xaAckMode=1"));

        doTestEviction(pooledFactory);
    }

    public void testEviction() throws Exception {
        PooledConnectionFactory pooledFactory =
                new PooledConnectionFactory(new ActiveMQConnectionFactory("mock:" + connector.getConnectUri() + "?closeAsync=false"));

        doTestEviction(pooledFactory);
    }

    public void doTestEviction(ConnectionFactory pooledFactory) throws Exception {
        PooledConnection connection = (PooledConnection) pooledFactory.createConnection();
        ActiveMQConnection amqC = (ActiveMQConnection) connection.getConnection();
        final CountDownLatch gotExceptionEvent = new CountDownLatch(1);
        amqC.addTransportListener(new TransportListener() {
            public void onCommand(Object command) {
            }
            public void onException(IOException error) {
                // we know connection is dead...
                // listeners are fired async
                gotExceptionEvent.countDown();
            }
            public void transportInterupted() {
            }
            public void transportResumed() {
            }
        });

        sendMessage(connection);
        LOG.info("sent one message worked fine");
        createConnectionFailure(connection);
        try {
            sendMessage(connection);
            TestCase.fail("Expected Error");
        } catch (JMSException e) {
        } finally {
            connection.close();
        }
        TestCase.assertTrue("exception event propagated ok", gotExceptionEvent.await(5, TimeUnit.SECONDS));
        // If we get another connection now it should be a new connection that
        // works.
        LOG.info("expect new connection after failure");
        Connection connection2 = pooledFactory.createConnection();
        sendMessage(connection2);
    }

    private void createConnectionFailure(Connection connection) throws Exception {
        ActiveMQConnection c = (ActiveMQConnection) ((PooledConnection)connection).getConnection();
        MockTransport t = (MockTransport)c.getTransportChannel().narrow(MockTransport.class);
        t.onException(new IOException("forcing exception for " + getName() + " to force pool eviction"));
        LOG.info("arranged for failure, chucked exception");
    }

    private void sendMessage(Connection connection) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(new ActiveMQQueue("FOO"));
        producer.send(session.createTextMessage("Test"));
        session.close();
    }

    protected void tearDown() throws Exception {
        broker.stop();
    }
}
