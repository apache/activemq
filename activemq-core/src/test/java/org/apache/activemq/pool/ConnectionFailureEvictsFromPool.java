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

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.test.TestSupport;
import org.apache.activemq.transport.mock.MockTransport;

public class ConnectionFailureEvictsFromPool extends TestSupport {

    private BrokerService broker;
    private ActiveMQConnectionFactory factory;
    private PooledConnectionFactory pooledFactory;

    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        TransportConnector connector = broker.addConnector("tcp://localhost:0");
        broker.start();
        factory = new ActiveMQConnectionFactory("mock:" + connector.getConnectUri());
        pooledFactory = new PooledConnectionFactory(factory);
    }

    public void testEviction() throws Exception {
        Connection connection = pooledFactory.createConnection();
        sendMessage(connection);
        createConnectionFailure(connection);
        try {
            sendMessage(connection);
            fail("Expected Error");
        } catch (JMSException e) {
        }

        // If we get another connection now it should be a new connection that
        // works.
        Connection connection2 = pooledFactory.createConnection();
        sendMessage(connection2);
    }

    private void createConnectionFailure(Connection connection) throws Exception {
        ActiveMQConnection c = ((PooledConnection)connection).getConnection();
        MockTransport t = (MockTransport)c.getTransportChannel().narrow(MockTransport.class);
        t.stop();
    }

    private void sendMessage(Connection connection) throws JMSException {
        Session session = connection.createSession(false, 0);
        MessageProducer producer = session.createProducer(new ActiveMQQueue("FOO"));
        producer.send(session.createTextMessage("Test"));
        session.close();
    }

    protected void tearDown() throws Exception {
        broker.stop();
    }
}
