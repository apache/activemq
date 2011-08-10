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
package org.apache.activemq.broker;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;

/**
 *
 *
 */
public class ReconnectWithJMXEnabledTest extends EmbeddedBrokerTestSupport {

    protected Connection connection;
    protected boolean transacted;
    protected int authMode = Session.AUTO_ACKNOWLEDGE;

    public void testTestUseConnectionCloseBrokerThenRestartInSameJVM() throws Exception {
        connection = connectionFactory.createConnection();
        useConnection(connection);
        connection.close();

        broker.stop();
        broker = createBroker();
        startBroker();

        connectionFactory = createConnectionFactory();
        connection = connectionFactory.createConnection();
        useConnection(connection);
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:0";
        super.setUp();
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString());
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setPersistent(isPersistent());
        answer.addConnector(bindAddress);
        return answer;
    }

    protected void useConnection(Connection connection) throws Exception {
        connection.setClientID("foo");
        connection.start();
        Session session = connection.createSession(transacted, authMode);
        Destination destination = createDestination();
        MessageConsumer consumer = session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);
        Message message = session.createTextMessage("Hello World");
        producer.send(message);
        Thread.sleep(1000);
        consumer.close();
    }
}
