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
package org.apache.activemq.bugs;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ5921Test {

    private static Logger LOG = LoggerFactory.getLogger(AMQ5921Test.class);

    private ActiveMQConnection connection;
    private BrokerService broker;
    private String connectionUri;

    @Rule
    public TestName name = new TestName();

    @Test
    public void testVoidSupport() throws Exception {
        sendMessage();

        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());

        MessageConsumer consumer = session.createConsumer(destination);
        ActiveMQObjectMessage msg = (ActiveMQObjectMessage) consumer.receive();
        AMQ5921MessagePayload payload = (AMQ5921MessagePayload) msg.getObject();
        LOG.info("Received: {}", payload.getField1());

        session.close();
    }

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistent(false);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();

        connection = createConnection();
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {}
        }

        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connectionUri);
    }

    protected ActiveMQConnection createConnection() throws Exception {
        return (ActiveMQConnection) createConnectionFactory().createConnection();
    }

    private void sendMessage() throws Exception {
        AMQ5921MessagePayload msgPayload = new AMQ5921MessagePayload();
        msgPayload.setField1(void.class);  // <-- does not work

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(name.getMethodName());
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        ObjectMessage message = session.createObjectMessage();
        message.setObject(msgPayload);

        producer.send(message);
        session.close();
    }
}
