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

import java.util.Arrays;
import java.util.Collection;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import javax.jms.*;
import java.net.URI;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * This tests that subscribing to a wildcard and sending a ConsumerControl
 * command for that wildcard sub will not auto create the destination
 * by mistake.
 */
@RunWith(Parameterized.class)
public class AMQ9475Test {

    @Parameters(name = "queue={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    public AMQ9475Test(boolean queue) {
        this.destination1 = queue ? new ActiveMQQueue("a.>") : new ActiveMQTopic("a.>");
        this.destination2 = queue ? new ActiveMQQueue("a") : new ActiveMQTopic("a");
    }

    private BrokerService brokerService;
    private String connectionUri;
    private final ActiveMQDestination destination1;
    private final ActiveMQDestination destination2;

    protected ConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory conFactory = new ActiveMQConnectionFactory(connectionUri);
        conFactory.setWatchTopicAdvisories(false);
        return conFactory;
    }

    @Before
    public void setUp() throws Exception {
        brokerService = BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false&useJmx=true"));
        brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.start();
        connectionUri = brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString();
    }

    // Normal use case to verify wildcard sub is not created
    @Test
    public void testNormalWildcardSub() throws Exception {
        Session session;
        try (Connection connection = createConnectionFactory().createConnection()) {
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(destination1);
            sendMessage(session, destination2, "test");
            assertNotNull(consumer.receive(1000));

            assertNull(brokerService.getBroker().getDestinationMap().get(destination1));
            assertNotNull(brokerService.getBroker().getDestinationMap().get(destination2));
        }
    }

    // Test that the wildcard dest is still not auto-created even after sending the
    // ConsumerControl object for it
    @Test
    public void testWildcardConsumerControl() throws Exception {
        Session session;
        try (ActiveMQConnection connection = (ActiveMQConnection) createConnectionFactory().createConnection()) {
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(destination1);

            ConsumerControl control = new ConsumerControl();
            control.setDestination(destination1);
            control.setConsumerId(consumer.getConsumerId());
            control.setPrefetch(10);
            connection.syncSendPacket(control);

            sendMessage(session, destination2, "test");
            assertNotNull(consumer.receive(1000));

            assertNull(brokerService.getBroker().getDestinationMap().get(destination1));
            assertNotNull(brokerService.getBroker().getDestinationMap().get(destination2));
        }
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    private void sendMessage(Session session, Destination destination, String text) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        producer.send(session.createTextMessage(text));
        producer.close();
    }
}
