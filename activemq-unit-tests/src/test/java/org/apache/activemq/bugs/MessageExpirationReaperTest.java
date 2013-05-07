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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import javax.jms.*;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test to determine if expired messages are being reaped if there is
 * no active consumer connected to the broker.
 */
public class MessageExpirationReaperTest {

    private BrokerService broker;
    private ConnectionFactory factory;
    private ActiveMQConnection connection;
    private final String destinationName = "TEST.Q";
    private final String brokerUrl = "tcp://localhost:0";
    private final String brokerName = "testBroker";
    private String connectionUri;

    @Before
    public void init() throws Exception {
        createBroker();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();

        factory = createConnectionFactory();
        connection = (ActiveMQConnection) factory.createConnection();
        connection.setClientID("test-connection");
        connection.start();
    }

    @After
    public void cleanUp() throws Exception {
        connection.close();
        broker.stop();
    }

    protected void createBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setBrokerName(brokerName);
        broker.addConnector(brokerUrl);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setExpireMessagesPeriod(500);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);

        broker.start();
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connectionUri);
    }

    protected Session createSession() throws Exception {
        return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void testExpiredMessageReaping() throws Exception {

        Session producerSession = createSession();
        ActiveMQDestination destination =  (ActiveMQDestination) producerSession.createQueue(destinationName);
        MessageProducer producer = producerSession.createProducer(destination);
        producer.setTimeToLive(1000);

        final int count = 3;
        // Send some messages with an expiration
        for (int i = 0; i < count; i++) {
            TextMessage message = producerSession.createTextMessage("" + i);
            producer.send(message);
        }

        // Let the messages expire
        Thread.sleep(2000);

        DestinationViewMBean view = createView(destination);

        assertEquals("Incorrect inflight count: " + view.getInFlightCount(), 0, view.getInFlightCount());
        assertEquals("Incorrect queue size count", 0, view.getQueueSize());
        assertEquals("Incorrect expired size count", view.getEnqueueCount(), view.getExpiredCount());

        // Send more messages with an expiration
        for (int i = 0; i < count; i++) {
            TextMessage message = producerSession.createTextMessage("" + i);
            producer.send(message);
        }

        // Let the messages expire
        Thread.sleep(2000);

        // Simply browse the queue
        Session browserSession = createSession();
        QueueBrowser browser = browserSession.createBrowser((Queue) destination);
        assertFalse("no message in the browser", browser.getEnumeration().hasMoreElements());

        // The messages expire and should be reaped because of the presence of
        // the queue browser
        assertEquals("Wrong inFlightCount: " + view.getInFlightCount(), 0, view.getInFlightCount());
    }

    @Test
    public void testExpiredMessagesOnTopic() throws Exception{
        Session session = createSession();

        // use a zero prefetch so messages don't go inflight
        ActiveMQTopic destination = new ActiveMQTopic(destinationName + "?consumer.prefetchSize=0");

        MessageProducer producer = session.createProducer(destination);

        // should have a durable sub because it's a little tricky to get messages to expire in
        // non-durable subs.. with durable subs, we can just expire in the topic using the expire
        // period.. also.. durable sub has to be "inactive" for the expire checker to actually
        // expire the messages
        MessageConsumer consumer = session.createDurableSubscriber(destination, "test-durable");

        producer.setTimeToLive(500);

        final int count = 3;
        // Send some messages with an expiration
        for (int i = 0; i < count; i++) {
            TextMessage message = session.createTextMessage("" + i);
            producer.send(message);
        }

        DestinationViewMBean view = createView(destination);
        // not expired yet...
        assertEquals("Incorrect enqueue count", 3, view.getEnqueueCount() );

        // close consumer so topic thinks consumer is inactive
        consumer.close();

        // Let the messages reach an expiry time
        Thread.sleep(2000);

        assertEquals("Incorrect inflight count: " + view.getInFlightCount(), 0, view.getInFlightCount());
        assertEquals("Incorrect queue size count", 0, view.getQueueSize());
        assertEquals("Incorrect expired size count", view.getEnqueueCount(), view.getExpiredCount());
    }

    protected DestinationViewMBean createView(ActiveMQDestination destination) throws Exception {
        String domain = "org.apache.activemq";
        ObjectName name;
        if (destination.isQueue()) {
            name = new ObjectName(domain + ":type=Broker,brokerName=" + brokerName + ",destinationType=Queue,destinationName=" + destinationName);
        } else {
            name = new ObjectName(domain + ":type=Broker,brokerName=" + brokerName + ",destinationType=Topic,destinationName=" + destinationName);
        }
        return (DestinationViewMBean) broker.getManagementContext().newProxyInstance(name, DestinationViewMBean.class,
                true);
    }
}
