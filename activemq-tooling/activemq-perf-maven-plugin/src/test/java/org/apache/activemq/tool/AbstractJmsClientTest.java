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
package org.apache.activemq.tool;

import static org.apache.activemq.command.ActiveMQDestination.*;
import static org.junit.Assert.assertEquals;

import java.net.URI;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.tool.properties.JmsClientProperties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AbstractJmsClientTest {

    public class NullJmsClient extends AbstractJmsClient {
        private JmsClientProperties client;

        public NullJmsClient(ConnectionFactory factory) {
            super(factory);
        }

        @Override
        public JmsClientProperties getClient() {
            return client;
        }

        @Override
        public void setClient(JmsClientProperties client) {
            this.client = client;
        }
    }

    private final String DEFAULT_DEST = "TEST.FOO";
    private static BrokerService brokerService;
    private static ActiveMQConnectionFactory connectionFactory;

    private AbstractJmsClient jmsClient;
    private JmsClientProperties clientProperties;

    @BeforeClass
    public static void setUpBrokerAndConnectionFactory() throws Exception {
        brokerService = BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false"));
        brokerService.start();
        connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
    }

    @AfterClass
    public static void tearDownBroker() throws Exception {
        brokerService.stop();
    }

    @Before
    public void setUp() {
        jmsClient = new NullJmsClient(connectionFactory);
        clientProperties = new JmsClientProperties();
        clientProperties.setDestName(DEFAULT_DEST);
        jmsClient.setClient(clientProperties);
    }

    @Test
    public void testCreateDestination() throws JMSException {
        assertDestinationNameType("dest", TOPIC_TYPE,
                asAmqDest(jmsClient.createDestination("dest")));
    }

    @Test
    public void testCreateDestination_topic() throws JMSException {
        assertDestinationNameType("dest", TOPIC_TYPE,
                asAmqDest(jmsClient.createDestination("topic://dest")));
    }

    @Test
    public void testCreateDestination_queue() throws JMSException {
        assertDestinationNameType("dest", QUEUE_TYPE,
                asAmqDest(jmsClient.createDestination("queue://dest")));
    }

    @Test
    public void testCreateDestination_tempQueue() throws JMSException {
        assertDestinationType(TEMP_QUEUE_TYPE,
                asAmqDest(jmsClient.createDestination("temp-queue://dest")));
    }

    @Test
    public void testCreateDestination_tempTopic() throws JMSException {
        assertDestinationType(TEMP_TOPIC_TYPE,
                asAmqDest(jmsClient.createDestination("temp-topic://dest")));
    }

    @Test
    public void testCreateDestinations_commaSeparated() throws JMSException {
        clientProperties.setDestName("queue://foo,topic://cheese");
        Destination[] destinations = jmsClient.createDestinations(1);
        assertEquals(2, destinations.length);
        assertDestinationNameType("foo", QUEUE_TYPE, asAmqDest(destinations[0]));
        assertDestinationNameType("cheese", TOPIC_TYPE, asAmqDest(destinations[1]));
    }

    @Test
    public void testCreateDestinations_multipleComposite() throws JMSException {
        clientProperties.setDestComposite(true);
        clientProperties.setDestName("queue://foo,queue://cheese");
        Destination[] destinations = jmsClient.createDestinations(1);
        assertEquals(1, destinations.length);
        // suffixes should be added
        assertDestinationNameType("foo,cheese", QUEUE_TYPE, asAmqDest(destinations[0]));
    }

    @Test
    public void testCreateDestinations() throws JMSException {
        Destination[] destinations = jmsClient.createDestinations(1);
        assertEquals(1, destinations.length);
        assertDestinationNameType(DEFAULT_DEST, TOPIC_TYPE, asAmqDest(destinations[0]));
    }

    @Test
    public void testCreateDestinations_multiple() throws JMSException {
        Destination[] destinations = jmsClient.createDestinations(2);
        assertEquals(2, destinations.length);
        // suffixes should be added
        assertDestinationNameType(DEFAULT_DEST + ".0", TOPIC_TYPE, asAmqDest(destinations[0]));
        assertDestinationNameType(DEFAULT_DEST + ".1", TOPIC_TYPE, asAmqDest(destinations[1]));
    }

    @Test
    public void testCreateDestinations_multipleCommaSeparated() throws JMSException {
        clientProperties.setDestName("queue://foo,topic://cheese");
        Destination[] destinations = jmsClient.createDestinations(2);
        assertEquals(4, destinations.length);
        // suffixes should be added
        assertDestinationNameType("foo.0", QUEUE_TYPE, asAmqDest(destinations[0]));
        assertDestinationNameType("foo.1", QUEUE_TYPE, asAmqDest(destinations[1]));
        assertDestinationNameType("cheese.0", TOPIC_TYPE, asAmqDest(destinations[2]));
        assertDestinationNameType("cheese.1", TOPIC_TYPE, asAmqDest(destinations[3]));
    }

    @Test
    public void testCreateDestinations_composite() throws JMSException {
        clientProperties.setDestComposite(true);
        Destination[] destinations = jmsClient.createDestinations(2);
        assertEquals(1, destinations.length);
        // suffixes should be added
        String expectedDestName = DEFAULT_DEST + ".0," + DEFAULT_DEST + ".1";
        assertDestinationNameType(expectedDestName, TOPIC_TYPE, asAmqDest(destinations[0]));
    }

    @Test
    public void testCreateDestinations_compositeQueue() throws JMSException {
        clientProperties.setDestComposite(true);
        clientProperties.setDestName("queue://" + DEFAULT_DEST);
        Destination[] destinations = jmsClient.createDestinations(2);
        assertEquals(1, destinations.length);
        // suffixes should be added
        String expectedDestName = DEFAULT_DEST + ".0," + DEFAULT_DEST + ".1";
        assertDestinationNameType(expectedDestName, QUEUE_TYPE, asAmqDest(destinations[0]));
    }

    @Test
    public void testCreateDestinations_compositeCommaSeparated() throws JMSException {
        clientProperties.setDestComposite(true);
        clientProperties.setDestName("queue://foo,topic://cheese");
        Destination[] destinations = jmsClient.createDestinations(2);
        assertEquals(2, destinations.length);

        assertDestinationNameType("foo.0,foo.1", QUEUE_TYPE, asAmqDest(destinations[0]));
        assertDestinationNameType("cheese.0,cheese.1", TOPIC_TYPE, asAmqDest(destinations[1]));
    }

    private void assertDestinationNameType(String physicalName, byte destinationType, ActiveMQDestination destination) {
        assertEquals(destinationType, destination.getDestinationType());
        assertEquals(physicalName, destination.getPhysicalName());
    }

    private void assertDestinationType(byte destinationType, ActiveMQDestination destination) {
        assertEquals(destinationType, destination.getDestinationType());
    }

    private ActiveMQDestination asAmqDest(Destination destination) {
        return (ActiveMQDestination) destination;
    }
}
