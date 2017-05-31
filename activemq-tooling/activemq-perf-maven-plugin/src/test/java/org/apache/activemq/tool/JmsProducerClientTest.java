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
import static org.junit.Assert.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.tool.properties.JmsProducerProperties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.jms.Destination;
import javax.jms.JMSException;
import java.net.URI;

public class JmsProducerClientTest {

    private final String DEFAULT_DEST = "TEST.FOO";
    private static BrokerService brokerService;
    private static ActiveMQConnectionFactory connectionFactory;

    private AbstractJmsClient jmsClient;
    private JmsProducerProperties producerProperties;

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
        jmsClient = new JmsProducerClient(connectionFactory);
        producerProperties = new JmsProducerProperties();
        producerProperties.setDestName(DEFAULT_DEST);
        jmsClient.setClient(producerProperties);
    }

    @Test
    public void testCreateDestination_tempQueue() throws JMSException {
        assertDestinationNameType("dest", QUEUE_TYPE,
                asAmqDest(jmsClient.createDestination("temp-queue://dest")));
    }

    @Test
    public void testCreateDestination_tempTopic() throws JMSException {
        assertDestinationNameType("dest", TOPIC_TYPE,
                asAmqDest(jmsClient.createDestination("temp-topic://dest")));
    }

    private void assertDestinationNameType(String physicalName, byte destinationType, ActiveMQDestination destination) {
        assertEquals(destinationType, destination.getDestinationType());
        assertEquals(physicalName, destination.getPhysicalName());
    }

    private ActiveMQDestination asAmqDest(Destination destination) {
        return (ActiveMQDestination) destination;
    }
}
