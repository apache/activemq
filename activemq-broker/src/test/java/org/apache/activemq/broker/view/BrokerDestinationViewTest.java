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
package org.apache.activemq.broker.view;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.DestinationsViewFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BrokerDestinationViewTest {

    protected BrokerService brokerService;
    protected ActiveMQConnectionFactory factory;
    protected Connection producerConnection;

    protected Session producerSession;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected Queue queue;
    protected int messageCount = 10000;
    protected int timeOutInSeconds = 10;
    protected String queueName = "testQueue";

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.start();

        factory =  new ActiveMQConnectionFactory(BrokerRegistry.getInstance().findFirst().getVmConnectorURI());
        producerConnection = factory.createConnection();
        producerConnection.start();
        producerSession = producerConnection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        queue = producerSession.createQueue(queueName);
        producer = producerSession.createProducer(queue);
    }

    @After
    public void tearDown() throws Exception {
        if (producerConnection != null){
            producerConnection.close();
        }
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void testBrokerDestinationView() throws Exception {
         for (int i = 0; i < messageCount; i++){
             Message message = producerSession.createTextMessage("test " + i);
             producer.send(message);

         }

         MessageBrokerView messageBrokerView = MessageBrokerViewRegistry.getInstance().lookup("");
         BrokerDestinationView destinationView = messageBrokerView.getQueueDestinationView(queueName);
         assertEquals(destinationView.getQueueSize(),messageCount);

         final DestinationsViewFilter filter = new DestinationsViewFilter();
         filter.setName(queueName);
         filter.setFilter("nonEmpty");
         final ObjectMapper mapper = new ObjectMapper();

         final BrokerViewMBean brokerView = getBrokerView();
         String output = brokerView.queryQueues(mapper.writeValueAsString(filter), 1, 10);
         Map<?,?> queryResults = mapper.readValue(output, Map.class);

         final Integer count = (Integer) queryResults.get("count");
         final Map<?,?> data = (Map<?, ?>) queryResults.get("data");
         assertEquals((Integer)1, count);
         assertEquals(1, data.size());
    }

    private BrokerViewMBean getBrokerView() throws MalformedObjectNameException {
        ObjectName brokerName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost");
        BrokerViewMBean view = (BrokerViewMBean) brokerService.getManagementContext().newProxyInstance(brokerName, BrokerViewMBean.class, true);
        assertNotNull(view);
        return view;
    }
}
