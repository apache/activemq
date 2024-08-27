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
package org.apache.activemq.broker.jmx;

import jakarta.jms.*;
import jakarta.jms.Connection;
import org.apache.activemq.*;
import org.apache.activemq.broker.*;
import org.apache.activemq.util.*;
import org.junit.*;

import static org.junit.Assert.assertTrue;

public class BrokerViewTest {
    protected BrokerService brokerService;
    protected ActiveMQConnectionFactory factory;
    protected Connection producerConnection;

    protected Session producerSession;
    protected MessageConsumer consumer;
    protected MessageProducer producer;
    protected Queue queue;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.start();

        factory =  new ActiveMQConnectionFactory(BrokerRegistry.getInstance().findFirst().getVmConnectorURI());
        producerConnection = factory.createConnection();
        producerConnection.start();
        producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = producerSession.createQueue("testQueue");
        producer = producerSession.createProducer(queue);
        producer.send(producerSession.createTextMessage("testMessage"));
    }

    @Test(timeout=120000)
    public void testBrokerViewRetrieveTotalQueuesAndTopicsCount() throws Exception {
        assertTrue(Wait.waitFor(() -> (brokerService.getAdminView()) != null));

        final BrokerView view = brokerService.getAdminView();
        // The total number of advisory topics
        assert(view.getTotalTopicsCount() == 4);
        // The queue created in setup
        assert(view.getTotalQueuesCount() == 1);
    }
}
