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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.AbortSlowAckConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.net.URI;

import static org.junit.Assert.assertNotNull;

public class AMQ7077Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ7077Test.class);

    private BrokerService brokerService;
    private String connectionUri;

    protected ConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory conFactory = new ActiveMQConnectionFactory(connectionUri);
        conFactory.setWatchTopicAdvisories(false);
        return conFactory;
    }

    protected AbortSlowAckConsumerStrategy createSlowConsumerStrategy() {
        AbortSlowAckConsumerStrategy strategy = new AbortSlowAckConsumerStrategy();
        strategy.setCheckPeriod(500);
        strategy.setMaxTimeSinceLastAck(1000);
        strategy.setMaxSlowCount(2);
        strategy.setIgnoreIdleConsumers(false);
        return strategy;
    }

    @Before
    public void setUp() throws Exception {
        brokerService = BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false&useJmx=true"));
        PolicyEntry policy = new PolicyEntry();

        policy.setSlowConsumerStrategy(createSlowConsumerStrategy());
        policy.setQueuePrefetch(10);
        policy.setTopicPrefetch(10);
        policy.setAdvisoryForSlowConsumers(true);
        PolicyMap pMap = new PolicyMap();
        pMap.put(new ActiveMQQueue(">"), policy);
        brokerService.setUseJmx(false);
        brokerService.setDestinationPolicy(pMap);
        brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.start();

        connectionUri = brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString();
    }

    @Test
    public void testAdvisoryOnSlowAckDetection() throws Exception {
        Connection connection = createConnectionFactory().createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination destination = session.createQueue("DD");

        MessageConsumer consumer = session.createConsumer(destination);
        // will be idle and can get removed but will be marked slow and now produce an advisory

        MessageConsumer advisoryConsumer = session.createConsumer(AdvisorySupport.getSlowConsumerAdvisoryTopic(destination));
        Message message = advisoryConsumer.receive(10000);
        if (message == null) {
            message = advisoryConsumer.receive(2000);
        }
        assertNotNull("Got advisory", message);
        connection.close();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }
}
