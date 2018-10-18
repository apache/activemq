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
import org.apache.activemq.broker.jmx.AbortSlowConsumerStrategyViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.AbortSlowAckConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;
import java.net.URI;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
        strategy.setMaxSlowDuration(0);
        strategy.setMaxSlowCount(4);
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

        MessageConsumer advisoryConsumer = session.createConsumer(AdvisorySupport.getSlowConsumerAdvisoryTopic(destination));

        MessageConsumer consumer = session.createConsumer(destination);
        // will be idle and can get removed but will be marked slow and now produce an advisory

        Message message = advisoryConsumer.receive(10000);
        if (message == null) {
            message = advisoryConsumer.receive(2000);
        }
        assertNotNull("Got advisory", message);
        connection.close();

        QueueViewMBean queue = getProxyToQueue(((Queue) destination).getQueueName());
        ObjectName slowConsumerPolicyMBeanName = queue.getSlowConsumerStrategy();
        assertNotNull(slowConsumerPolicyMBeanName);

        AbortSlowConsumerStrategyViewMBean abortPolicy = (AbortSlowConsumerStrategyViewMBean)
                brokerService.getManagementContext().newProxyInstance(slowConsumerPolicyMBeanName, AbortSlowConsumerStrategyViewMBean.class, true);

        assertTrue("slow list is gone on remove", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                TabularData slowOnes = abortPolicy.getSlowConsumers();
                LOG.info("slow ones:"  + slowOnes);
                return slowOnes.size() == 0;
            }
        }));

    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }
}
