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
package org.apache.activemq.broker.virtual;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class VirtualTopicFlowControlDiscardTest {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualTopicFlowControlDiscardTest.class);

    final String payload = new String(new byte[155]);
    int numConsumers = 2;
    int total = 500;

    @Parameterized.Parameter(0)
    public boolean concurrentSend;

    @Parameterized.Parameter(1)
    public boolean transactedSend;

    @Parameterized.Parameter(2)
    public boolean sendFailGlobal;

    @Parameterized.Parameter(3)
    public boolean persistentBroker;


    BrokerService brokerService;
    ConnectionFactory connectionFactory;

    @Before
    public void createBroker() throws Exception  {
        brokerService = new BrokerService();
        brokerService.setPersistent(persistentBroker);
        brokerService.setDeleteAllMessagesOnStartup(true);
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry restrictedUsage = new PolicyEntry();
        restrictedUsage.setCursorMemoryHighWaterMark(50);
        restrictedUsage.setMemoryLimit(5000);
        restrictedUsage.setCursorMemoryHighWaterMark(110);
        if (sendFailGlobal) {
            brokerService.getSystemUsage().setSendFailIfNoSpace(true);
        } else {
            restrictedUsage.setSendFailIfNoSpace(true);
            restrictedUsage.setSendFailIfNoSpaceAfterTimeout(0);
        }

        policyMap.put(new ActiveMQQueue("Consumer.0.VirtualTopic.TEST"), restrictedUsage);
        brokerService.setDestinationPolicy(policyMap);
        brokerService.start();

        for (DestinationInterceptor destinationInterceptor  : brokerService.getDestinationInterceptors()) {
                for (VirtualDestination virtualDestination : ((VirtualDestinationInterceptor) destinationInterceptor).getVirtualDestinations()) {
                    if (virtualDestination instanceof VirtualTopic) {
                        ((VirtualTopic) virtualDestination).setConcurrentSend(concurrentSend);
                        ((VirtualTopic) virtualDestination).setTransactedSend(transactedSend);
                        ((VirtualTopic) virtualDestination).setDropOnResourceLimit(true);
                }
            }
        }
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        ActiveMQPrefetchPolicy zeroPrefetch = new ActiveMQPrefetchPolicy();
        zeroPrefetch.setAll(0);
        activeMQConnectionFactory.setPrefetchPolicy(zeroPrefetch);
        connectionFactory = activeMQConnectionFactory;
    }

    @After
    public void stopBroker() throws Exception  {
        brokerService.stop();
    }

    @Parameterized.Parameters(name ="cS=#{0},tS=#{1},g=#{2},persist=#{3}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.TRUE},
                {Boolean.TRUE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE},
                {Boolean.FALSE, Boolean.TRUE, Boolean.TRUE, Boolean.FALSE},
                {Boolean.TRUE, Boolean.FALSE, Boolean.TRUE, Boolean.FALSE},
                {Boolean.FALSE, Boolean.FALSE, Boolean.TRUE, Boolean.FALSE},
                {Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.FALSE},
                {Boolean.TRUE, Boolean.TRUE, Boolean.FALSE, Boolean.FALSE},
                {Boolean.FALSE, Boolean.FALSE, Boolean.FALSE, Boolean.TRUE},

        });
    }

    @Test
    public void testFanoutWithResourceException() throws Exception {

        Connection connection1 = connectionFactory.createConnection();
        connection1.start();

        Session session = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        for (int i=0; i<numConsumers; i++) {
            session.createConsumer(new ActiveMQQueue("Consumer." + i + ".VirtualTopic.TEST"));
        }

        Connection connection2 = connectionFactory.createConnection();
        connection2.start();
        Session producerSession = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(new ActiveMQTopic("VirtualTopic.TEST"));

        long start = System.currentTimeMillis();
        LOG.info("Starting producer: " + start);
        for (int i = 0; i < total; i++) {
            producer.send(producerSession.createTextMessage(payload));
        }
        LOG.info("Done producer, duration: " + (System.currentTimeMillis() - start) );

        Destination destination  = brokerService.getDestination(new ActiveMQQueue("Consumer.0.VirtualTopic.TEST"));
        LOG.info("Dest 0 size: " + (destination.getDestinationStatistics().getEnqueues().getCount()));
        assertTrue("did not get all", (destination.getDestinationStatistics().getEnqueues().getCount() < total));

        assertTrue("got all", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Destination dest  = brokerService.getDestination(new ActiveMQQueue("Consumer.1.VirtualTopic.TEST"));
                LOG.info("Dest 1 size: " + dest.getDestinationStatistics().getEnqueues().getCount());
                return total == dest.getDestinationStatistics().getEnqueues().getCount();
            }
        }));

        try {
            connection1.close();
        } catch (Exception ex) {}
        try {
            connection2.close();
        } catch (Exception ex) {}
    }
}
