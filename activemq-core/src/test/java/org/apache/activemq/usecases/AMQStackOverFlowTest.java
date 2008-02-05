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

package org.apache.activemq.usecases;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.usage.SystemUsage;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

public class AMQStackOverFlowTest extends TestCase {

    private static final String URL1 = "tcp://localhost:61616";

    private static final String URL2 = "tcp://localhost:61617";

    public void testStackOverflow() throws Exception {
        BrokerService brokerService1 = null;
        BrokerService brokerService2 = null;

        try {
            brokerService1 = createBrokerService("broker1", URL1, URL2);
            brokerService1.start();
            brokerService2 = createBrokerService("broker2", URL2, URL1);
            brokerService2.start();

            final ActiveMQConnectionFactory cf1 = new ActiveMQConnectionFactory(URL1);
            cf1.setUseAsyncSend(false);

            final ActiveMQConnectionFactory cf2 = new ActiveMQConnectionFactory(URL2);
            cf2.setUseAsyncSend(false);

            final JmsTemplate template1 = new JmsTemplate(cf1);
            template1.setReceiveTimeout(10000);

            template1.send("test.q", new MessageCreator() {

                public Message createMessage(Session session) throws JMSException {
                    return session.createTextMessage("test");
                }

            });

            final JmsTemplate template2 = new JmsTemplate(cf2);
            template2.setReceiveTimeout(10000);

            final Message m = template2.receive("test.q");
            assertTrue(m instanceof TextMessage);

            final TextMessage tm = (TextMessage)m;

            Assert.assertEquals("test", tm.getText());

            template2.send("test2.q", new MessageCreator() {

                public Message createMessage(Session session) throws JMSException {
                    return session.createTextMessage("test2");
                }

            });

            final Message m2 = template1.receive("test2.q");
            assertNotNull(m2);
            assertTrue(m2 instanceof TextMessage);

            final TextMessage tm2 = (TextMessage)m2;

            Assert.assertEquals("test2", tm2.getText());

        } finally {

            brokerService1.stop();
            brokerService1 = null;
            brokerService2.stop();
            brokerService2 = null;

        }

    }

    private BrokerService createBrokerService(final String brokerName, final String uri1, final String uri2)
        throws Exception {
        final BrokerService brokerService = new BrokerService();

        brokerService.setBrokerName(brokerName);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);

        final SystemUsage memoryManager = new SystemUsage();
        //memoryManager.getMemoryUsage().setLimit(10);
        brokerService.setSystemUsage(memoryManager);

        final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();

        final PolicyEntry entry = new PolicyEntry();
        entry.setQueue(">");
        //entry.setMemoryLimit(1);
        policyEntries.add(entry);

        final PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(policyEntries);
        brokerService.setDestinationPolicy(policyMap);

        final TransportConnector tConnector = new TransportConnector();
        tConnector.setUri(new URI(uri1));
        tConnector.setBrokerName(brokerName);
        tConnector.setName(brokerName + ".transportConnector");
        brokerService.addConnector(tConnector);

        if (uri2 != null) {
            final NetworkConnector nc = new DiscoveryNetworkConnector(new URI("static:" + uri2));
            nc.setBridgeTempDestinations(true);
            nc.setBrokerName(brokerName);
            //nc.setPrefetchSize(1);
            brokerService.addNetworkConnector(nc);
        }

        return brokerService;

    }
}
