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
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class AMQ5863CompositePublishTest {
    static Logger LOG = LoggerFactory.getLogger(AMQ5863CompositePublishTest.class);
    String brokerUrl;
    BrokerService brokerService;

    @Parameterized.Parameter(0)
    public TestSupport.PersistenceAdapterChoice persistenceAdapterChoice = TestSupport.PersistenceAdapterChoice.KahaDB;

    @Parameterized.Parameters(name = "#store:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {TestSupport.PersistenceAdapterChoice.KahaDB},
                {TestSupport.PersistenceAdapterChoice.LevelDB},
                {TestSupport.PersistenceAdapterChoice.JDBC}
        });
    }

    @Before
    public void startBroker() throws Exception {
        brokerService = new BrokerService();
        TestSupport.setPersistenceAdapter(brokerService, persistenceAdapterChoice);
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setUseCache(false);
        defaultEntry.setExpireMessagesPeriod(0);
        policyMap.setDefaultEntry(defaultEntry);
        brokerService.setDestinationPolicy(policyMap);

        TransportConnector transportConnector = brokerService.addConnector("tcp://0.0.0.0:0");
        brokerService.start();
        brokerUrl = transportConnector.getPublishableConnectString();
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void test() throws Exception {

        ActiveMQQueue compositeSendTo = new ActiveMQQueue("one,two,three");
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
        connectionFactory.setWatchTopicAdvisories(false);

        Connection connection = connectionFactory.createConnection();
        connection.start();

        try {

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createProducer(compositeSendTo).send(session.createTextMessage("Bing"));

            for (ActiveMQDestination dest : compositeSendTo.getCompositeDestinations()) {
                Message message = session.createConsumer(dest).receive(5000);
                LOG.info("From: " + dest + ", " + message.getJMSDestination());
                assertNotNull("got message from: " + dest, message);
            }

        } finally {
            connection.close();
        }
    }

}
