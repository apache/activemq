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

import jakarta.jms.Connection;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertTrue;

public class AMQ9685Test {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    private BrokerService brokerService;
    private Connection connection;
    private String dir;
    private ActiveMQQueue destination = new ActiveMQQueue("Consumer.foo.");

    @Before
    public void init() throws Exception {
        dir = temp.newFolder().getAbsolutePath();
        brokerService = createBroker();
        brokerService.start();
        connection = new ActiveMQConnectionFactory(
            brokerService.getVmConnectorURI()).createConnection();
        connection.start();
    }

    @After
    public void after() throws Exception {
        try {
            connection.close();
        } catch (Exception e) {
            //swallow any error so broker can still be stopped
        }
        brokerService.stop();
    }

    @Test
    public void testVirtualTopicMissingNameManual() throws Exception {
        // Test manual creation
        brokerService.getRegionBroker().addDestination(brokerService.getAdminConnectionContext(),
            destination, false);

        // Verify created
        assertTrue(brokerService.getBroker().getDestinationMap().containsKey(destination));
        assertTrue(brokerService.getPersistenceAdapter().getDestinations().contains(destination));

        // Verify restart without issue
        restart();
    }

    @Test
    public void testVirtualTopicMissingNameConsumer() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // test consumer attempt dynamic creation
        session.createConsumer(destination, null);

        // Verify created
        assertTrue(brokerService.getBroker().getDestinationMap().containsKey(destination));
        assertTrue(brokerService.getPersistenceAdapter().getDestinations().contains(destination));

        // Verify restart without issue
        restart();
    }

    private BrokerService createBroker() {
        BrokerService broker = new BrokerService();
        broker.setAdvisorySupport(false);
        broker.setPersistent(true);
        broker.setDataDirectory(dir);

        VirtualTopic virtualTopic = new VirtualTopic();
        VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
        interceptor.setVirtualDestinations(new VirtualDestination[]{virtualTopic});
        broker.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});
        return broker;
    }

    private void restart() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
        brokerService = createBroker();
        brokerService.start();
        brokerService.waitUntilStarted();
    }
}
