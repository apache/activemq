/*
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
package org.apache.activemq.broker.region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import java.util.Arrays;
import java.util.Collection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
@Category(ParallelTest.class)
public class FailedSubscriptionCleanupTest {

    @Parameterized.Parameters(name = "isTopic({0})")
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{{Boolean.TRUE}, {Boolean.FALSE}});
    }

    private final ActiveMQDestination parentDest;
    private final ActiveMQDestination dest;

    private BrokerService brokerService;
    private TopicRegion topicRegion;
    private QueueRegion queueRegion;

    public FailedSubscriptionCleanupTest(boolean topic) {
        this.parentDest = topic ? new ActiveMQTopic("test.dest.>") : new ActiveMQQueue("test.dest.>");
        this.dest = topic ? new ActiveMQTopic("test.dest.1") : new ActiveMQQueue("test.dest.1");
    }

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setDestinationFactory(new DestinationFactoryImpl(brokerService,
                brokerService.getTaskRunnerFactory(), brokerService.getPersistenceAdapter()) {
            @Override
            public Destination createDestination(ConnectionContext context,
                    ActiveMQDestination destination, DestinationStatistics destinationStatistics)
                    throws Exception {
                return new DestinationFilter(
                        super.createDestination(context, destination, destinationStatistics)) {
                    @Override
                    public void addSubscription(ConnectionContext context, Subscription sub)
                            throws Exception {
                        super.addSubscription(context, sub);
                        if (destination.equals(dest)) {
                            throw new IllegalStateException("failed");
                        }
                    }
                };
            }
        });
        brokerService.start();
        brokerService.waitUntilStarted();
        RegionBroker regionBroker = (RegionBroker) brokerService.getRegionBroker();
        topicRegion = (TopicRegion) regionBroker.getTopicRegion();
        queueRegion = (QueueRegion) regionBroker.getQueueRegion();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    private Connection createConnection() throws Exception {
        var factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        factory.setWatchTopicAdvisories(false);
        var connection = factory.createConnection();
        connection.start();
        return connection;
    }

    // cleanup for failure adding consumer on single dest
    @Test
    public void testFailedSubscriptionCleanup() throws Exception {
        AbstractRegion region = dest.isTopic() ? topicRegion : queueRegion;
        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertThrows(JMSException.class, () -> session.createConsumer(dest));

        assertEquals(0, brokerService.getDestination(dest)
                .getDestinationStatistics().getConsumers().getCount());
        assertTrue(region.getSubscriptions().isEmpty());
    }

    // check cleanup for failure for multiple matching dests
    @Test
    public void testMultipleFailedSubscriptionCleanup() throws Exception {
        brokerService.getBroker().addDestination(brokerService.getAdminConnectionContext(),
                parentDest, false);

        AbstractRegion region = dest.isTopic() ? topicRegion : queueRegion;
        Connection connection = createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertThrows(JMSException.class, () -> session.createConsumer(dest));
        assertTrue(region.getSubscriptions().isEmpty());
        assertEquals(0, brokerService.getDestination(dest)
                .getDestinationStatistics().getConsumers().getCount());
        assertEquals(0, brokerService.getDestination(parentDest)
                .getDestinationStatistics().getConsumers().getCount());
    }

}
