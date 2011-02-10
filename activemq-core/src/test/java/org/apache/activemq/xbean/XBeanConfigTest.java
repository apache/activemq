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
package org.apache.activemq.xbean;

import java.net.URI;
import junit.framework.TestCase;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.policy.DispatchPolicy;
import org.apache.activemq.broker.region.policy.LastImageSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.RoundRobinDispatchPolicy;
import org.apache.activemq.broker.region.policy.StrictOrderDispatchPolicy;
import org.apache.activemq.broker.region.policy.SubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.TimedSubscriptionRecoveryPolicy;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision: 1.1 $
 */
public class XBeanConfigTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(XBeanConfigTest.class);

    protected BrokerService brokerService;
    protected Broker broker;
    protected ConnectionContext context;
    protected ConnectionInfo info;

    public void testBrokerConfiguredCorrectly() throws Exception {

        // Validate the system properties are being evaluated in xbean.
        assertEquals("testbroker", brokerService.getBrokerName());

        Topic topic = (Topic)broker.addDestination(context, new ActiveMQTopic("FOO.BAR"),true);
        DispatchPolicy dispatchPolicy = topic.getDispatchPolicy();
        assertTrue("dispatchPolicy should be RoundRobinDispatchPolicy: " + dispatchPolicy, dispatchPolicy instanceof RoundRobinDispatchPolicy);

        SubscriptionRecoveryPolicy subscriptionRecoveryPolicy = topic.getSubscriptionRecoveryPolicy();
        assertTrue("subscriptionRecoveryPolicy should be LastImageSubscriptionRecoveryPolicy: " + subscriptionRecoveryPolicy,
                   subscriptionRecoveryPolicy instanceof LastImageSubscriptionRecoveryPolicy);

        LOG.info("destination: " + topic);
        LOG.info("dispatchPolicy: " + dispatchPolicy);
        LOG.info("subscriptionRecoveryPolicy: " + subscriptionRecoveryPolicy);

        topic = (Topic)broker.addDestination(context, new ActiveMQTopic("ORDERS.BOOKS"),true);
        dispatchPolicy = topic.getDispatchPolicy();
        assertTrue("dispatchPolicy should be StrictOrderDispatchPolicy: " + dispatchPolicy, dispatchPolicy instanceof StrictOrderDispatchPolicy);

        subscriptionRecoveryPolicy = topic.getSubscriptionRecoveryPolicy();
        assertTrue("subscriptionRecoveryPolicy should be TimedSubscriptionRecoveryPolicy: " + subscriptionRecoveryPolicy,
                   subscriptionRecoveryPolicy instanceof TimedSubscriptionRecoveryPolicy);
        TimedSubscriptionRecoveryPolicy timedSubcriptionPolicy = (TimedSubscriptionRecoveryPolicy)subscriptionRecoveryPolicy;
        assertEquals("getRecoverDuration()", 60000, timedSubcriptionPolicy.getRecoverDuration());

        LOG.info("destination: " + topic);
        LOG.info("dispatchPolicy: " + dispatchPolicy);
        LOG.info("subscriptionRecoveryPolicy: " + subscriptionRecoveryPolicy);
    }

    @Override
    protected void setUp() throws Exception {
        System.setProperty("brokername", "testbroker");
        brokerService = createBroker();
        broker = brokerService.getBroker();

        // started automatically
        // brokerService.start();

        context = new ConnectionContext();
        context.setBroker(broker);
        info = new ConnectionInfo();
        info.setClientId("James");
        info.setUserName("James");
        info.setConnectionId(new ConnectionId("1234"));

        try {
            broker.addConnection(context, info);
        } catch (Throwable e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        assertNotNull("No broker created!");
    }

    @Override
    protected void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    protected BrokerService createBroker() throws Exception {
        String uri = "org/apache/activemq/xbean/activemq-policy.xml";
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }

}
