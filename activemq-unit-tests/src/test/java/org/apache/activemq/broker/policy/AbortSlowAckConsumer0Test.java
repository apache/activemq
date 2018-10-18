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
package org.apache.activemq.broker.policy;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.AbortSlowAckConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class AbortSlowAckConsumer0Test extends AbortSlowConsumer0Test {

    protected long maxTimeSinceLastAck = 5 * 1000;
    protected AbortSlowAckConsumerStrategy strategy;

    @Parameterized.Parameters(name = "isTopic({0})")
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{{Boolean.TRUE}, {Boolean.FALSE}});
    }

    public AbortSlowAckConsumer0Test(Boolean isTopic) {
        super();
        this.topic = isTopic;
    }

    @Override
    protected AbortSlowAckConsumerStrategy createSlowConsumerStrategy() {
        AbortSlowAckConsumerStrategy strategy = new AbortSlowAckConsumerStrategy();
        strategy.setAbortConnection(abortConnection);
        strategy.setCheckPeriod(checkPeriod);
        strategy.setMaxSlowDuration(maxSlowDuration);
        strategy.setMaxTimeSinceLastAck(maxTimeSinceLastAck);

        return strategy;
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        PolicyEntry policy = new PolicyEntry();

        strategy = createSlowConsumerStrategy();
        underTest = strategy;

        policy.setSlowConsumerStrategy(strategy);
        policy.setQueuePrefetch(10);
        policy.setTopicPrefetch(10);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);
        return broker;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.getPrefetchPolicy().setAll(1);
        return factory;
    }

    @Override
    @Test
    public void testSlowConsumerIsAbortedViaJmx() throws Exception {
        strategy.setMaxTimeSinceLastAck(500); // so jmx does the abort
        super.testSlowConsumerIsAbortedViaJmx();
    }

    @Test
    public void testZeroPrefetchConsumerIsAborted() throws Exception {
        strategy.setMaxTimeSinceLastAck(2000); // Make it shorter

        ActiveMQConnection conn = (ActiveMQConnection) createConnectionFactory().createConnection();
        conn.setExceptionListener(this);
        connections.add(conn);

        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageConsumer consumer = sess.createConsumer(destination);
        assertNotNull(consumer);
        conn.start();
        startProducers(destination, 20);

        Message message = consumer.receive(5000);
        assertNotNull(message);

        TimeUnit.SECONDS.sleep(15);

        try {
            consumer.receive(5000);
            fail("Slow consumer not aborted.");
        } catch (Exception ex) {
        }
    }

    @Test
    public void testIdleConsumerCanBeAbortedNoMessages() throws Exception {
        strategy.setIgnoreIdleConsumers(false);
        strategy.setMaxTimeSinceLastAck(2000); // Make it shorter

        ActiveMQConnection conn = (ActiveMQConnection) createConnectionFactory().createConnection();
        conn.setExceptionListener(this);
        connections.add(conn);

        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageConsumer consumer = sess.createConsumer(destination);
        assertNotNull(consumer);
        conn.start();

        startProducers(destination, 1);

        Message message = consumer.receive(5000);
        assertNotNull(message);

        // Consumer needs to be closed before the reeive call.
        TimeUnit.SECONDS.sleep(15);

        try {
            consumer.receive(5000);
            fail("Idle consumer not aborted.");
        } catch (Exception ex) {
        }
    }

    @Test
    public void testIdleConsumerCanBeAborted() throws Exception {
        strategy.setIgnoreIdleConsumers(false);
        strategy.setMaxTimeSinceLastAck(2000); // Make it shorter

        ActiveMQConnection conn = (ActiveMQConnection) createConnectionFactory().createConnection();
        conn.setExceptionListener(this);
        connections.add(conn);

        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageConsumer consumer = sess.createConsumer(destination);
        assertNotNull(consumer);
        conn.start();
        startProducers(destination, 1);

        Message message = consumer.receive(5000);
        assertNotNull(message);
        message.acknowledge();

        // Consumer needs to be closed before the reeive call.
        TimeUnit.SECONDS.sleep(15);

        try {
            consumer.receive(5000);
            fail("Idle consumer not aborted.");
        } catch (Exception ex) {
        }
    }
}
