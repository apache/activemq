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

import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.AbortSlowAckConsumerStrategy;
import org.apache.activemq.broker.region.policy.AbortSlowConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AbortSlowAckConsumerTest extends AbortSlowConsumerTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbortSlowAckConsumerTest.class);

    protected long maxTimeSinceLastAck = 5 * 1000;

    @Override
    protected AbortSlowConsumerStrategy createSlowConsumerStrategy() {
        return new AbortSlowConsumerStrategy();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        PolicyEntry policy = new PolicyEntry();

        AbortSlowAckConsumerStrategy strategy = (AbortSlowAckConsumerStrategy) underTest;
        strategy.setAbortConnection(abortConnection);
        strategy.setCheckPeriod(checkPeriod);
        strategy.setMaxSlowDuration(maxSlowDuration);
        strategy.setMaxTimeSinceLastAck(maxTimeSinceLastAck);

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
    public void testSlowConsumerIsAbortedViaJmx() throws Exception {
        AbortSlowAckConsumerStrategy strategy = (AbortSlowAckConsumerStrategy) underTest;
        strategy.setMaxTimeSinceLastAck(500); // so jmx does the abort
        super.testSlowConsumerIsAbortedViaJmx();
    }

    @Override
    public void initCombosForTestSlowConsumerIsAborted() {
        addCombinationValues("abortConnection", new Object[]{Boolean.TRUE, Boolean.FALSE});
        addCombinationValues("topic", new Object[]{Boolean.TRUE, Boolean.FALSE});
    }

    public void testZeroPrefetchConsumerIsAborted() throws Exception {
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

        try {
            consumer.receive(20000);
            fail("Slow consumer not aborted.");
        } catch(Exception ex) {
        }
    }

    public void testIdleConsumerCanBeAbortedNoMessages() throws Exception {
        AbortSlowAckConsumerStrategy strategy = (AbortSlowAckConsumerStrategy) underTest;
        strategy.setIgnoreIdleConsumers(false);

        ActiveMQConnection conn = (ActiveMQConnection) createConnectionFactory().createConnection();
        conn.setExceptionListener(this);
        connections.add(conn);

        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageConsumer consumer = sess.createConsumer(destination);
        assertNotNull(consumer);
        conn.start();
        startProducers(destination, 20);

        try {
            consumer.receive(20000);
            fail("Idle consumer not aborted.");
        } catch(Exception ex) {
        }
    }

    public void testIdleConsumerCanBeAborted() throws Exception {
        AbortSlowAckConsumerStrategy strategy = (AbortSlowAckConsumerStrategy) underTest;
        strategy.setIgnoreIdleConsumers(false);

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
        message.acknowledge();

        try {
            consumer.receive(20000);
            fail("Slow consumer not aborted.");
        } catch(Exception ex) {
        }
    }
}
