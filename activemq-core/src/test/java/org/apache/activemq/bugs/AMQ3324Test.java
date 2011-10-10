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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.MirroredQueue;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ3324Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ3324Test.class);

    private static final String bindAddress = "tcp://0.0.0.0:0";
    private BrokerService broker;
    private ActiveMQConnectionFactory cf;

    private static final int MESSAGE_COUNT = 100;

    @Before
    public void setUp() throws Exception {
        broker = this.createBroker();
        String address = broker.getTransportConnectors().get(0).getPublishableConnectString();
        broker.start();
        broker.waitUntilStarted();

        cf = new ActiveMQConnectionFactory(address);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testTempMessageConsumedAdvisoryConnectionClose() throws Exception {

        Connection connection = cf.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final TemporaryQueue queue = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(queue);

        final Topic advisoryTopic = AdvisorySupport.getMessageConsumedAdvisoryTopic((ActiveMQDestination) queue);

        MessageConsumer advisoryConsumer = session.createConsumer(advisoryTopic);
        MessageProducer producer = session.createProducer(queue);

        // send lots of messages to the tempQueue
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            BytesMessage m = session.createBytesMessage();
            m.writeBytes(new byte[1024]);
            producer.send(m);
        }

        // consume one message from tempQueue
        Message msg = consumer.receive(5000);
        assertNotNull(msg);

        // check one advisory message has produced on the advisoryTopic
        Message advCmsg = advisoryConsumer.receive(5000);
        assertNotNull(advCmsg);

        connection.close();
        LOG.debug("Connection closed, destinations should now become inactive.");

        assertTrue("The destination " + advisoryTopic + "was not removed. ", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getAdminView().getTopics().length == 0;
            }
        }));

        assertTrue("The destination " + queue + " was not removed. ", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getAdminView().getTemporaryQueues().length == 0;
            }
        }));
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseMirroredQueues(true);
        answer.setPersistent(false);
        answer.setSchedulePeriodForDestinationPurge(1000);

        PolicyEntry entry = new PolicyEntry();
        entry.setGcInactiveDestinations(true);
        entry.setInactiveTimoutBeforeGC(2000);
        entry.setProducerFlowControl(true);
        entry.setAdvisoryForConsumed(true);
        entry.setAdvisdoryForFastProducers(true);
        entry.setAdvisoryForDelivery(true);
        PolicyMap map = new PolicyMap();
        map.setDefaultEntry(entry);

        MirroredQueue mirrorQ = new MirroredQueue();
        mirrorQ.setCopyMessage(true);
        DestinationInterceptor[] destinationInterceptors = new DestinationInterceptor[]{mirrorQ};
        answer.setDestinationInterceptors(destinationInterceptors);

        answer.setDestinationPolicy(map);
        answer.addConnector(bindAddress);

        return answer;
    }
}
