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

import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.util.Wait;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;


@RunWith(value = Parameterized.class)
public class DurableSubscriptionOffline2Test extends DurableSubscriptionOfflineTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionOffline2Test.class);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Boolean[]> getTestParameters() {
        Boolean[] f = {Boolean.FALSE};
        Boolean[] t = {Boolean.TRUE};
        List<Boolean[]> booleanChoices = new ArrayList<Boolean[]>();
        booleanChoices.add(f);
        booleanChoices.add(t);

        return booleanChoices;
    }

    public DurableSubscriptionOffline2Test(Boolean keepDurableSubsActive) {
        this.keepDurableSubsActive = keepDurableSubsActive.booleanValue();

        LOG.info(">>>> running {} with keepDurableSubsActive: {}", testName.getMethodName(), this.keepDurableSubsActive);
    }


    @Test(timeout = 60 * 1000)
    public void testJMXCountersWithOfflineSubs() throws Exception {
        // create durable subscription 1
        Connection con = createConnection("cliId1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", null, true);
        session.close();
        con.close();

        // restart broker
        broker.stop();
        createBroker(false /*deleteAllMessages*/);

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            producer.send(topic, message);
        }
        session.close();
        con.close();

        // consume some messages
        con = createConnection("cliId1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);

        for (int i=0; i<sent/2; i++) {
            Message m =  consumer.receive(4000);
            assertNotNull("got message: " + i, m);
            LOG.info("Got :" + i + ", " + m);
        }

        // check some counters while active
        ObjectName activeDurableSubName = broker.getAdminView().getDurableTopicSubscribers()[0];
        LOG.info("active durable sub name: " + activeDurableSubName);
        final DurableSubscriptionViewMBean durableSubscriptionView = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(activeDurableSubName, DurableSubscriptionViewMBean.class, true);

        assertTrue("is active", durableSubscriptionView.isActive());
        assertEquals("all enqueued", keepDurableSubsActive ? 10 : 0, durableSubscriptionView.getEnqueueCounter());
        assertTrue("correct waiting acks", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return 5 == durableSubscriptionView.getMessageCountAwaitingAcknowledge();
            }
        }));
        assertEquals("correct dequeue", 5, durableSubscriptionView.getDequeueCounter());


        ObjectName destinationName = broker.getAdminView().getTopics()[0];
        TopicViewMBean topicView = (TopicViewMBean) broker.getManagementContext().newProxyInstance(destinationName, TopicViewMBean.class, true);
        assertEquals("correct enqueue", 10, topicView.getEnqueueCount());
        assertEquals("topic view dequeue not updated", 5, topicView.getDequeueCount());
        assertEquals("inflight", 5, topicView.getInFlightCount());

        session.close();
        con.close();

        // check some counters when inactive
        ObjectName inActiveDurableSubName = broker.getAdminView().getInactiveDurableTopicSubscribers()[0];
        LOG.info("inactive durable sub name: " + inActiveDurableSubName);
        DurableSubscriptionViewMBean durableSubscriptionView1 = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(inActiveDurableSubName, DurableSubscriptionViewMBean.class, true);

        assertTrue("is not active", !durableSubscriptionView1.isActive());
        assertEquals("all enqueued", keepDurableSubsActive ? 10 : 0, durableSubscriptionView1.getEnqueueCounter());
        assertEquals("correct awaiting ack", 0, durableSubscriptionView1.getMessageCountAwaitingAcknowledge());
        assertEquals("correct dequeue", keepDurableSubsActive ? 5 : 0, durableSubscriptionView1.getDequeueCounter());

        // destination view
        assertEquals("correct enqueue", 10, topicView.getEnqueueCount());
        assertEquals("topic view dequeue not updated", 5, topicView.getDequeueCount());
        assertEquals("inflight back to 0 after deactivate", 0, topicView.getInFlightCount());

        // consume the rest
        con = createConnection("cliId1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(topic, "SubsId", null, true);

        for (int i=0; i<sent/2;i++) {
            Message m =  consumer.receive(30000);
            assertNotNull("got message: " + i, m);
            LOG.info("Got :" + i + ", " + m);
        }

        activeDurableSubName = broker.getAdminView().getDurableTopicSubscribers()[0];
        LOG.info("durable sub name: " + activeDurableSubName);
        final DurableSubscriptionViewMBean durableSubscriptionView2 = (DurableSubscriptionViewMBean)
                broker.getManagementContext().newProxyInstance(activeDurableSubName, DurableSubscriptionViewMBean.class, true);

        assertTrue("is active", durableSubscriptionView2.isActive());
        assertEquals("all enqueued", keepDurableSubsActive ? 10 : 0, durableSubscriptionView2.getEnqueueCounter());
        assertTrue("correct dequeue", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                long val = durableSubscriptionView2.getDequeueCounter();
                LOG.info("dequeue count:" + val);
                return 10 == val;
            }
        }));
    }
}
