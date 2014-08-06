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
package org.apache.activemq.broker.region;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.apache.activemq.util.Wait.Condition;

public class DestinationGCTest extends EmbeddedBrokerTestSupport {

    ActiveMQQueue queue = new ActiveMQQueue("TEST");
    ActiveMQQueue otherQueue = new ActiveMQQueue("TEST-OTHER");

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.setDestinations(new ActiveMQDestination[] {queue});
        broker.setSchedulePeriodForDestinationPurge(1000);
        broker.setMaxPurgedDestinationsPerSweep(1);
        PolicyEntry entry = new PolicyEntry();
        entry.setGcInactiveDestinations(true);
        entry.setInactiveTimoutBeforeGC(3000);
        PolicyMap map = new PolicyMap();
        map.setDefaultEntry(entry);
        broker.setDestinationPolicy(map);
        return broker;
    }

    public void testDestinationGCWithActiveConsumers() throws Exception {
        assertEquals(1, broker.getAdminView().getQueues().length);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createProducer(otherQueue).close();
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        });
        connection.start();

        TimeUnit.SECONDS.sleep(5);

        assertTrue("After GC runs there should be one Queue.", Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getAdminView().getQueues().length == 1;
            }
        }));

        connection.close();
    }

    public void testDestinationGc() throws Exception {
        assertEquals(1, broker.getAdminView().getQueues().length);
        assertTrue("After GC runs the Queue should be empty.", Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getAdminView().getQueues().length == 0;
            }
        }));
    }

    public void testDestinationGcLimit() throws Exception {

        broker.getAdminView().addQueue("TEST1");
        broker.getAdminView().addQueue("TEST2");
        broker.getAdminView().addQueue("TEST3");
        broker.getAdminView().addQueue("TEST4");

        assertEquals(5, broker.getAdminView().getQueues().length);
        Thread.sleep(7000);
        int queues = broker.getAdminView().getQueues().length;
        assertTrue(queues > 0 && queues < 5);
        assertTrue("After GC runs the Queue should be empty.", Wait.waitFor(new Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return broker.getAdminView().getQueues().length == 0;
            }
        }));
    }
}
