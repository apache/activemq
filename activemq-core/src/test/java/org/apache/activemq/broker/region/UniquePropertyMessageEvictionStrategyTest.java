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

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.UniquePropertyMessageEvictionStrategy;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;

public class UniquePropertyMessageEvictionStrategyTest extends EmbeddedBrokerTestSupport {



    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker =  super.createBroker();
                final List<PolicyEntry> policyEntries = new ArrayList<PolicyEntry>();
        final PolicyEntry entry = new PolicyEntry();
        entry.setTopic(">");

        entry.setAdvisoryForDiscardingMessages(true);
        entry.setTopicPrefetch(1);

        ConstantPendingMessageLimitStrategy pendingMessageLimitStrategy = new ConstantPendingMessageLimitStrategy();
        pendingMessageLimitStrategy.setLimit(10);
        entry.setPendingMessageLimitStrategy(pendingMessageLimitStrategy);


        UniquePropertyMessageEvictionStrategy messageEvictionStrategy = new UniquePropertyMessageEvictionStrategy();
        messageEvictionStrategy.setPropertyName("sequenceI");
        entry.setMessageEvictionStrategy(messageEvictionStrategy);

        // let evicted messages disappear
        entry.setDeadLetterStrategy(null);
        policyEntries.add(entry);

        final PolicyMap policyMap = new PolicyMap();
        policyMap.setPolicyEntries(policyEntries);
        broker.setDestinationPolicy(policyMap);

        return broker;
    }

    public void testEviction() throws Exception {
        Connection conn = connectionFactory.createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        javax.jms.Topic destination = session.createTopic("TEST");

        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destination);

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                TextMessage msg = session.createTextMessage("message " + i + j);
                msg.setIntProperty("sequenceI", i);
                msg.setIntProperty("sequenceJ", j);
                producer.send(msg);
                Thread.sleep(100);
            }
        }


        for (int i = 0; i < 11; i++) {
            javax.jms.Message msg = consumer.receive(1000);
            assertNotNull(msg);
            int seqI = msg.getIntProperty("sequenceI");
            int seqJ = msg.getIntProperty("sequenceJ");
            if (i ==0 ) {
                assertEquals(0, seqI);
                assertEquals(0, seqJ);
            } else {
                    assertEquals(9, seqJ);
                    assertEquals(i - 1, seqI);
            }
            //System.out.println(msg.getIntProperty("sequenceI") + " " + msg.getIntProperty("sequenceJ"));
        }

        javax.jms.Message msg = consumer.receive(1000);
        assertNull(msg);

    }

}
