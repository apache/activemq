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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.TopicImpl;
import org.junit.Test;

public class AMQ4696Test extends AmqpTestSupport {

    @Test(timeout=30*1000)
    public void simpleDurableTopicTest() throws Exception {
        String TOPIC_NAME = "topic://AMQ4696Test" + System.currentTimeMillis();
        String durableClientId = "AMQPDurableTopicTestClient";
        String durableSubscriberName = "durableSubscriberName";

        BrokerView adminView = this.brokerService.getAdminView();
        int durableSubscribersAtStart = adminView.getDurableTopicSubscribers().length;
        int inactiveSubscribersAtStart = adminView.getInactiveDurableTopicSubscribers().length;
        LOG.debug(">>>> At Start, durable Subscribers {} inactiveDurableSubscribers {}", durableSubscribersAtStart, inactiveSubscribersAtStart);

        TopicConnectionFactory factory = new ConnectionFactoryImpl("localhost", port, "admin", "password");
        Topic topic = new TopicImpl("topic://" + TOPIC_NAME);
        TopicConnection subscriberConnection = factory.createTopicConnection();
        subscriberConnection.setClientID(durableClientId);
        TopicSession subscriberSession = subscriberConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber messageConsumer = subscriberSession.createDurableSubscriber(topic, durableSubscriberName);

        assertNotNull(messageConsumer);

        int durableSubscribers = adminView.getDurableTopicSubscribers().length;
        int inactiveSubscribers = adminView.getInactiveDurableTopicSubscribers().length;
        LOG.debug(">>>> durable Subscribers after creation {} inactiveDurableSubscribers {}", durableSubscribers, inactiveSubscribers);
        assertEquals("Wrong number of durable subscribers after first subscription", 1, (durableSubscribers - durableSubscribersAtStart));
        assertEquals("Wrong number of inactive durable subscribers after first subscription", 0, (inactiveSubscribers - inactiveSubscribersAtStart));

        subscriberConnection.close();
        subscriberConnection = null;

        durableSubscribers = adminView.getDurableTopicSubscribers().length;
        inactiveSubscribers = adminView.getInactiveDurableTopicSubscribers().length;
        LOG.debug(">>>> durable Subscribers after close {} inactiveDurableSubscribers {}", durableSubscribers, inactiveSubscribers);
        assertEquals("Wrong number of durable subscribers after close", 0, (durableSubscribersAtStart));
        assertEquals("Wrong number of inactive durable subscribers after close", 1, (inactiveSubscribers - inactiveSubscribersAtStart));
    }
}
