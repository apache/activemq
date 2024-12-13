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
package org.apache.activemq.broker.jmx;

import jakarta.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BrokerViewTest {
    @Test(timeout=120000)
    public void testBrokerViewRetrieveQueuesAndTopicsCount() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.start();

        ActiveMQConnectionFactory factory =  new ActiveMQConnectionFactory(BrokerRegistry.getInstance().findFirst().getVmConnectorURI());
        Connection producerConnection = factory.createConnection();
        producerConnection.start();
        // Create non-suppressed queue
        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = producerSession.createQueue("testQueue");
        MessageProducer producer = producerSession.createProducer(queue);
        producer.send(producerSession.createTextMessage("testMessage"));
        // Create temporary queue
        Session tempProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue tempQueue = tempProducerSession.createTemporaryQueue();
        MessageProducer tempProducer = tempProducerSession.createProducer(tempQueue);
        tempProducer.send(tempProducerSession.createTextMessage("testMessage"));
        // Create non-suppressed topic
        Session topicProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = topicProducerSession.createTopic("testTopic");
        MessageProducer topicProducer = topicProducerSession.createProducer(topic);
        topicProducer.send(topicProducerSession.createTextMessage("testMessage"));
        // Create temporary topic
        Session tempTopicProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic tempTopic = tempTopicProducerSession.createTemporaryTopic();
        MessageProducer tempTopicProducer = tempTopicProducerSession.createProducer(tempTopic);
        tempTopicProducer.send(tempTopicProducerSession.createTextMessage("testMessage"));

        assertTrue(Wait.waitFor(() -> (brokerService.getAdminView()) != null));
        final BrokerView view = brokerService.getAdminView();
        assert(view.getTotalTopicsCount() == 11); // Including advisory topics
        assert(view.getTotalNonSuppressedTopicsCount() == 11);
        assert(view.getTotalTemporaryTopicsCount() == 1);
        assert(view.getTotalQueuesCount() == 1);
        assert(view.getTotalNonSuppressedQueuesCount() == 1);
        assert(view.getTotalTemporaryQueuesCount() == 1);
    }
}
