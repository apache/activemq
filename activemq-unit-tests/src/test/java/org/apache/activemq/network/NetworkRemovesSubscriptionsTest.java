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
package org.apache.activemq.network;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQTopic;

/**
 * Various Tests to show the memory leak suspect in network of brokers. This is
 * for https://issues.apache.org/activemq/browse/AMQ-2530
 *
 */
public class NetworkRemovesSubscriptionsTest extends TestCase {
    private final static String frontEndAddress = "tcp://0.0.0.0:61617";
    private final static String backEndAddress = "tcp://0.0.0.0:61616";
    private final static String TOPIC_NAME = "TEST_TOPIC";
    private BrokerService frontEnd;
    private BrokerService backEnd;
    private final ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(frontEndAddress);
    private final ActiveMQTopic topic = new ActiveMQTopic(TOPIC_NAME);

    public void testWithSessionAndSubsciberClose() throws Exception {

        TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();

        for (int i = 0; i < 100; i++) {
            TopicSession subscriberSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = subscriberSession.createSubscriber(topic);
            DummyMessageListener listener = new DummyMessageListener();
            subscriber.setMessageListener(listener);
            subscriber.close();
            subscriberSession.close();
        }
        connection.close();
        Thread.sleep(1000);
        Destination dest = backEnd.getRegionBroker().getDestinationMap().get(topic);
        assertNotNull(dest);
        assertTrue(dest.getConsumers().isEmpty());
    }

    public void testWithSessionCloseOutsideTheLoop() throws Exception {

        TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();
        TopicSession subscriberSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        for (int i = 0; i < 100; i++) {

            TopicSubscriber subscriber = subscriberSession.createSubscriber(topic);
            DummyMessageListener listener = new DummyMessageListener();
            subscriber.setMessageListener(listener);
            subscriber.close();
        }
        subscriberSession.close();
        connection.close();
        Thread.sleep(1000);
        Destination dest = backEnd.getRegionBroker().getDestinationMap().get(topic);
        assertNotNull(dest);
        assertTrue(dest.getConsumers().isEmpty());

    }

    public void testWithOneSubscriber() throws Exception {

        TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();
        TopicSession subscriberSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        TopicSubscriber subscriber = subscriberSession.createSubscriber(topic);
        DummyMessageListener listener = new DummyMessageListener();
        subscriber.setMessageListener(listener);
        subscriber.close();
        subscriberSession.close();
        connection.close();
        Thread.sleep(1000);
        Destination dest = backEnd.getRegionBroker().getDestinationMap().get(topic);
        assertNotNull(dest);
        assertTrue(dest.getConsumers().isEmpty());
    }

    public void testWithoutSessionAndSubsciberClose() throws Exception {

        TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();

        for (int i = 0; i < 100; i++) {
            TopicSession subscriberSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = subscriberSession.createSubscriber(topic);
            assertNotNull(subscriber);
        }

        connection.close();
        Thread.sleep(1000);
        Destination dest = backEnd.getRegionBroker().getDestinationMap().get(topic);
        assertNotNull(dest);
        assertTrue(dest.getConsumers().isEmpty());
    }

    /**
     * Running this test you can produce a leak of only 2 ConsumerInfo on BE
     * broker, NOT 200 as in other cases!
     *
     */
    public void testWithoutSessionAndSubsciberClosePlayAround() throws Exception {

        TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();

        for (int i = 0; i < 100; i++) {
            TopicSession subscriberSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = subscriberSession.createSubscriber(topic);
            DummyMessageListener listener = new DummyMessageListener();
            subscriber.setMessageListener(listener);
            if (i != 50) {
                subscriber.close();
                subscriberSession.close();
            }
        }

        connection.close();
        Thread.sleep(1000);
        Destination dest = backEnd.getRegionBroker().getDestinationMap().get(topic);
        assertNotNull(dest);
        assertTrue(dest.getConsumers().isEmpty());
    }

    class DummyMessageListener implements MessageListener {

        @Override
        public void onMessage(Message arg0) {
            // TODO Auto-generated method stub

        }
    }

    @Override
    protected void setUp() throws Exception {
        this.backEnd = new BrokerService();
        this.backEnd.setBrokerName("backEnd");
        this.backEnd.setPersistent(false);
        NetworkConnector backEndNetwork = this.backEnd.addNetworkConnector("static://" + frontEndAddress);
        backEndNetwork.setName("backEndNetwork");
        backEndNetwork.setDynamicOnly(true);
        this.backEnd.addConnector(backEndAddress);
        this.backEnd.start();

        this.frontEnd = new BrokerService();
        this.frontEnd.setBrokerName("frontEnd");
        this.frontEnd.setPersistent(false);
        NetworkConnector frontEndNetwork = this.frontEnd.addNetworkConnector("static://" + backEndAddress);
        frontEndNetwork.setName("frontEndNetwork");
        this.frontEnd.addConnector(frontEndAddress);
        this.frontEnd.start();
        Thread.sleep(2000);
    }

    @Override
    protected void tearDown() throws Exception {
        if (this.backEnd != null) {
            this.backEnd.stop();
        }
        if (this.frontEnd != null) {
            this.frontEnd.stop();
        }
    }

}
