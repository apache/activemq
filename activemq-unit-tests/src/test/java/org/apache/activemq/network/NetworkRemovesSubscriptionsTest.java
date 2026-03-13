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

import java.net.URI;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Session;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicSession;
import jakarta.jms.TopicSubscriber;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.Wait;

/**
 * Various Tests to show the memory leak suspect in network of brokers. This is
 * for https://issues.apache.org/activemq/browse/AMQ-2530
 *
 */
public class NetworkRemovesSubscriptionsTest extends TestCase {
    private static final String TOPIC_NAME = "TEST_TOPIC";
    private final ActiveMQTopic topic = new ActiveMQTopic(TOPIC_NAME);
    private BrokerService frontEnd;
    private BrokerService backEnd;
    private ActiveMQConnectionFactory connectionFactory;

    public void testWithSessionAndSubsciberClose() throws Exception {

        final TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();

        for (int i = 0; i < 100; i++) {
            final TopicSession subscriberSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            final TopicSubscriber subscriber = subscriberSession.createSubscriber(topic);
            subscriber.setMessageListener(msg -> { });
            subscriber.close();
            subscriberSession.close();
        }
        connection.close();
        assertBackEndConsumersEmpty();
    }

    public void testWithSessionCloseOutsideTheLoop() throws Exception {

        final TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();
        final TopicSession subscriberSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        for (int i = 0; i < 100; i++) {

            final TopicSubscriber subscriber = subscriberSession.createSubscriber(topic);
            subscriber.setMessageListener(msg -> { });
            subscriber.close();
        }
        subscriberSession.close();
        connection.close();
        assertBackEndConsumersEmpty();

    }

    public void testWithOneSubscriber() throws Exception {

        final TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();
        final TopicSession subscriberSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        final TopicSubscriber subscriber = subscriberSession.createSubscriber(topic);
        subscriber.setMessageListener(msg -> { });
        subscriber.close();
        subscriberSession.close();
        connection.close();
        assertBackEndConsumersEmpty();
    }

    public void testWithoutSessionAndSubsciberClose() throws Exception {

        final TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();

        for (int i = 0; i < 100; i++) {
            final TopicSession subscriberSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            final TopicSubscriber subscriber = subscriberSession.createSubscriber(topic);
            assertNotNull(subscriber);
        }

        connection.close();
        assertBackEndConsumersEmpty();
    }

    /**
     * Running this test you can produce a leak of only 2 ConsumerInfo on BE
     * broker, NOT 200 as in other cases!
     *
     */
    public void testWithoutSessionAndSubsciberClosePlayAround() throws Exception {

        final TopicConnection connection = connectionFactory.createTopicConnection();
        connection.start();

        for (int i = 0; i < 100; i++) {
            final TopicSession subscriberSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            final TopicSubscriber subscriber = subscriberSession.createSubscriber(topic);
            subscriber.setMessageListener(msg -> { });
            if (i != 50) {
                subscriber.close();
                subscriberSession.close();
            }
        }

        connection.close();
        assertBackEndConsumersEmpty();
    }

    private void assertBackEndConsumersEmpty() throws Exception {
        assertTrue("backEnd consumers should be empty", Wait.waitFor(() -> {
            final Destination dest = backEnd.getRegionBroker().getDestinationMap().get(topic);
            return dest != null && dest.getConsumers().isEmpty();
        }, TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(100)));
    }

    @Override
    protected void setUp() throws Exception {
        this.backEnd = new BrokerService();
        this.backEnd.setBrokerName("backEnd");
        this.backEnd.setPersistent(false);
        final TransportConnector backEndConnector = this.backEnd.addConnector("tcp://localhost:0");
        this.backEnd.start();
        this.backEnd.waitUntilStarted();

        final URI backEndConnectURI = backEndConnector.getConnectUri();

        this.frontEnd = new BrokerService();
        this.frontEnd.setBrokerName("frontEnd");
        this.frontEnd.setPersistent(false);
        final TransportConnector frontEndConnector = this.frontEnd.addConnector("tcp://localhost:0");
        this.frontEnd.start();
        this.frontEnd.waitUntilStarted();

        final URI frontEndConnectURI = frontEndConnector.getConnectUri();

        // Add network connectors using actual assigned ephemeral ports
        final NetworkConnector backEndNetwork = this.backEnd.addNetworkConnector("static://" + frontEndConnectURI);
        backEndNetwork.setName("backEndNetwork");
        backEndNetwork.setDynamicOnly(true);
        backEndNetwork.start();

        final NetworkConnector frontEndNetwork = this.frontEnd.addNetworkConnector("static://" + backEndConnectURI);
        frontEndNetwork.setName("frontEndNetwork");
        frontEndNetwork.start();

        // Wait for both network bridges to be fully started
        assertTrue("backEnd bridge should start", Wait.waitFor(() ->
            !this.backEnd.getNetworkConnectors().isEmpty()
                && !this.backEnd.getNetworkConnectors().get(0).activeBridges().isEmpty(),
            TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(100)));

        assertTrue("frontEnd bridge should start", Wait.waitFor(() ->
            !this.frontEnd.getNetworkConnectors().isEmpty()
                && !this.frontEnd.getNetworkConnectors().get(0).activeBridges().isEmpty(),
            TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS.toMillis(100)));

        this.connectionFactory = new ActiveMQConnectionFactory(frontEndConnectURI);
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
