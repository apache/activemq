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

package org.apache.activemq.transport.fanout;

import jakarta.jms.Connection;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.MessageIdList;

public class FanoutTest extends TestCase {

    BrokerService broker1;
    BrokerService broker2;

    Connection producerConnection;
    Session producerSession;
    final int messageCount = 100;

    public void setUp() throws Exception {
        broker1 = new BrokerService();
        broker1.setBrokerName("brokerA");
        broker1.setPersistent(false);
        broker1.setUseJmx(false);
        broker1.addConnector("tcp://localhost:0");
        broker1.start();
        broker1.waitUntilStarted();

        broker2 = new BrokerService();
        broker2.setBrokerName("brokerB");
        broker2.setPersistent(false);
        broker2.setUseJmx(false);
        broker2.addConnector("tcp://localhost:0");
        broker2.start();
        broker2.waitUntilStarted();

        final String broker1Uri = broker1.getTransportConnectors().get(0).getConnectUri().toString();
        final String broker2Uri = broker2.getTransportConnectors().get(0).getConnectUri().toString();

        final ActiveMQConnectionFactory producerFactory = new ActiveMQConnectionFactory(
                "fanout:(static:(" + broker1Uri + "," + broker2Uri + "))?fanOutQueues=true");
        producerConnection = producerFactory.createConnection();
        producerConnection.start();
        producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void tearDown() throws Exception {
        producerSession.close();
        producerConnection.close();

        broker1.stop();
        broker2.stop();
    }

    public void testSendReceive() throws Exception {

        final MessageProducer prod = createProducer();
        for (int i = 0; i < messageCount; i++) {
            prod.send(producerSession.createTextMessage("Message " + i));
        }
        prod.close();

        final String broker1Uri = broker1.getTransportConnectors().get(0).getConnectUri().toString();
        final String broker2Uri = broker2.getTransportConnectors().get(0).getConnectUri().toString();
        assertMessagesReceived(broker1Uri);
        assertMessagesReceived(broker2Uri);
    }

    protected MessageProducer createProducer() throws Exception {
        return producerSession.createProducer(producerSession.createQueue("TEST"));
    }

    protected void assertMessagesReceived(final String brokerURL) throws Exception {
        final ActiveMQConnectionFactory consumerFactory = new ActiveMQConnectionFactory(brokerURL);
        final Connection consumerConnection = consumerFactory.createConnection();
        consumerConnection.start();
        final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageConsumer consumer = consumerSession.createConsumer(consumerSession.createQueue("TEST"));
        final MessageIdList listener = new MessageIdList();
        consumer.setMessageListener(listener);
        listener.waitForMessagesToArrive(messageCount);
        listener.assertMessagesReceived(messageCount);

        consumer.close();
        consumerConnection.close();
        consumerSession.close();
    }
}
