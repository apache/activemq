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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(ParallelTest.class)
public class AMQ9118Test {

    private BrokerService broker;
    private String connectionUri;
    private ActiveMQConnection consumerConnection;
    private ActiveMQConnection producerConnection;
    private ExecutorService executor;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistent(false);
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();
        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();

        consumerConnection = (ActiveMQConnection) new ActiveMQConnectionFactory("failover:(" + connectionUri + ")").createConnection();
        consumerConnection.getPrefetchPolicy().setAll(0);
        consumerConnection.start();

        producerConnection = (ActiveMQConnection) new ActiveMQConnectionFactory(connectionUri).createConnection();
        producerConnection.start();

        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
        }
        for (ActiveMQConnection connection : new ActiveMQConnection[] {consumerConnection, producerConnection}) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {}
            }
        }
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60000)
    public void testPullConsumerDoesNotHangOnSuppressedDuplicate() throws Exception {
        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = consumerSession.createTopic(name.getMethodName());
        final MessageConsumer consumer = consumerSession.createConsumer(topic);

        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(topic);

        TextMessage original = producerSession.createTextMessage("original");
        producer.send(original);
        ActiveMQMessage duplicate = (ActiveMQMessage) ((ActiveMQMessage) original).copy();
        duplicate.setProducerId(duplicate.getMessageId().getProducerId());
        producerConnection.syncSendPacket(duplicate);

        Message received = consumer.receive(5000);
        assertNotNull(received);
        assertEquals("original", ((TextMessage) received).getText());

        producer.send(producerSession.createTextMessage("next"));

        received = null;
        for (int i = 0; i < 5 && received == null; i++) {
            Future<Message> pull = executor.submit(() -> consumer.receive(1000));
            received = pull.get(10, TimeUnit.SECONDS);
        }
        assertNotNull("consumer should have received the next message", received);
        assertEquals("next", ((TextMessage) received).getText());
    }
}
