/*
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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.TopicSubscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that clearUnMarshalled data gets called properly to reduce memory usage
 */
@RunWith(Parameterized.class)
public class AMQ6477Test {

    static final Logger LOG = LoggerFactory.getLogger(AMQ6477Test.class);

    @Rule
    public TemporaryFolder dataDir = new TemporaryFolder();

    private BrokerService brokerService;
    private String connectionUri;
    private final ActiveMQQueue queue = new ActiveMQQueue("queue");
    private final ActiveMQTopic topic = new ActiveMQTopic("topic");
    private final int numMessages = 10;
    private Connection connection;
    private Session session;
    private SubType subType;
    private boolean persistent;

    protected enum SubType {QUEUE, TOPIC, DURABLE};

    @Parameters(name="subType={0},isPersistent={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {SubType.QUEUE, false},
                {SubType.TOPIC, false},
                {SubType.DURABLE, false},
                {SubType.QUEUE, true},
                {SubType.TOPIC, true},
                {SubType.DURABLE, true}
            });
    }

    /**
     */
    public AMQ6477Test(SubType subType, boolean persistent) {
        super();
        this.subType = subType;
        this.persistent = persistent;
    }

    @Before
    public void before() throws Exception {
        brokerService = new BrokerService();
        TransportConnector connector = brokerService.addConnector("tcp://localhost:0");
        connectionUri = connector.getPublishableConnectString();
        brokerService.setPersistent(persistent);
        brokerService.setDataDirectory(dataDir.getRoot().getAbsolutePath());
        brokerService.setUseJmx(false);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setReduceMemoryFootprint(true);

        policyMap.setDefaultEntry(entry);
        brokerService.setDestinationPolicy(policyMap);

        brokerService.start();
        brokerService.waitUntilStarted();

        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        connection = factory.createConnection();
        connection.setClientID("clientId");
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            connection.stop();
        }
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Test(timeout=30000)
    public void testReduceMemoryFootprint() throws Exception {

        ActiveMQDestination destination = subType.equals(SubType.QUEUE) ? queue : topic;

        MessageConsumer consumer = subType.equals(SubType.DURABLE) ?
                session.createDurableSubscriber(topic, "sub1") : session.createConsumer(destination);
        MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < numMessages; i++) {
            TextMessage m = session.createTextMessage("test");
            m.setStringProperty("test", "test");
            producer.send(m);
        }

        Subscription sub = brokerService.getDestination(destination).getConsumers().get(0);
        List<MessageReference> messages = getSubscriptionMessages(sub);

        //Go through each message and make sure the unmarshalled fields are null
        //then call the getters which will unmarshall the data again to show the marshalled
        //data exists
        for (MessageReference ref : messages) {
            ActiveMQTextMessage message = (ActiveMQTextMessage) ref.getMessage();
            Field propertiesField = Message.class.getDeclaredField("properties");
            propertiesField.setAccessible(true);
            Field textField = ActiveMQTextMessage.class.getDeclaredField("text");
            textField.setAccessible(true);

            assertNull(textField.get(message));
            assertNull(propertiesField.get(message));
            assertNotNull(message.getProperties());
            assertNotNull(message.getText());
        }
        consumer.close();
    }


    @SuppressWarnings("unchecked")
    protected List<MessageReference> getSubscriptionMessages(Subscription sub) throws Exception {
        Field dispatchedField = null;
        Field dispatchLockField = null;

        if (sub instanceof TopicSubscription) {
            dispatchedField = TopicSubscription.class.getDeclaredField("dispatched");
            dispatchLockField = TopicSubscription.class.getDeclaredField("dispatchLock");
        } else {
            dispatchedField = PrefetchSubscription.class.getDeclaredField("dispatched");
            dispatchLockField = PrefetchSubscription.class.getDeclaredField("dispatchLock");
        }
        dispatchedField.setAccessible(true);
        dispatchLockField.setAccessible(true);

        synchronized (dispatchLockField.get(sub)) {
            return new ArrayList<MessageReference>((List<MessageReference>)dispatchedField.get(sub));
        }
    }

}