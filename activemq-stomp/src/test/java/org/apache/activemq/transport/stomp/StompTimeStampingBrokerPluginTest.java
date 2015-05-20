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
package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.util.TimeStampingBrokerPlugin;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompTimeStampingBrokerPluginTest extends StompTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(StompTimeStampingBrokerPluginTest.class);

    private Connection connection;
    private Session session;

    @Override
    protected void addAdditionalPlugins(List<BrokerPlugin> plugins) throws Exception {
        plugins.add(new TimeStampingBrokerPlugin());
    }

    @Override
    protected void applyBrokerPolicies() throws Exception {
        // Add policy and individual DLQ strategy
        PolicyEntry policy = new PolicyEntry();
        DeadLetterStrategy strategy = new IndividualDeadLetterStrategy();
        strategy.setProcessExpired(true);
        ((IndividualDeadLetterStrategy)strategy).setUseQueueForQueueMessages(true);
        ((IndividualDeadLetterStrategy)strategy).setQueuePrefix("DLQ.");
        strategy.setProcessNonPersistent(true);
        policy.setDeadLetterStrategy(strategy);

        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        brokerService.setDestinationPolicy(pMap);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        stompConnect();

        connection = cf.createConnection("system", "manager");
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();
    }

    @Test(timeout = 60000)
    public void testSendMessage() throws Exception {

        Destination destination = session.createQueue(getQueueName());
        MessageConsumer consumer = session.createConsumer(destination);

        String frame = "CONNECT\n" + "login:system\n" + "passcode:manager\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        long timestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
        long expires = timestamp + TimeUnit.SECONDS.toMillis(5);

        LOG.info("Message timestamp = {}, expires = {}", timestamp, expires);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" +
                "timestamp:" + timestamp + "\n" +
                "expires:" + expires +
                "\n\n" + "Hello World 1" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        frame = "SEND\n" + "destination:/queue/" + getQueueName() + "\n" +
            "timestamp:" + timestamp + "\n" +
            "expires:" + expires +
            "\n\n" + "Hello World 2" + Stomp.NULL;

        stompConnection.sendFrame(frame);

        TextMessage message = (TextMessage)consumer.receive(2500);
        assertNotNull(message);

        TimeUnit.SECONDS.sleep(10);

        message = (TextMessage)consumer.receive(2500);
        assertNull(message);
    }
}
