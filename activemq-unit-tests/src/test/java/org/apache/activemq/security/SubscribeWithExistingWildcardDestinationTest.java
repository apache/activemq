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
package org.apache.activemq.security;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.jaas.GroupPrincipal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Queue;

import static org.junit.Assert.assertNotNull;

public class SubscribeWithExistingWildcardDestinationTest  {
    protected ActiveMQConnectionFactory factory;
    protected BrokerService broker;
    private static GroupPrincipal ADMINS = new GroupPrincipal("admins");
    private static GroupPrincipal USERS = new GroupPrincipal("users");

    @Before
    public void setUp() throws Exception {
        broker = BrokerFactory.createBroker(new URI("broker://()/localhost?persistent=false"));

        DestinationMap readAccess = new DefaultAuthorizationMap();
        readAccess.put(new ActiveMQTopic(">"), ADMINS);
        readAccess.put(new ActiveMQTopic("A.B"), USERS);

        DestinationMap writeAccess = new DefaultAuthorizationMap();
        writeAccess.put(new ActiveMQTopic(">"), ADMINS);

        DestinationMap adminAccess = new DefaultAuthorizationMap();
        adminAccess.put(new ActiveMQTopic(">"), ADMINS);

        List<AuthenticationUser> users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser("admin", "password", ADMINS.getName()));
        users.add(new AuthenticationUser("user", "password", USERS.getName()));

        broker.setPlugins(new BrokerPlugin[] {
            new AuthorizationPlugin(new SimpleAuthorizationMap(writeAccess, readAccess, adminAccess)),
            new SimpleAuthenticationPlugin(users)
        });
        broker.setAdvisorySupport(false);
        broker.start();
        factory = new ActiveMQConnectionFactory("vm://localhost");
        factory.setWatchTopicAdvisories(false);
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test
    public void testSubscribeWithExistingWildcardDestination() throws Exception {
        Connection userConnection = factory.createConnection("user", "password");
        Connection adminConnection = factory.createConnection("admin", "password");
        Session userSession = userConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session adminSession = adminConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        userConnection.start();
        adminConnection.start();

        MessageConsumer adminConsumer = adminSession.createConsumer(new ActiveMQTopic("A.>"));
        MessageProducer producer = adminSession.createProducer(new ActiveMQTopic("A.B"));
        producer.send(adminSession.createTextMessage("test"));
        producer = adminSession.createProducer(new ActiveMQTopic("A.>"));
        producer.send(adminSession.createTextMessage("test"));
        assertNotNull(adminConsumer.receive(5000));

        MessageConsumer userConsumer = userSession.createConsumer(new ActiveMQTopic("A.B"));
        producer.send(adminSession.createTextMessage("test"));
        assertNotNull(userConsumer.receive(5000));

        userSession.close();
        adminSession.close();
        userConnection.close();
        adminConnection.close();
    }
}
