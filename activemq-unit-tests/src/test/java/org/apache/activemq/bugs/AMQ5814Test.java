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

import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ5814Test {

    private BrokerService brokerService;
    private String openwireClientUrl;

    public BrokerPlugin configureAuthentication() throws Exception {
        List<AuthenticationUser> users = new ArrayList<>();
        users.add(new AuthenticationUser("publisher", "123", "publisher"));
        users.add(new AuthenticationUser("subscriber", "123", "subscriber"));
        users.add(new AuthenticationUser("admin", "123", "publisher,subscriber"));

        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);

        return authenticationPlugin;
    }

    public BrokerPlugin configureAuthorization() throws Exception {

        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> authorizationEntries = new ArrayList<>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setTopic("dcu.>");
        entry.setRead("subscriber");
        entry.setWrite("publisher");
        entry.setAdmin("publisher,subscriber");
        authorizationEntries.add(entry);

        entry = new AuthorizationEntry();
        entry.setTopic("ActiveMQ.Advisory.>");
        entry.setRead("publisher,subscriber");
        entry.setWrite("publisher,subscriber");
        entry.setAdmin("publisher,subscriber");
        authorizationEntries.add(entry);

        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap(authorizationEntries);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(authorizationMap);

        return authorizationPlugin;
    }

    @Before
    public void setup() throws Exception {

        TransportConnector openwireConnector = new TransportConnector();
        openwireConnector.setUri(new URI("tcp://localhost:0"));
        openwireConnector.setName("openwire");

        ArrayList<BrokerPlugin> plugins = new ArrayList<>();
        plugins.add(configureAuthentication());
        plugins.add(configureAuthorization());

        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.addConnector(openwireConnector);
        if (!plugins.isEmpty()) {
            BrokerPlugin[] array = new BrokerPlugin[plugins.size()];
            brokerService.setPlugins(plugins.toArray(array));
        }
        brokerService.start();
        brokerService.waitUntilStarted();

        openwireClientUrl = openwireConnector.getPublishableConnectString();
    }

    @After
    public void shutdown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
            brokerService = null;
        }
    }

    @Test(timeout=30000)
    public void testProduceConsumeWithAuthorization() throws Exception {
        ConnectionFactory factory = new ActiveMQConnectionFactory(openwireClientUrl);
        Connection connection1 = factory.createConnection("subscriber", "123");
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic wildCarded = session1.createTopic("dcu.>");
        MessageConsumer consumer = session1.createConsumer(wildCarded);
        connection1.start();

        Connection connection2 = factory.createConnection("publisher", "123");
        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic named = session2.createTopic("dcu.id");
        MessageProducer producer = session2.createProducer(named);
        producer.send(session2.createTextMessage("test"));

        assertNotNull(consumer.receive(2000));

        connection1.close();
        connection2.close();
    }
}
