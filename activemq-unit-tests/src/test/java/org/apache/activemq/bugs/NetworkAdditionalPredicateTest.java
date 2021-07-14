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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.junit.Assert;
import org.junit.Test;

import javax.jms.*;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NetworkAdditionalPredicateTest {

    private BrokerService createBroker(String brokerName, String uri, String networkConnectorUrl, String bridgeName) throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setPersistent(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getManagementContext().setCreateConnector(false);
        broker.setPlugins(new BrokerPlugin[] {
                getAuthenticationPlugin(),
                getAuthorizationPlugin(),
                new PredicatePlugin()
        });

        broker.setUseJmx(true);
        broker.setBrokerName(brokerName);
        broker.addConnector(new URI(uri));

        addNetworkConnector(broker);
        broker.setSchedulePeriodForDestinationPurge(0);
        broker.getSystemUsage().setSendFailIfNoSpace(true);
        broker.getSystemUsage().getMemoryUsage().setLimit(512 * 1024 * 1024);

        KahaDBPersistenceAdapter kahaDBPersistenceAdapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        kahaDBPersistenceAdapter.setConcurrentStoreAndDispatchQueues(false);

        NetworkConnector nc = new DiscoveryNetworkConnector(new URI(networkConnectorUrl));
        nc.setName(bridgeName);
        nc.setNetworkTTL(1);
        nc.setDecreaseNetworkConsumerPriority(true);
        nc.setDynamicOnly(true);
        nc.setPrefetchSize(1000);
        nc.setUserName("bridge");
        nc.setPassword("bridge");
        broker.addNetworkConnector(nc);

        return broker;
    }

    private BrokerPlugin getAuthorizationPlugin() throws Exception {
        final DefaultAuthorizationMap map = new DefaultAuthorizationMap();
        final AuthorizationEntry queueAuthEntry = new AuthorizationEntry();
        queueAuthEntry.setQueue(">");
        queueAuthEntry.setAdmin("admins");
        queueAuthEntry.setRead("admins");
        queueAuthEntry.setWrite("admins");
        final AuthorizationEntry topicAuthEntry = new AuthorizationEntry();
        topicAuthEntry.setTopic(">");
        topicAuthEntry.setAdmin("admins");
        topicAuthEntry.setRead("admins");
        topicAuthEntry.setWrite("admins");
        map.setAuthorizationEntries(Arrays.asList(queueAuthEntry, topicAuthEntry));

        return new AuthorizationPlugin(map);
    }

    private BrokerPlugin getAuthenticationPlugin() {
        final AuthenticationUser user1 = new AuthenticationUser("user1", "user1", "admins");
        final AuthenticationUser user2 = new AuthenticationUser("user2", "user2", "admins");
        final AuthenticationUser user3 = new AuthenticationUser("user3", "user3", "admins");
        final AuthenticationUser bridge = new AuthenticationUser("bridge", "bridge", "admins");

        return new SimpleAuthenticationPlugin(Arrays.asList(user1, user2, user3, bridge));
    }

    private void addNetworkConnector(BrokerService broker) throws Exception {
    }

    @Test
    public void testSubscribeAcrossNetworkWithAdditionalPredicate() throws Exception {
        final BrokerService broker1 = createBroker("B1", "tcp://localhost:61601", "static:(tcp://localhost:61602)", "bridge1");
        final BrokerService broker2 = createBroker("B2", "tcp://localhost:61602", "static:(tcp://localhost:61601)", "bridge2");

        broker1.start();
        broker2.start();

        // send messages - targetUser1 and targetUser2 - all messages to broker1

        sendMessages();

        // consume messages - target user1 from broker1, user2 from broker2

        final CountDownLatch latch = new CountDownLatch(2000);

        consumeMessages("tcp://localhost:61601", "user1", "user1", latch);
        consumeMessages("tcp://localhost:61602", "user2", "user2", latch);

        latch.await(20, TimeUnit.MINUTES);

        broker1.stop();
        broker2.stop();
    }

    private void consumeMessages(final String brokerUri, final String username, final String password, final CountDownLatch latch) throws Exception {
        final Runnable r = new Runnable() {

            @Override
            public void run() {
                try {
                    final ActiveMQConnectionFactory cf1 = new ActiveMQConnectionFactory(brokerUri);
                    final Connection connection = cf1.createConnection(username, password);
                    connection.start();
                    final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    final MessageConsumer consumer = session.createConsumer(new ActiveMQQueue("TESTQUEUE"));
                    consumer.setMessageListener(new MessageListener() {
                        @Override
                        public void onMessage(Message message) {
                            try {
                                System.out.println("Received ");
                                Assert.assertNotNull(message);
                                Assert.assertEquals(username, message.getStringProperty("targetUser"));
                                latch.countDown();
                            } catch (JMSException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        final Thread t = new Thread(r);
        t.setDaemon(true); // don't block shutdown
        t.start();
    }

    private void sendMessages() throws Exception {
        final ActiveMQConnectionFactory cf1 = new ActiveMQConnectionFactory("tcp://localhost:61601");
        final Connection connection = cf1.createConnection("user3", "user3");
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageProducer producer = session.createProducer(new ActiveMQQueue("TESTQUEUE"));

        final List<String> targetUsers = Arrays.asList("user1", "user2");

        for (int i = 0; i < 1000; i++) {
            for (String targetUser : targetUsers) {
                final TextMessage textMessage = session.createTextMessage("Test message: " + i);
                textMessage.setStringProperty("targetUser", targetUser);
                producer.send(textMessage);
            }
        }

        producer.close();
        session.close();
        connection.stop();
        connection.close();
    }


    private static class PredicatePlugin implements BrokerPlugin {
        @Override
        public Broker installPlugin(Broker broker) throws Exception {
            return new PredicateBroker(broker);
        }
    }

    private static class PredicateBroker extends BrokerFilter {
        public PredicateBroker(final Broker next) {
            super(next);
        }

        @Override
        public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
            if ("TESTQUEUE".equals(info.getDestination().getPhysicalName())
                && (!"bridge".equals(context.getUserName()))) { // don't filter out messages over the network bridge
                info.setAdditionalPredicate(SelectorParser.parse("targetUser='" + context.getUserName() + "'"));
            }

            return super.addConsumer(context, info);
        }
    }
}
