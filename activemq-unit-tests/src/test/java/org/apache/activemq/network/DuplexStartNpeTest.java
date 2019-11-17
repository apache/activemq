/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.network;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.util.TestUtils;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertNotNull;

public class DuplexStartNpeTest {
    private static final Logger LOG = LoggerFactory.getLogger(DuplexStartNpeTest.class);
    final ActiveMQQueue dest = new ActiveMQQueue("QQ");
    final List<BrokerService> brokerServices = new ArrayList<>();
    final List<Connection> connections = new ArrayList();
    final static String urlString = "tcp://localhost:" + TestUtils.findOpenPort();
    final static int NUM_MESSAGES = 10;

    @Test
    public void reproduceNpe() throws Exception {
        BrokerService broker0 = createBroker();

        NetworkConnector networkConnector = broker0.addNetworkConnector("masterslave:(" + urlString + "," + urlString + ")");
        networkConnector.setDuplex(true);
        networkConnector.setStaticBridge(true);

        // ensure there is demand on start
        networkConnector.setStaticallyIncludedDestinations(Arrays.<ActiveMQDestination>asList(new ActiveMQQueue[]{dest}));


        broker0.start();

        publish(broker0.getVmConnectorURI());


        BrokerService broker1 = createBroker();
        broker1.addConnector(urlString);

        broker1.setPlugins(new BrokerPlugin[] {
                new BrokerPluginSupport() {
                    @Override
                    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
                        super.addConnection(context, info);

                        if (info.getClientId() != null  && info.getClientId().contains("_duplex_")) {
                            LOG.info("New connection for broker1: " + info);
                            // snooz on return to simulate stall
                            TimeUnit.MILLISECONDS.sleep(500);
                        }
                    }
                }
        });

        broker1.start();

        // get the message over the bridge
        consume(new URI(urlString));
    }

    private void consume(URI uri) throws Exception {
        MessageConsumer messageConsumer = connectionFactory(uri).createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(dest);
        for (int i=0; i<NUM_MESSAGES; i++) {
            assertNotNull("got message: " + i, messageConsumer.receive(5000));
        }
    }

    private void publish(URI uri) throws Exception {
        MessageProducer messageProducer = connectionFactory(uri).createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE).createProducer(dest);
        for (int i=0; i<NUM_MESSAGES; i++) {
            messageProducer.send(new ActiveMQTextMessage());
        }
    }


    private ActiveMQConnectionFactory connectionFactory(URI uri) {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(uri) {
            @Override
            public Connection createConnection() throws JMSException {
                Connection connection = super.createConnection();
                connections.add(connection);
                // auto start!
                connection.start();
                return connection;
            }
        };
        connectionFactory.setWatchTopicAdvisories(false);
        return connectionFactory;
    }

    private BrokerService createBroker() {
        BrokerService brokerService = new BrokerService();
        brokerService.setBrokerName("B" + brokerServices.size());
        brokerService.setBrokerId(brokerService.getBrokerName());
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerServices.add(brokerService);
        return  brokerService;
    }

    @After
    public void tearDown() throws Exception {
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (Exception ignored) {}
        }
        connections.clear();

        for (BrokerService brokerService : brokerServices) {
            try {
                brokerService.stop();
            } catch (Exception ignored) {}
        }
        brokerServices.clear();
    }

}
