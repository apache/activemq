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
package org.apache.activemq.transport.failover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverClusterTestSupport extends TestCase {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int NUMBER_OF_CLIENTS = 30;

    private String clientUrl;

    private final Map<String, BrokerService> brokers = new HashMap<String, BrokerService>();
    private final List<ActiveMQConnection> connections = new ArrayList<ActiveMQConnection>();

    protected void assertClientsConnectedToTwoBrokers() throws Exception {
        assertClientsConnectedToXBrokers(2);
    }

    protected void assertClientsConnectedToThreeBrokers() throws Exception {
        assertClientsConnectedToXBrokers(3);
    }

    protected void assertClientsConnectedToXBrokers(final int x) throws Exception {
        final Set<String> set = new HashSet<String>();
        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                set.clear();
                for (ActiveMQConnection c : connections) {
                    if (c.getTransportChannel().getRemoteAddress() != null) {
                        set.add(c.getTransportChannel().getRemoteAddress());
                    }
                }
                return set.size() == x;
            }
        });

        assertTrue("Only " + x + " connections should be found: " + set,
                set.size() == x);
    }

    protected void assertClientsConnectionsEvenlyDistributed(double minimumPercentage) {
        Map<String, Double> clientConnectionCounts = new HashMap<String, Double>();
        int total = 0;
        for (ActiveMQConnection c : connections) {
            String key = c.getTransportChannel().getRemoteAddress();
            if (key != null) {
                total++;
                if (clientConnectionCounts.containsKey(key)) {
                    double count = clientConnectionCounts.get(key);
                    count += 1.0;
                    clientConnectionCounts.put(key, count);
                } else {
                    clientConnectionCounts.put(key, 1.0);
                }
            }
        }
        Set<String> keys = clientConnectionCounts.keySet();
        for(String key: keys){
            double count = clientConnectionCounts.get(key);
            double percentage = count / total;
            logger.info(count + " of " + total + " connections for " + key + " = " + percentage);
            assertTrue("Connections distribution expected to be >= than " + minimumPercentage
                    + ".  Actuall distribution was " + percentage + " for connection " + key,
                    percentage >= minimumPercentage);
        }
    }

    protected void assertAllConnectedTo(String url) throws Exception {
        for (ActiveMQConnection c : connections) {
            assertEquals(url, c.getTransportChannel().getRemoteAddress());
        }
    }

    protected void assertBrokerInfo(String brokerName) throws Exception {
        for (ActiveMQConnection c : connections) {
            assertEquals(brokerName, c.getBrokerInfo().getBrokerName());
        }
    }

    protected void addBroker(String name, BrokerService brokerService) {
        brokers.put(name, brokerService);
    }

    protected BrokerService getBroker(String name) {
        return brokers.get(name);
    }

    protected void stopBroker(String name) throws Exception {
        BrokerService broker = brokers.remove(name);
        broker.stop();
        broker.waitUntilStopped();
    }

    protected BrokerService removeBroker(String name) {
        return brokers.remove(name);
    }

    protected void destroyBrokerCluster() throws JMSException, Exception {
        for (BrokerService b : brokers.values()) {
            try {
                b.stop();
                b.waitUntilStopped();
            } catch (Exception e) {
                // Keep on going, we want to try and stop them all.
                logger.info("Error while stopping broker["+ b.getBrokerName() +"] continuing...");
            }
        }
        brokers.clear();
    }

    protected void shutdownClients() throws JMSException {
        for (Connection c : connections) {
            c.close();
        }
    }

    protected BrokerService createBroker(String brokerName) throws Exception {
        BrokerService answer = new BrokerService();
        answer.setPersistent(false);
        answer.setUseJmx(false);
        answer.setBrokerName(brokerName);
        answer.setUseShutdownHook(false);
        return answer;
    }

    protected void addTransportConnector(BrokerService brokerService,
                                         String connectorName, String uri, boolean clustered) throws Exception {
        TransportConnector connector = brokerService.addConnector(uri);
        connector.setName(connectorName);
        if (clustered) {
            connector.setRebalanceClusterClients(true);
            connector.setUpdateClusterClients(true);
            connector.setUpdateClusterClientsOnRemove(true);
        } else {
            connector.setRebalanceClusterClients(false);
            connector.setUpdateClusterClients(false);
            connector.setUpdateClusterClientsOnRemove(false);
        }
    }

    protected void addNetworkBridge(BrokerService answer, String bridgeName,
                                    String uri, boolean duplex, String destinationFilter) throws Exception {
        NetworkConnector network = answer.addNetworkConnector(uri);
        network.setName(bridgeName);
        network.setDuplex(duplex);
        if (destinationFilter != null && !destinationFilter.equals("")) {
            network.setDestinationFilter(bridgeName);
        }
    }

    protected void createClients() throws Exception {
        createClients(NUMBER_OF_CLIENTS);
    }

    @SuppressWarnings("unused")
    protected void createClients(int numOfClients) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(clientUrl);
        for (int i = 0; i < numOfClients; i++) {
            ActiveMQConnection c = (ActiveMQConnection) factory.createConnection();
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(getClass().getName());
            MessageConsumer consumer = s.createConsumer(queue);
            connections.add(c);
        }
    }

    public String getClientUrl() {
        return clientUrl;
    }

    public void setClientUrl(String clientUrl) {
        this.clientUrl = clientUrl;
    }
}
