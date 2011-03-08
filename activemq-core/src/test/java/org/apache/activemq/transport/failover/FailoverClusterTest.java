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

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.NetworkConnector;


public class FailoverClusterTest extends TestCase {

    private static final int NUMBER = 10;
    private static final String BROKER_A_BIND_ADDRESS = "tcp://0.0.0.0:61616";
    private static final String BROKER_B_BIND_ADDRESS = "tcp://0.0.0.0:61617";
    private static final String BROKER_A_NAME = "BROKERA";
    private static final String BROKER_B_NAME = "BROKERB";
    private BrokerService brokerA;
    private BrokerService brokerB;
    private String clientUrl;

    private final List<ActiveMQConnection> connections = new ArrayList<ActiveMQConnection>();


    public void testClusterConnectedAfterClients() throws Exception {
        createClients();
        if (brokerB == null) {
            brokerB = createBrokerB(BROKER_B_BIND_ADDRESS);
        }
        Thread.sleep(5000);
        Set<String> set = new HashSet<String>();
        for (ActiveMQConnection c : connections) {
            set.add(c.getTransportChannel().getRemoteAddress());
        }
        assertTrue(set.size() > 1);
    }

    public void testClusterURIOptionsStrip() throws Exception {
        createClients();
        if (brokerB == null) {
            // add in server side only url param, should not be propagated
            brokerB = createBrokerB(BROKER_B_BIND_ADDRESS + "?transport.closeAsync=false");
        }
        Thread.sleep(5000);
        Set<String> set = new HashSet<String>();
        for (ActiveMQConnection c : connections) {
            set.add(c.getTransportChannel().getRemoteAddress());
        }
        assertTrue(set.size() > 1);
    }


    public void testClusterConnectedBeforeClients() throws Exception {

        if (brokerB == null) {
            brokerB = createBrokerB(BROKER_B_BIND_ADDRESS);
        }
        Thread.sleep(5000);
        createClients();
        Thread.sleep(2000);
        brokerA.stop();
        Thread.sleep(2000);

        URI brokerBURI = new URI(BROKER_B_BIND_ADDRESS);
        for (ActiveMQConnection c : connections) {
            String addr = c.getTransportChannel().getRemoteAddress();
            assertTrue(addr.indexOf("" + brokerBURI.getPort()) > 0);
        }
    }

    @Override
    protected void setUp() throws Exception {
        if (brokerA == null) {
            brokerA = createBrokerA(BROKER_A_BIND_ADDRESS + "?transport.closeAsync=false");
            clientUrl = "failover://(" + brokerA.getTransportConnectors().get(0).getPublishableConnectString() + ")";
        }
    }

    @Override
    protected void tearDown() throws Exception {
        for (Connection c : connections) {
            c.close();
        }
        if (brokerB != null) {
            brokerB.stop();
            brokerB = null;
        }
        if (brokerA != null) {
            brokerA.stop();
            brokerA = null;
        }
    }

    protected BrokerService createBrokerA(String uri) throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(false);
        configureConsumerBroker(answer, uri);
        answer.start();
        return answer;
    }

    protected void configureConsumerBroker(BrokerService answer, String uri) throws Exception {
        answer.setBrokerName(BROKER_A_NAME);
        answer.setPersistent(false);
        TransportConnector connector = answer.addConnector(uri);
        connector.setRebalanceClusterClients(true);
        connector.setUpdateClusterClients(true);
        answer.setUseShutdownHook(false);
    }

    protected BrokerService createBrokerB(String uri) throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(false);
        configureNetwork(answer, uri);
        answer.start();
        return answer;
    }

    protected void configureNetwork(BrokerService answer, String uri) throws Exception {
        answer.setBrokerName(BROKER_B_NAME);
        answer.setPersistent(false);
        NetworkConnector network = answer.addNetworkConnector("static://" + BROKER_A_BIND_ADDRESS);
        network.setDuplex(true);
        TransportConnector connector = answer.addConnector(uri);
        connector.setRebalanceClusterClients(true);
        connector.setUpdateClusterClients(true);
        answer.setUseShutdownHook(false);
    }

    protected void createClients() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(clientUrl);
        for (int i = 0; i < NUMBER; i++) {
            ActiveMQConnection c = (ActiveMQConnection) factory.createConnection();
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = s.createQueue(getClass().getName());
            MessageConsumer consumer = s.createConsumer(queue);
            connections.add(c);
        }
    }
}
