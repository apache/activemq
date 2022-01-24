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
package org.apache.activemq.partition;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.partition.dto.Partitioning;
import org.apache.activemq.partition.dto.Target;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Unit tests for the PartitionBroker plugin.
 */
public class PartitionBrokerTest {

    protected HashMap<String, BrokerService> brokers = new HashMap<String, BrokerService>();
    protected ArrayList<Connection> connections = new ArrayList<Connection>();
    Partitioning partitioning;

    @Before
    public void setUp() throws Exception {
        partitioning = new Partitioning();
        partitioning.brokers = new HashMap<String, String>();
    }

    /**
     * Partitioning can only re-direct failover clients since those
     * can re-connect and re-establish their state with another broker.
     */
    @Test(timeout = 1000*60*60)
    public void testNonFailoverClientHasNoPartitionEffect() throws Exception {

        partitioning.byClientId = new HashMap<String, Target>();
        partitioning.byClientId.put("client1", new Target("broker1"));
        createBrokerCluster(2);

        Connection connection = createConnectionToUrl(getConnectURL("broker2"));
        within(5, TimeUnit.SECONDS, new Task() {
            public void run() throws Exception {
                assertEquals(0, getTransportConnector("broker1").getConnections().size());
                assertEquals(1, getTransportConnector("broker2").getConnections().size());
            }
        });

        connection.setClientID("client1");
        connection.start();

        Thread.sleep(1000);
        assertEquals(0, getTransportConnector("broker1").getConnections().size());
        assertEquals(1, getTransportConnector("broker2").getConnections().size());
    }

    @Test(timeout = 1000*60*60)
    public void testPartitionByClientId() throws Exception {
        partitioning.byClientId = new HashMap<String, Target>();
        partitioning.byClientId.put("client1", new Target("broker1"));
        partitioning.byClientId.put("client2", new Target("broker2"));
        createBrokerCluster(2);

        Connection connection = createConnectionTo("broker2");

        within(5, TimeUnit.SECONDS, new Task() {
            public void run() throws Exception {
                assertEquals(0, getTransportConnector("broker1").getConnections().size());
                assertEquals(1, getTransportConnector("broker2").getConnections().size());
            }
        });

        connection.setClientID("client1");
        connection.start();
        within(5, TimeUnit.SECONDS, new Task() {
            public void run() throws Exception {
                assertEquals(1, getTransportConnector("broker1").getConnections().size());
                assertEquals(0, getTransportConnector("broker2").getConnections().size());
            }
        });
    }

    @Test(timeout = 1000*60*60)
    public void testPartitionByQueue() throws Exception {
        partitioning.byQueue = new HashMap<String, Target>();
        partitioning.byQueue.put("foo", new Target("broker1"));
        createBrokerCluster(2);

        Connection connection2 = createConnectionTo("broker2");

        within(5, TimeUnit.SECONDS, new Task() {
            public void run() throws Exception {
                assertEquals(0, getTransportConnector("broker1").getConnections().size());
                assertEquals(1, getTransportConnector("broker2").getConnections().size());
            }
        });

        Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session2.createConsumer(session2.createQueue("foo"));

        within(5, TimeUnit.SECONDS, new Task() {
            public void run() throws Exception {
                assertEquals(1, getTransportConnector("broker1").getConnections().size());
                assertEquals(0, getTransportConnector("broker2").getConnections().size());
            }
        });

        Connection connection1 = createConnectionTo("broker2");
        Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session1.createProducer(session1.createQueue("foo"));

        within(5, TimeUnit.SECONDS, new Task() {
            public void run() throws Exception {
                assertEquals(1, getTransportConnector("broker1").getConnections().size());
                assertEquals(1, getTransportConnector("broker2").getConnections().size());
            }
        });

        for (int i = 0; i < 100; i++) {
            producer.send(session1.createTextMessage("#" + i));
        }

        within(5, TimeUnit.SECONDS, new Task() {
            public void run() throws Exception {
                assertEquals(2, getTransportConnector("broker1").getConnections().size());
                assertEquals(0, getTransportConnector("broker2").getConnections().size());
            }
        });
    }


    static interface Task {
        public void run() throws Exception;
    }

    private void within(int time, TimeUnit unit, Task task) throws InterruptedException {
        long timeMS = unit.toMillis(time);
        long deadline = System.currentTimeMillis() + timeMS;
        while (true) {
            try {
                task.run();
                return;
            } catch (Throwable e) {
                long remaining = deadline - System.currentTimeMillis();
                if( remaining <=0 ) {
                    if( e instanceof RuntimeException ) {
                        throw (RuntimeException)e;
                    }
                    if( e instanceof Error ) {
                        throw (Error)e;
                    }
                    throw new RuntimeException(e);
                }
                Thread.sleep(Math.min(timeMS/10, remaining));
            }
        }
    }

    protected Connection createConnectionTo(String brokerId) throws IOException, URISyntaxException, JMSException {
        return createConnectionToUrl("failover://(" + getConnectURL(brokerId) + ")?randomize=false");
    }

    private Connection createConnectionToUrl(String url) throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        Connection connection = factory.createConnection();
        connections.add(connection);
        return connection;
    }

    protected String getConnectURL(String broker) throws IOException, URISyntaxException {
        TransportConnector tcp = getTransportConnector(broker);
        return tcp.getConnectUri().toString();
    }

    private TransportConnector getTransportConnector(String broker) {
        BrokerService brokerService = brokers.get(broker);
        if( brokerService==null ) {
            throw new IllegalArgumentException("Invalid broker id");
        }
        return brokerService.getTransportConnectorByName("tcp");
    }

    protected void createBrokerCluster(int brokerCount) throws Exception {
        for (int i = 1; i <= brokerCount; i++) {
            String brokerId = "broker" + i;
            BrokerService broker = createBroker(brokerId);
            broker.setPersistent(false);
            broker.addConnector("tcp://localhost:0").setName("tcp");
            addPartitionBrokerPlugin(broker);
            broker.start();
            broker.waitUntilStarted();
            partitioning.brokers.put(brokerId, getConnectURL(brokerId));
        }
    }

    protected void addPartitionBrokerPlugin(BrokerService broker) {
        PartitionBrokerPlugin plugin = new PartitionBrokerPlugin();
        plugin.setConfig(partitioning);
        broker.setPlugins(new BrokerPlugin[]{plugin});
    }

    protected BrokerService createBroker(String name) {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(name);
        brokers.put(name, broker);
        return broker;
    }

    @After
    public void tearDown() throws Exception {
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (Throwable e) {
            }
        }
        connections.clear();
        for (BrokerService broker : brokers.values()) {
            try {
                broker.stop();
                broker.waitUntilStopped();
            } catch (Throwable e) {
            }
        }
        brokers.clear();
    }

}
