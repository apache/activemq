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
package org.apache.activemq;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.MessageIdList;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.Resource;

/**
 * Test case support that allows the easy management and connection of several
 * brokers.
 * 
 * @version $Revision$
 */
public class JmsMultipleBrokersTestSupport extends CombinationTestSupport {
    public static final String AUTO_ASSIGN_TRANSPORT = "tcp://localhost:0";
    public static int MAX_SETUP_TIME = 5000;

    protected Map brokers;
    protected Map destinations;

    protected int messageSize = 1;

    protected boolean persistentDelivery = true;
    protected boolean verbose;

    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName) throws Exception {
        return bridgeBrokers(localBrokerName, remoteBrokerName, false, 1);
    }

    protected void bridgeBrokers(String localBrokerName, String remoteBrokerName, boolean dynamicOnly) throws Exception {
        BrokerService localBroker = ((BrokerItem)brokers.get(localBrokerName)).broker;
        BrokerService remoteBroker = ((BrokerItem)brokers.get(remoteBrokerName)).broker;

        bridgeBrokers(localBroker, remoteBroker, dynamicOnly, 1);
    }

    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName, boolean dynamicOnly, int networkTTL) throws Exception {
        BrokerService localBroker = ((BrokerItem)brokers.get(localBrokerName)).broker;
        BrokerService remoteBroker = ((BrokerItem)brokers.get(remoteBrokerName)).broker;

        return bridgeBrokers(localBroker, remoteBroker, dynamicOnly, networkTTL);
    }

    // Overwrite this method to specify how you want to bridge the two brokers
    // By default, bridge them using add network connector of the local broker
    // and the first connector of the remote broker
    protected NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker, boolean dynamicOnly, int networkTTL) throws Exception {
        List transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI;
        if (!transportConnectors.isEmpty()) {
            remoteURI = ((TransportConnector)transportConnectors.get(0)).getConnectUri();
            NetworkConnector connector = new DiscoveryNetworkConnector(new URI("static:" + remoteURI));
            connector.setDynamicOnly(dynamicOnly);
            connector.setNetworkTTL(networkTTL);
            localBroker.addNetworkConnector(connector);
            MAX_SETUP_TIME = 2000;
            return connector;
        } else {
            throw new Exception("Remote broker has no registered connectors.");
        }

    }

    // This will interconnect all brokes using multicast
    protected void bridgeAllBrokers() throws Exception {
        bridgeAllBrokers("default");
    }

    protected void bridgeAllBrokers(String groupName) throws Exception {
        Collection brokerList = brokers.values();
        for (Iterator i = brokerList.iterator(); i.hasNext();) {
            BrokerService broker = ((BrokerItem)i.next()).broker;
            List transportConnectors = broker.getTransportConnectors();

            if (transportConnectors.isEmpty()) {
                broker.addConnector(new URI(AUTO_ASSIGN_TRANSPORT));
                transportConnectors = broker.getTransportConnectors();
            }

            TransportConnector transport = (TransportConnector)transportConnectors.get(0);
            transport.setDiscoveryUri(new URI("multicast://" + groupName));
            broker.addNetworkConnector("multicast://" + groupName);
        }

        // Multicasting may take longer to setup
        MAX_SETUP_TIME = 8000;
    }

    protected void startAllBrokers() throws Exception {
        Collection brokerList = brokers.values();
        for (Iterator i = brokerList.iterator(); i.hasNext();) {
            BrokerService broker = ((BrokerItem)i.next()).broker;
            broker.start();
        }

        Thread.sleep(MAX_SETUP_TIME);
    }

    protected BrokerService createBroker(String brokerName) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(brokerName);
        brokers.put(brokerName, new BrokerItem(broker));

        return broker;
    }

    protected BrokerService createBroker(URI brokerUri) throws Exception {
        BrokerService broker = BrokerFactory.createBroker(brokerUri);
        brokers.put(broker.getBrokerName(), new BrokerItem(broker));

        return broker;
    }

    protected BrokerService createBroker(Resource configFile) throws Exception {
        BrokerFactoryBean brokerFactory = new BrokerFactoryBean(configFile);
        brokerFactory.afterPropertiesSet();

        BrokerService broker = brokerFactory.getBroker();
        brokers.put(broker.getBrokerName(), new BrokerItem(broker));

        return broker;
    }

    protected ConnectionFactory getConnectionFactory(String brokerName) throws Exception {
        BrokerItem brokerItem = (BrokerItem)brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.factory;
        }
        return null;
    }

    protected Connection createConnection(String brokerName) throws Exception {
        BrokerItem brokerItem = (BrokerItem)brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.createConnection();
        }
        return null;
    }

    protected MessageConsumer createConsumer(String brokerName, Destination dest) throws Exception {
        return createConsumer(brokerName, dest, null);
    }

    protected MessageConsumer createConsumer(String brokerName, Destination dest, CountDownLatch latch) throws Exception {
        BrokerItem brokerItem = (BrokerItem)brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.createConsumer(dest, latch);
        }
        return null;
    }

    protected MessageConsumer createDurableSubscriber(String brokerName, Topic dest, String name) throws Exception {
        BrokerItem brokerItem = (BrokerItem)brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.createDurableSubscriber(dest, name);
        }
        return null;
    }

    protected MessageIdList getBrokerMessages(String brokerName) {
        BrokerItem brokerItem = (BrokerItem)brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.getAllMessages();
        }
        return null;
    }

    protected MessageIdList getConsumerMessages(String brokerName, MessageConsumer consumer) {
        BrokerItem brokerItem = (BrokerItem)brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.getConsumerMessages(consumer);
        }
        return null;
    }

    protected void sendMessages(String brokerName, Destination destination, int count) throws Exception {
        BrokerItem brokerItem = (BrokerItem)brokers.get(brokerName);

        Connection conn = brokerItem.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = brokerItem.createProducer(destination, sess);
        producer.setDeliveryMode(persistentDelivery ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

        for (int i = 0; i < count; i++) {
            TextMessage msg = createTextMessage(sess, conn.getClientID() + ": Message-" + i);
            producer.send(msg);
        }

        producer.close();
        sess.close();
        conn.close();
        brokerItem.connections.remove(conn);
    }

    protected TextMessage createTextMessage(Session session, String initText) throws Exception {
        TextMessage msg = session.createTextMessage();

        // Pad message text
        if (initText.length() < messageSize) {
            char[] data = new char[messageSize - initText.length()];
            Arrays.fill(data, '*');
            String str = new String(data);
            msg.setText(initText + str);

            // Do not pad message text
        } else {
            msg.setText(initText);
        }

        return msg;
    }

    protected ActiveMQDestination createDestination(String name, boolean topic) throws JMSException {
        Destination dest;
        if (topic) {
            dest = new ActiveMQTopic(name);
            destinations.put(name, dest);
            return (ActiveMQDestination)dest;
        } else {
            dest = new ActiveMQQueue(name);
            destinations.put(name, dest);
            return (ActiveMQDestination)dest;
        }
    }

    protected void setUp() throws Exception {
        super.setUp();
        brokers = new HashMap();
        destinations = new HashMap();
    }

    protected void tearDown() throws Exception {
        destroyAllBrokers();
        super.tearDown();
    }

    protected void destroyBroker(String brokerName) throws Exception {
        BrokerItem brokerItem = (BrokerItem)brokers.remove(brokerName);

        if (brokerItem != null) {
            brokerItem.destroy();
        }
    }

    protected void destroyAllBrokers() throws Exception {
        for (Iterator i = brokers.values().iterator(); i.hasNext();) {
            BrokerItem brokerItem = (BrokerItem)i.next();
            brokerItem.destroy();
        }
        brokers.clear();
    }

    // Class to group broker components together
    public class BrokerItem {
        public BrokerService broker;
        public ActiveMQConnectionFactory factory;
        public List connections;
        public Map consumers;
        public MessageIdList allMessages = new MessageIdList();

        private IdGenerator id;

        public boolean persistent;

        public BrokerItem(BrokerService broker) throws Exception {
            this.broker = broker;

            factory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
            consumers = Collections.synchronizedMap(new HashMap());
            connections = Collections.synchronizedList(new ArrayList());
            allMessages.setVerbose(verbose);
            id = new IdGenerator(broker.getBrokerName() + ":");
        }

        public Connection createConnection() throws Exception {
            Connection conn = factory.createConnection();
            conn.setClientID(id.generateId());

            connections.add(conn);
            return conn;
        }

        public MessageConsumer createConsumer(Destination dest) throws Exception {
            return createConsumer(dest, null);
        }

        public MessageConsumer createConsumer(Destination dest, CountDownLatch latch) throws Exception {
            Connection c = createConnection();
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return createConsumerWithSession(dest, s, latch);
        }

        public MessageConsumer createConsumerWithSession(Destination dest, Session sess) throws Exception {
            return createConsumerWithSession(dest, sess, null);
        }

        public MessageConsumer createConsumerWithSession(Destination dest, Session sess, CountDownLatch latch) throws Exception {
            MessageConsumer client = sess.createConsumer(dest);
            MessageIdList messageIdList = new MessageIdList();
            messageIdList.setCountDownLatch(latch);
            messageIdList.setParent(allMessages);
            client.setMessageListener(messageIdList);
            consumers.put(client, messageIdList);
            return client;
        }

        public MessageConsumer createDurableSubscriber(Topic dest, String name) throws Exception {
            Connection c = createConnection();
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return createDurableSubscriber(dest, s, name);
        }

        public MessageConsumer createDurableSubscriber(Topic dest, Session sess, String name) throws Exception {
            MessageConsumer client = sess.createDurableSubscriber((Topic)dest, name);
            MessageIdList messageIdList = new MessageIdList();
            messageIdList.setParent(allMessages);
            client.setMessageListener(messageIdList);
            consumers.put(client, messageIdList);

            return client;
        }

        public MessageIdList getAllMessages() {
            return allMessages;
        }

        public MessageIdList getConsumerMessages(MessageConsumer consumer) {
            return (MessageIdList)consumers.get(consumer);
        }

        public MessageProducer createProducer(Destination dest) throws Exception {
            Connection c = createConnection();
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return createProducer(dest, s);
        }

        public MessageProducer createProducer(Destination dest, Session sess) throws Exception {
            MessageProducer client = sess.createProducer(dest);
            client.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            return client;
        }

        public void destroy() throws Exception {
            while (!connections.isEmpty()) {
                Connection c = (Connection)connections.remove(0);
                try {
                    c.close();
                } catch (ConnectionClosedException e) {
                }
            }

            broker.stop();
            consumers.clear();

            broker = null;
            connections = null;
            consumers = null;
            factory = null;
        }
    }

}
