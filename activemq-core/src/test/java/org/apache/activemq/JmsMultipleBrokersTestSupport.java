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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.advisory.ConsumerEvent;
import org.apache.activemq.advisory.ConsumerEventSource;
import org.apache.activemq.advisory.ConsumerListener;
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
import org.apache.activemq.util.Wait;
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
    public static int maxSetupTime = 5000;

    protected Map<String, BrokerItem> brokers;
    protected Map<String, Destination> destinations;

    protected int messageSize = 1;

    protected boolean persistentDelivery = true;
    protected boolean verbose;

    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName) throws Exception {
        return bridgeBrokers(localBrokerName, remoteBrokerName, false, 1, true);
    }

    protected void bridgeBrokers(String localBrokerName, String remoteBrokerName, boolean dynamicOnly) throws Exception {
        BrokerService localBroker = brokers.get(localBrokerName).broker;
        BrokerService remoteBroker = brokers.get(remoteBrokerName).broker;

        bridgeBrokers(localBroker, remoteBroker, dynamicOnly, 1, true, false);
    }

    protected NetworkConnector bridgeBrokers(String localBrokerName, String remoteBrokerName, boolean dynamicOnly, int networkTTL, boolean conduit) throws Exception {
        BrokerService localBroker = brokers.get(localBrokerName).broker;
        BrokerService remoteBroker = brokers.get(remoteBrokerName).broker;

        return bridgeBrokers(localBroker, remoteBroker, dynamicOnly, networkTTL, conduit, false);
    }

    // Overwrite this method to specify how you want to bridge the two brokers
    // By default, bridge them using add network connector of the local broker
    // and the first connector of the remote broker
    protected NetworkConnector bridgeBrokers(BrokerService localBroker, BrokerService remoteBroker, boolean dynamicOnly, int networkTTL, boolean conduit, boolean failover) throws Exception {
        List<TransportConnector> transportConnectors = remoteBroker.getTransportConnectors();
        URI remoteURI;
        if (!transportConnectors.isEmpty()) {
            remoteURI = transportConnectors.get(0).getConnectUri();
            String uri = "static:(" + remoteURI + ")";
            if (failover) {
                uri = "static:(failover:(" + remoteURI + "))";
            }
            NetworkConnector connector = new DiscoveryNetworkConnector(new URI(uri));
            connector.setDynamicOnly(dynamicOnly);
            connector.setNetworkTTL(networkTTL);
            connector.setConduitSubscriptions(conduit);
            localBroker.addNetworkConnector(connector);
            maxSetupTime = 2000;
            return connector;
        } else {
            throw new Exception("Remote broker has no registered connectors.");
        }

    }

    // This will interconnect all brokers using multicast
    protected void bridgeAllBrokers() throws Exception {
        bridgeAllBrokers("default", 1, false, false);
    }
    
    protected void bridgeAllBrokers(String groupName, int ttl, boolean suppressduplicateQueueSubs) throws Exception {
        bridgeAllBrokers(groupName, ttl, suppressduplicateQueueSubs, false);
    }

    protected void bridgeAllBrokers(String groupName, int ttl, boolean suppressduplicateQueueSubs, boolean decreasePriority) throws Exception {
        Collection<BrokerItem> brokerList = brokers.values();
        for (Iterator<BrokerItem> i = brokerList.iterator(); i.hasNext();) {
            BrokerService broker = i.next().broker;
            List<TransportConnector> transportConnectors = broker.getTransportConnectors();

            if (transportConnectors.isEmpty()) {
                broker.addConnector(new URI(AUTO_ASSIGN_TRANSPORT));
                transportConnectors = broker.getTransportConnectors();
            }

            TransportConnector transport = transportConnectors.get(0);
            transport.setDiscoveryUri(new URI("multicast://default?group=" + groupName));
            NetworkConnector nc = broker.addNetworkConnector("multicast://default?group=" + groupName);
            nc.setNetworkTTL(ttl);
            nc.setSuppressDuplicateQueueSubscriptions(suppressduplicateQueueSubs);
            nc.setDecreaseNetworkConsumerPriority(decreasePriority);
        }

        // Multicasting may take longer to setup
        maxSetupTime = 8000;
    }


    protected void waitForBridgeFormation(final int min) throws Exception {
        for (BrokerItem brokerItem : brokers.values()) {
            final BrokerService broker = brokerItem.broker;
            if (!broker.getNetworkConnectors().isEmpty()) {
                Wait.waitFor(new Wait.Condition() {
                    public boolean isSatisified() throws Exception {
                        return (broker.getNetworkConnectors().get(0).activeBridges().size() >= min);
                    }}, Wait.MAX_WAIT_MILLIS * 2);
            }
        }
    }

    protected void waitForBridgeFormation() throws Exception {
        waitForBridgeFormation(1);
    }

    protected void startAllBrokers() throws Exception {
        Collection<BrokerItem> brokerList = brokers.values();
        for (Iterator<BrokerItem> i = brokerList.iterator(); i.hasNext();) {
            BrokerService broker = i.next().broker;
            broker.start();
        }

        Thread.sleep(maxSetupTime);
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
        BrokerItem brokerItem = brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.factory;
        }
        return null;
    }

    protected Connection createConnection(String brokerName) throws Exception {
        BrokerItem brokerItem = brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.createConnection();
        }
        return null;
    }

    protected MessageConsumer createSyncConsumer(String brokerName, Destination dest) throws Exception {
        BrokerItem brokerItem = brokers.get(brokerName);
        if (brokerItem != null) {
            Connection con = brokerItem.createConnection();
            con.start();
            Session sess = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = sess.createConsumer(dest);
            return consumer;
        }
        return null;
    }

    protected MessageConsumer createConsumer(String brokerName, Destination dest) throws Exception {
        return createConsumer(brokerName, dest, null, null);
    }

    protected MessageConsumer createConsumer(String brokerName, Destination dest, String messageSelector) throws Exception {
        return createConsumer(brokerName, dest, null, messageSelector);
    }
    
    protected MessageConsumer createConsumer(String brokerName, Destination dest, CountDownLatch latch) throws Exception {
    	return createConsumer(brokerName, dest, latch, null);
    }
    
    protected MessageConsumer createConsumer(String brokerName, Destination dest, CountDownLatch latch, String messageSelector) throws Exception {
        BrokerItem brokerItem = brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.createConsumer(dest, latch, messageSelector);
        }
        return null;
    }
    
    protected QueueBrowser createBrowser(String brokerName, Destination dest) throws Exception {
        BrokerItem brokerItem = brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.createBrowser(dest);
        }
        return null;
    }

    protected MessageConsumer createDurableSubscriber(String brokerName, Topic dest, String name) throws Exception {
        BrokerItem brokerItem = brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.createDurableSubscriber(dest, name);
        }
        return null;
    }

    protected MessageIdList getBrokerMessages(String brokerName) {
        BrokerItem brokerItem = brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.getAllMessages();
        }
        return null;
    }

    protected MessageIdList getConsumerMessages(String brokerName, MessageConsumer consumer) {
        BrokerItem brokerItem = brokers.get(brokerName);
        if (brokerItem != null) {
            return brokerItem.getConsumerMessages(consumer);
        }
        return null;
    }
    
    protected void assertConsumersConnect(String brokerName, Destination destination, final int count, long timeout) throws Exception {
        BrokerItem brokerItem = brokers.get(brokerName);
        Connection conn = brokerItem.createConnection();
        conn.start();
        ConsumerEventSource ces = new ConsumerEventSource(conn, destination);

        try {
        	final AtomicInteger actualConnected = new AtomicInteger();
	        final CountDownLatch latch = new CountDownLatch(1);        
	        ces.setConsumerListener(new ConsumerListener(){
				public void onConsumerEvent(ConsumerEvent event) {
					if( actualConnected.get() < count ) {
						actualConnected.set(event.getConsumerCount());
					}
					if( event.getConsumerCount() >= count ) {
						latch.countDown();
					}				
				}
			});
	        ces.start();
	        
	        latch.await(timeout, TimeUnit.MILLISECONDS);
	        assertTrue("Expected at least "+count+" consumers to connect, but only "+actualConnected.get()+" connectect within "+timeout+" ms", actualConnected.get() >= count);
	        
        } finally {
            ces.stop();
            conn.close();
            brokerItem.connections.remove(conn);
        }
    }


    protected void sendMessages(String brokerName, Destination destination, int count) throws Exception {
    	sendMessages(brokerName, destination, count, null);
    }
    
    protected void sendMessages(String brokerName, Destination destination, int count, HashMap<String, Object>properties) throws Exception {
        BrokerItem brokerItem = brokers.get(brokerName);

        Connection conn = brokerItem.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = brokerItem.createProducer(destination, sess);
        producer.setDeliveryMode(persistentDelivery ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

        for (int i = 0; i < count; i++) {
            TextMessage msg = createTextMessage(sess, conn.getClientID() + ": Message-" + i);
            if (properties != null) {
            	for (String propertyName : properties.keySet()) {
            		msg.setObjectProperty(propertyName, properties.get(propertyName));
            	}
            }
            producer.send(msg);
            onSend(i, msg);
        }

        producer.close();
        sess.close();
        conn.close();
        brokerItem.connections.remove(conn);
    }

    protected void onSend(int i, TextMessage msg) {
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
        brokers = new HashMap<String, BrokerItem>();
        destinations = new HashMap<String, Destination>();
    }

    protected void tearDown() throws Exception {
        destroyAllBrokers();
        super.tearDown();
    }

    protected void destroyBroker(String brokerName) throws Exception {
        BrokerItem brokerItem = brokers.remove(brokerName);

        if (brokerItem != null) {
            brokerItem.destroy();
        }
    }

    protected void destroyAllBrokers() throws Exception {
        for (Iterator<BrokerItem> i = brokers.values().iterator(); i.hasNext();) {
            BrokerItem brokerItem = i.next();
            brokerItem.destroy();
        }
        brokers.clear();
    }

    // Class to group broker components together
    public class BrokerItem {
        public BrokerService broker;
        public ActiveMQConnectionFactory factory;
        public List<Connection> connections;
        public Map<MessageConsumer, MessageIdList> consumers;
        public MessageIdList allMessages = new MessageIdList();
        public boolean persistent;
        private IdGenerator id;

        public BrokerItem(BrokerService broker) throws Exception {
            this.broker = broker;

            factory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
            consumers = Collections.synchronizedMap(new HashMap<MessageConsumer, MessageIdList>());
            connections = Collections.synchronizedList(new ArrayList<Connection>());
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
            return createConsumer(dest, null, null);
        }
        
        public MessageConsumer createConsumer(Destination dest, String messageSelector) throws Exception {
        	return createConsumer(dest, null, messageSelector);
        }

        public MessageConsumer createConsumer(Destination dest, CountDownLatch latch, String messageSelector) throws Exception {
            Connection c = createConnection();
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return createConsumerWithSession(dest, s, latch, messageSelector);
        }

        public MessageConsumer createConsumerWithSession(Destination dest, Session sess) throws Exception {
            return createConsumerWithSession(dest, sess, null, null);
        }

        public MessageConsumer createConsumerWithSession(Destination dest, Session sess, CountDownLatch latch, String messageSelector) throws Exception {
            MessageConsumer client = sess.createConsumer(dest, messageSelector);
            MessageIdList messageIdList = new MessageIdList();
            messageIdList.setCountDownLatch(latch);
            messageIdList.setParent(allMessages);
            client.setMessageListener(messageIdList);
            consumers.put(client, messageIdList);
            return client;
        }
        
        public QueueBrowser createBrowser(Destination dest) throws Exception {
            Connection c = createConnection();
            c.start();
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return s.createBrowser((Queue)dest);
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
            return consumers.get(consumer);
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
                Connection c = connections.remove(0);
                try {
                    c.close();
                } catch (ConnectionClosedException e) {
                } catch (JMSException e) {
                }
            }

            broker.stop();
            broker.waitUntilStopped();
            consumers.clear();

            broker = null;
            connections = null;
            consumers = null;
            factory = null;
        }
    }

}
