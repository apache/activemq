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
package org.apache.activemq.network;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TopicSubscriber;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class NetworkBrokerDetachTest {

	private final static String BROKER_NAME = "broker";
	private final static String REM_BROKER_NAME = "networkedBroker";
	private final static String DESTINATION_NAME = "testQ";
	private final static int    NUM_CONSUMERS = 1;
	
    protected static final Log LOG = LogFactory.getLog(NetworkBrokerDetachTest.class);
    protected final int numRestarts = 3;
    protected final int networkTTL = 2;
    protected final boolean dynamicOnly = false;

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(BROKER_NAME);
        configureBroker(broker);
        broker.addConnector("tcp://localhost:61617");
        NetworkConnector networkConnector = broker.addNetworkConnector("static:(tcp://localhost:62617?wireFormat.maxInactivityDuration=500)?useExponentialBackOff=false");
        configureNetworkConnector(networkConnector);
        return broker;
    }

    protected BrokerService createNetworkedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(REM_BROKER_NAME);
        configureBroker(broker);
        broker.getManagementContext().setCreateConnector(false);
        broker.addConnector("tcp://localhost:62617");
        NetworkConnector networkConnector = broker.addNetworkConnector("static:(tcp://localhost:61617?wireFormat.maxInactivityDuration=500)?useExponentialBackOff=false");
        configureNetworkConnector(networkConnector);
        return broker;
    }
    
    private void configureNetworkConnector(NetworkConnector networkConnector) {
        networkConnector.setDuplex(false);
        networkConnector.setNetworkTTL(networkTTL);
        networkConnector.setDynamicOnly(dynamicOnly);
    }
    
    // variants for each store....
    private void configureBroker(BrokerService broker) throws Exception {
        //KahaPersistenceAdapter persistenceAdapter = new KahaPersistenceAdapter();
        //persistenceAdapter.setDirectory(new File("target/activemq-data/kaha/" + broker.getBrokerName() + "/NetworBrokerDetatchTest"));
        //broker.setPersistenceAdapter(persistenceAdapter);        
        
        KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
        persistenceAdapter.setDirectory(new File("target/activemq-data/kahadb/NetworBrokerDetatchTest"));
        broker.setPersistenceAdapter(persistenceAdapter);
        
        // default AMQ
    }

    @Test
    public void testNetworkedBrokerDetach() throws Exception {
        BrokerService broker = createBroker();
        broker.start();
        
        BrokerService networkedBroker = createNetworkedBroker();
        networkedBroker.start();
        
        LOG.info("Creating Consumer on the networked broker ...");
        // Create a consumer on the networked broker 
        ConnectionFactory consFactory = createConnectionFactory(networkedBroker);
        Connection consConn = consFactory.createConnection();
        Session consSession = consConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQDestination destination = (ActiveMQDestination) consSession.createQueue(DESTINATION_NAME);
        for(int i=0; i<NUM_CONSUMERS; i++) {
            consSession.createConsumer(destination);
        }
        
        assertTrue("got expected consumer count from mbean within time limit", 
                verifyConsumerCount(1, destination, BROKER_NAME));
        
        
        LOG.info("Stopping Consumer on the networked broker ...");
        // Closing the connection will also close the consumer 
        consConn.close();
        
        // We should have 0 consumer for the queue on the local broker
        assertTrue("got expected 0 count from mbean within time limit", verifyConsumerCount(0, destination, BROKER_NAME));
        
        networkedBroker.stop();
        networkedBroker.waitUntilStopped();
        broker.stop();
        broker.waitUntilStopped();
    }

    
    @Test
    public void testNetworkedBrokerDurableSubAfterRestart() throws Exception {        
        BrokerService brokerOne = createBroker();
        brokerOne.setDeleteAllMessagesOnStartup(true);
        brokerOne.start();

        BrokerService brokerTwo = createNetworkedBroker();
        brokerTwo.setDeleteAllMessagesOnStartup(true);
        brokerTwo.start();
        
        final AtomicInteger count = new AtomicInteger(0);
        MessageListener counter = new MessageListener() {
            public void onMessage(Message message) {
                count.incrementAndGet();
            }
        };
        
        LOG.info("Creating durable consumer on each broker ...");
        ActiveMQTopic destination = registerDurableConsumer(brokerTwo, counter);
        registerDurableConsumer(brokerOne, counter);
        
        assertTrue("got expected consumer count from local broker mbean within time limit",
                verifyConsumerCount(2, destination, BROKER_NAME));
        
        assertTrue("got expected consumer count from network broker mbean within time limit",
                verifyConsumerCount(2, destination, REM_BROKER_NAME));
        
        sendMessageTo(destination, brokerOne);
        
        assertTrue("Got one message on each", verifyMessageCount(2, count));
        
        LOG.info("Stopping brokerTwo...");
        brokerTwo.stop();
        brokerTwo.waitUntilStopped();           
        
        LOG.info("restarting  broker Two...");
        brokerTwo = createNetworkedBroker();
        brokerTwo.start();
   
        LOG.info("Recreating durable Consumer on the broker after restart...");
        registerDurableConsumer(brokerTwo, counter);
        
        // give advisories a chance to percolate
        TimeUnit.SECONDS.sleep(5);
        
        sendMessageTo(destination, brokerOne);
        
        // expect similar after restart
        assertTrue("got expected consumer count from local broker mbean within time limit",
                verifyConsumerCount(2, destination, BROKER_NAME));
 
        // a durable sub is auto bridged on restart unless dynamicOnly=true
        assertTrue("got expected consumer count from network broker mbean within time limit",
                verifyConsumerCount(2, destination, REM_BROKER_NAME));

        assertTrue("got no inactive subs on broker", verifyDurableConsumerCount(0, BROKER_NAME));
        assertTrue("got no inactive subs on other broker", verifyDurableConsumerCount(0, REM_BROKER_NAME));

        assertTrue("Got two more messages after restart", verifyMessageCount(4, count));
        TimeUnit.SECONDS.sleep(1);
        assertTrue("still Got just two more messages", verifyMessageCount(4, count));
        
        brokerTwo.stop();
        brokerTwo.waitUntilStopped();
        brokerOne.stop();
        brokerOne.waitUntilStopped();
    }

    private boolean verifyMessageCount(final int i, final AtomicInteger count) throws Exception {
        return Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                return i == count.get();
            }      
        });
    }

    private ActiveMQTopic registerDurableConsumer(
            BrokerService brokerService, MessageListener listener) throws Exception {
        ConnectionFactory factory = createConnectionFactory(brokerService);
        Connection connection = factory.createConnection();
        connection.setClientID("DurableOne");
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQTopic destination = (ActiveMQTopic) session.createTopic(DESTINATION_NAME);
        // unique to a broker
        TopicSubscriber sub = session.createDurableSubscriber(destination, "SubOne" + brokerService.getBrokerName());
        sub.setMessageListener(listener);
        return destination;
    }

    private void sendMessageTo(ActiveMQTopic destination, BrokerService brokerService) throws Exception {
        ConnectionFactory factory = createConnectionFactory(brokerService);
        Connection conn = factory.createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createProducer(destination).send(session.createTextMessage("Hi"));
        conn.close();
    }
    
    protected ConnectionFactory createConnectionFactory(final BrokerService broker) throws Exception {
        
        String url = ((TransportConnector) broker.getTransportConnectors().get(0)).getServer().getConnectURI().toString();
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
        connectionFactory.setOptimizedMessageDispatch(true);
        connectionFactory.setCopyMessageOnSend(false);
        connectionFactory.setUseCompression(false);
        connectionFactory.setDispatchAsync(false);
        connectionFactory.setUseAsyncSend(false);
        connectionFactory.setOptimizeAcknowledge(false);
        connectionFactory.setWatchTopicAdvisories(true);
        ActiveMQPrefetchPolicy qPrefetchPolicy= new ActiveMQPrefetchPolicy();
        qPrefetchPolicy.setQueuePrefetch(100);
        qPrefetchPolicy.setTopicPrefetch(1000);
        connectionFactory.setPrefetchPolicy(qPrefetchPolicy);
        connectionFactory.setAlwaysSyncSend(true);
        return connectionFactory;
    }
    
    // JMX Helper Methods 
    private boolean verifyConsumerCount(final long expectedCount, final ActiveMQDestination destination, final String brokerName) throws Exception {
        return Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                boolean result = false;
                MBeanServerConnection mbsc = getMBeanServerConnection();
                if (mbsc != null) {                
                    // We should have 1 consumer for the queue on the local broker
                    Object consumers = getAttribute(mbsc, brokerName, destination.isQueue() ? "Queue" : "Topic", "Destination=" + destination.getPhysicalName(), "ConsumerCount");
                    if (consumers != null) {
                        LOG.info("Consumers for " + destination.getPhysicalName() + " on " + brokerName + " : " + consumers);
                        if (expectedCount == ((Long)consumers).longValue()) {
                            result = true;
                        }
                    }
                }
                return result;
            }      
        });
    }
    
    
    private boolean verifyDurableConsumerCount(final long expectedCount, final String brokerName) throws Exception {
        return Wait.waitFor(new Wait.Condition() {
            public boolean isSatisified() throws Exception {
                boolean result = false;
                MBeanServerConnection mbsc = getMBeanServerConnection();
                if (mbsc != null) {
                    Set subs = getMbeans(mbsc, brokerName, "Subscription", "active=false,*");
                    if (subs != null) {
                        LOG.info("inactive durable subs on " + brokerName + " : " + subs);
                        if (expectedCount == subs.size()) {
                            result = true;
                        }
                    }
                }
                return result;
            }      
        });
    }

    
    private MBeanServerConnection getMBeanServerConnection() throws MalformedURLException {
        final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi");
        MBeanServerConnection mbsc = null;
        try {
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
            mbsc = jmxc.getMBeanServerConnection();
        } catch (Exception ignored) {
            LOG.warn("getMBeanServer ex: " + ignored);
        }
        // If port 1099 is in use when the Broker starts, starting the jmx
        // connector will fail.  So, if we have no mbsc to query, skip the
        // test.
        assumeNotNull(mbsc);
        return mbsc;
    }
    
    
    private Set getMbeans(MBeanServerConnection mbsc, String brokerName, String type, String pattern) throws Exception {
        Set obj = null;
        try {
            obj = mbsc.queryMBeans(getObjectName(brokerName, type, pattern), null);
        } catch (InstanceNotFoundException ignored) {
            LOG.warn("getAttribute ex: " + ignored);
        }
        return obj;
    }
    
    private Object getAttribute(MBeanServerConnection mbsc, String brokerName, String type, String pattern, String attrName) throws Exception {
        Object obj = null;
        try {
            obj = mbsc.getAttribute(getObjectName(brokerName, type, pattern), attrName);
        } catch (InstanceNotFoundException ignored) {
            LOG.warn("getAttribute ex: " + ignored);
        }
        return obj;
    }
    
    private ObjectName getObjectName(String brokerName, String type, String pattern) throws Exception {
      ObjectName beanName = new ObjectName(
        "org.apache.activemq:BrokerName=" + brokerName + ",Type=" + type +"," + pattern
      );
      
      return beanName;
    }
}
