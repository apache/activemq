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

import java.net.MalformedURLException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NetworkBrokerDetachTest extends TestCase {

	private final static String BROKER_NAME = "broker";
	private final static String REM_BROKER_NAME = "networkedBroker";
	private final static String QUEUE_NAME = "testQ";
	private final static int    NUM_CONSUMERS = 1;
	
    protected static final Log LOG = LogFactory.getLog(NetworkBrokerDetachTest.class);
    protected final int numRestarts = 3;

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(BROKER_NAME);
        broker.addConnector("tcp://localhost:61617");
        NetworkConnector networkConnector = broker.addNetworkConnector("static:(tcp://localhost:62617?wireFormat.maxInactivityDuration=500)?useExponentialBackOff=false");
        networkConnector.setDuplex(false);
        return broker;
    }
    
    protected BrokerService createNetworkedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName(REM_BROKER_NAME);
        broker.addConnector("tcp://localhost:62617");
        return broker;
    }
    
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
        
        for(int i=0; i<NUM_CONSUMERS; i++) {
          MessageConsumer consumer = consSession.createConsumer(consSession.createQueue(QUEUE_NAME));
        }

        
        Thread.sleep(5000);
        
        MBeanServerConnection mbsc = getMBeanServerConnection();
        // We should have 1 consumer for the queue on the local broker
        Object consumers = getAttribute(mbsc, "Queue", "Destination=" + QUEUE_NAME, "ConsumerCount");
        LOG.info("Consumers for " + QUEUE_NAME + " on " + BROKER_NAME + " : " + consumers);
        assertEquals(1L, ((Long)consumers).longValue());       
        
        
        LOG.info("Stopping Consumer on the networked broker ...");
        // Closing the connection will also close the consumer 
        consConn.close();
        
        Thread.sleep(5000);
        
        // We should have 0 consumer for the queue on the local broker
        consumers = getAttribute(mbsc, "Queue", "Destination=" + QUEUE_NAME, "ConsumerCount");
        LOG.info("Consumers for " + QUEUE_NAME + " on " + BROKER_NAME + " : " + consumers);
        assertEquals(0L, ((Long)consumers).longValue());       
        
        networkedBroker.stop();
        networkedBroker.waitUntilStopped();
        broker.stop();
        broker.waitUntilStopped();
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
    
    private MBeanServerConnection getMBeanServerConnection() throws MalformedURLException {
        final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi");
        MBeanServerConnection mbsc = null;
        try {
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
            mbsc = jmxc.getMBeanServerConnection();

//            // trace all existing MBeans
//            Set<?> all = mbsc.queryMBeans(null, null);
//            LOG.info("Total MBean count=" + all.size());
//            for (Object o : all) {
//                ObjectInstance bean = (ObjectInstance)o;
//                LOG.info(bean.getObjectName());
//            }
        } catch (Exception ignored) {
        }
        return mbsc;
    }
    
    private Object getAttribute(MBeanServerConnection mbsc, String type, String pattern, String attrName) throws Exception {
        Object obj = mbsc.getAttribute(getObjectName(BROKER_NAME, type, pattern), attrName);
        return obj;
    }
    
    private ObjectName getObjectName(String brokerName, String type, String pattern) throws Exception {
      ObjectName beanName = new ObjectName(
        "org.apache.activemq:BrokerName=" + brokerName + ",Type=" + type +"," + pattern
      );
      
      return beanName;
    }
}
