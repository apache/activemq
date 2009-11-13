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
package org.apache.activemq.transport.discovery;

import java.net.URI;
import java.util.Vector;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DiscoveryTransportNoBrokerTest extends CombinationTestSupport {

    private static final Log LOG = LogFactory.getLog(DiscoveryTransportNoBrokerTest.class);

    public void testNoExtraThreads() throws Exception {
        BrokerService broker = new BrokerService();
        TransportConnector tcp = broker.addConnector("tcp://localhost:0?transport.closeAsync=false");
        String group = "GR-" +  System.currentTimeMillis();
        URI discoveryUri = new URI("multicast://default?group=" + group);
        tcp.setDiscoveryUri(discoveryUri);
        broker.start();
        broker.waitUntilStarted();
        
        Vector<String> existingNames = new Vector<String>();
        Thread[] threads = getThreads();
        for (Thread t : threads) {
            existingNames.add(t.getName());
        }
        final int idleThreadCount = threads.length;
        LOG.info("Broker started - thread Count:" + idleThreadCount);
        
       final int noConnectionToCreate = 10;
        for (int i=0; i<10;i++) {
            ActiveMQConnectionFactory factory = 
                new ActiveMQConnectionFactory("discovery:(multicast://239.255.2.3:6155?group=" + group +")?closeAsync=false");
            LOG.info("Connecting.");
            Connection connection = factory.createConnection();
            connection.setClientID("test");  
            connection.close();
        }
        Thread.sleep(2000);
        threads = getThreads();
        for (Thread t : threads) {
            if (!existingNames.contains(t.getName())) {
                LOG.info("Remaining thread:" + t);
            }
        }
        assertTrue("no extra threads per connection", Thread.activeCount() - idleThreadCount < noConnectionToCreate);
    }
   
    
    private Thread[] getThreads() {
        Thread[] threads = new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        return threads;
    }


    public void testMaxReconnectAttempts() throws JMSException {
        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("discovery:(multicast://doesNOTexist)");
            LOG.info("Connecting.");
            Connection connection = factory.createConnection();
            connection.setClientID("test");
            fail("Did not fail to connect as expected.");
        }
        catch ( JMSException expected ) {
            assertTrue("reason is java.io.IOException, was: " + expected.getCause(), expected.getCause() instanceof java.io.IOException);
        }
    }
    
    public void testInitialConnectDelayWithNoBroker() throws Exception {
        // the initialReconnectDelay only kicks in once a set of connect URL have
        // been returned from the discovery agent.
        // Up to that point the reconnectDelay is used which has a default value of 10
        //
        long initialReconnectDelay = 4000;
        long startT = System.currentTimeMillis();
        String groupId = "WillNotMatch" + startT;
        try {
            String urlStr = "discovery:(multicast://default?group=" + groupId + 
                ")?useExponentialBackOff=false&maxReconnectAttempts=2&reconnectDelay=" + initialReconnectDelay;          
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(urlStr);
            LOG.info("Connecting.");
            Connection connection = factory.createConnection();
            connection.setClientID("test");
            fail("Did not fail to connect as expected.");
        } catch ( JMSException expected ) {
            assertTrue("reason is java.io.IOException, was: " + expected.getCause(), expected.getCause() instanceof java.io.IOException);
            long duration = System.currentTimeMillis() - startT;
            assertTrue("took at least initialReconnectDelay time: " + duration + " e:" + expected, duration >= initialReconnectDelay);
        }
    }
}
