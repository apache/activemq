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

import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DiscoveryTransportNoBrokerTest extends CombinationTestSupport {

    private static final Log LOG = LogFactory.getLog(DiscoveryTransportNoBrokerTest.class);
    
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
            assertTrue("took at least initialReconnectDelay time: " + duration, duration >= initialReconnectDelay);
        }
    }
}
