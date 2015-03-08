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
package org.apache.activemq.bugs;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSession;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This test will check that maximumProducersAllowedPerConnection is enforced
 * when set even when a client uses anonymous producers.
 */
public class AMQ5649Test {
    private BrokerService broker;
    private URI brokerConnectURI;
    private int MAX_PRODUCERS = 10;
    
    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);

        TransportConnector connector = broker.addConnector(new TransportConnector());
        connector.setUri(new URI("tcp://0.0.0.0:0"));
        connector.setName("tcp");
        //configure the max producers
        connector.setMaximumProducersAllowedPerConnection(MAX_PRODUCERS);
        
        broker.start();
        broker.waitUntilStarted();

        brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    /**
     * This test checks producers with a destination specified to make sure an
     * exception happens when exceeding the maximum allowed producers per connection
     * @throws Exception
     */
    @Test(expected=JMSException.class)
    public void testMaximumProducersAllowedPerConnection() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.start();
        Session session = connection.createSession(false, TopicSession.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("test.topic");
        // Verify that maximumProducersAllowedPerConnection throws an exception
        try {
	        for (int i =0; i < MAX_PRODUCERS + 1; i++) {
	        	session.createProducer(topic);
	        }
	    } finally {
	    	connection.stop();
	    }
    }

    /**
     * This test checks producers WITHOUT a destination specified to make sure an
     * exception happens when exceeding the maximum allowed producers per connection
     * with an anonymous producer
     * @throws Exception
     */
    @Test(expected=JMSException.class)
    public void testMaximumAnonymousProducersAllowedPerConnection() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(brokerConnectURI).createConnection();
        connection.start();
        Session session = connection.createSession(false, TopicSession.AUTO_ACKNOWLEDGE);
        // Verify that maximumProducersAllowedPerConnection throws an exception
        // when using anonymous producers with no destination set
        // This would previously work before AMQ-5649 was resolved
        try {
	        for (int i =0; i < MAX_PRODUCERS + 1; i++) {
	        	session.createProducer(null);
	        }
        } finally {
        	connection.stop();
        }
    }

}
