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
package org.apache.activemq.broker.util;

import java.net.URI;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.test.JmsTopicSendReceiveTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 
 */
public class PluginBrokerTest extends JmsTopicSendReceiveTest {
    private static final Logger LOG = LoggerFactory.getLogger(PluginBrokerTest.class);
    private BrokerService broker;

    protected void setUp() throws Exception {
        broker = createBroker();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }   
    }

    protected BrokerService createBroker() throws Exception {
        return createBroker("org/apache/activemq/util/plugin-broker.xml");
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }

	protected void assertMessageValid(int index, Message message)
			throws JMSException {
		// check if broker path has been set 
		assertEquals("localhost", message.getStringProperty("BrokerPath"));
		ActiveMQMessage amqMsg = (ActiveMQMessage)message;
		if (index == 7) {
			// check custom expiration
			assertTrue("expiration is in range, depends on two distinct calls to System.currentTimeMillis", 1500 < amqMsg.getExpiration() - amqMsg.getTimestamp());
		} else if (index == 9) {
			// check ceiling
			assertTrue("expiration ceeling is in range, depends on two distinct calls to System.currentTimeMillis", 59500 < amqMsg.getExpiration() - amqMsg.getTimestamp());
		} else {
			// check default expiration
			assertEquals(1000, amqMsg.getExpiration() - amqMsg.getTimestamp());
		}
		super.assertMessageValid(index, message);
	}
	
    protected void sendMessage(int index, Message message) throws Exception {
    	if (index == 7) {
    		producer.send(producerDestination, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, 2000);
    	} else if (index == 9) {
    		producer.send(producerDestination, message, Message.DEFAULT_DELIVERY_MODE, Message.DEFAULT_PRIORITY, 200000);
    	} else {
    		super.sendMessage(index, message);
    	}
    }
    
}
