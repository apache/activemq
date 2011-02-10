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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicRequestor;
import javax.jms.TopicSession;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class NetworkConnectionsCleanedupTest extends TestCase {

    protected static final int MESSAGE_COUNT = 10;
    private static final Logger LOG = LoggerFactory.getLogger(NetworkConnectionsCleanedupTest.class);

    protected AbstractApplicationContext context;
    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    protected Session localSession;
    protected Session remoteSession;
    protected ActiveMQTopic included;
    protected ActiveMQTopic excluded;
    protected String consumerName = "durableSubs";

    public void testNetworkConnections() throws Exception {
    	String uri = "static:(tcp://localhost:61617)?initialReconnectDelay=100";
    	List<ActiveMQDestination> list = new ArrayList<ActiveMQDestination>();
    	for (int i =0;i < 100;i++){
    	    list.add(new ActiveMQTopic("FOO"+i));
    	}
    	String bindAddress = "tcp://localhost:61616";
		BrokerService broker = new BrokerService();
		broker.setUseJmx(false);
		broker.setPersistent(false);
		broker.addConnector(bindAddress);
		NetworkConnector network = broker.addNetworkConnector(uri);
		network.setDynamicOnly(true);
		network.setStaticallyIncludedDestinations(list);
		uri = "static:(tcp://localhost:61618)?initialReconnectDelay=100";
		network = broker.addNetworkConnector(uri);
        network.setDynamicOnly(true);
        network.setStaticallyIncludedDestinations(list);
		broker.setUseShutdownHook(false);
		broker.start();
		Thread.sleep(1000 * 3600);
    }

    

}
