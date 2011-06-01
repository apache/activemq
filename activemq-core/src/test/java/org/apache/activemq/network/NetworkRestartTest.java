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

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import javax.jms.*;

public class NetworkRestartTest extends TestCase {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkRestartTest.class);

    protected Connection localConnection;
    protected Connection remoteConnection;
    protected BrokerService localBroker;
    protected BrokerService remoteBroker;
    protected Session localSession;
    protected Session remoteSession;

    protected ActiveMQQueue included=new ActiveMQQueue("include.test.foo");


    public void testConnectorRestart() throws Exception {
        MessageConsumer remoteConsumer = remoteSession.createConsumer(included);
        MessageProducer localProducer = localSession.createProducer(included);

        localProducer.send(localSession.createTextMessage("before"));
        Message before = remoteConsumer.receive(1000);
        assertNotNull(before);
        assertEquals("before", ((TextMessage)before).getText());

        // restart connector

        NetworkConnector connector = localBroker.getNetworkConnectorByName("networkConnector");

        LOG.info("Stopping connector");
        connector.stop();

        Thread.sleep(5000);
        LOG.info("Starting connector");
        connector.start();

        Thread.sleep(5000);


        localProducer.send(localSession.createTextMessage("after"));
        Message after = remoteConsumer.receive(1000);
        assertNotNull(after);
        assertEquals("after", ((TextMessage)after).getText());

    }


    protected void setUp() throws Exception {
        super.setUp();
        doSetUp();
    }

    protected void tearDown() throws Exception {
        localBroker.deleteAllMessages();
        remoteBroker.deleteAllMessages();
        doTearDown();
        super.tearDown();
    }

    protected void doTearDown() throws Exception {
        localConnection.close();
        remoteConnection.close();
        localBroker.stop();
        localBroker.waitUntilStopped();
        remoteBroker.stop();
        remoteBroker.waitUntilStopped();
    }

    protected void doSetUp() throws Exception {

        remoteBroker = createRemoteBroker();
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
        localBroker = createLocalBroker();
        localBroker.start();
        localBroker.waitUntilStarted();

        String localURI = "tcp://localhost:61616";
        String remoteURI = "tcp://localhost:61617";
        ActiveMQConnectionFactory fac = new ActiveMQConnectionFactory(localURI);
        localConnection = fac.createConnection();
        localConnection.setClientID("local");
        localConnection.start();

        fac = new ActiveMQConnectionFactory(remoteURI);
        fac.setWatchTopicAdvisories(false);
        remoteConnection = fac.createConnection();
        remoteConnection.setClientID("remote");
        remoteConnection.start();

        localSession = localConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected String getRemoteBrokerURI() {
        return "org/apache/activemq/network/remoteBroker.xml";
    }

    protected String getLocalBrokerURI() {
        return "org/apache/activemq/network/localBroker.xml";
    }

    protected BrokerService createBroker(String uri) throws Exception {
        Resource resource = new ClassPathResource(uri);
        BrokerFactoryBean factory = new BrokerFactoryBean(resource);
        resource = new ClassPathResource(uri);
        factory = new BrokerFactoryBean(resource);
        factory.afterPropertiesSet();
        BrokerService result = factory.getBroker();
        return result;
    }

    protected BrokerService createLocalBroker() throws Exception {
        return createBroker(getLocalBrokerURI());
    }

    protected BrokerService createRemoteBroker() throws Exception {
        return createBroker(getRemoteBrokerURI());
    }

}
