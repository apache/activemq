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
package org.apache.activemq.proxy;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.ConsumerThread;
import org.apache.activemq.util.ProducerThread;

import javax.jms.Connection;
import javax.jms.Session;
import java.net.URI;

public class ProxyFailoverTest extends TestCase {

    BrokerService proxyBroker;
    BrokerService remoteBroker;

    @Override
    protected void setUp() throws Exception {

        startRemoteBroker(true);

        proxyBroker = new BrokerService();
        ProxyConnector connector = new ProxyConnector();
        connector.setBind(new URI("tcp://localhost:51618"));
        connector.setProxyToLocalBroker(false);
        connector.setRemote(new URI("failover:(tcp://localhost:61616)"));
        proxyBroker.addProxyConnector(connector);
        proxyBroker.setPersistent(false);
        proxyBroker.setUseJmx(false);
        proxyBroker.start();
        proxyBroker.waitUntilStarted();

    }

    @Override
    protected void tearDown() throws Exception {
        proxyBroker.stop();
        proxyBroker.waitUntilStopped();

        remoteBroker.stop();
        remoteBroker.waitUntilStopped();
    }

    public void testFailover() throws Exception {
        ActiveMQConnectionFactory producerFactory = new ActiveMQConnectionFactory("failover:(tcp://localhost:61616,tcp://localhost:61626)?randomize=false");
        Connection producerConnection = producerFactory.createConnection();
        producerConnection.start();
        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ProducerThread producer = new ProducerThread(producerSession, producerSession.createQueue("ProxyTest"));
        producer.setSleep(10);
        producer.start();


        //ActiveMQConnectionFactory consumerFactory = new ActiveMQConnectionFactory("tcp://localhost:51618?wireFormat.cacheEnabled=false");
        ActiveMQConnectionFactory consumerFactory = new ActiveMQConnectionFactory("tcp://localhost:51618");
        Connection consumerConnection = consumerFactory.createConnection();
        consumerConnection.start();
        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ConsumerThread consumer = new ConsumerThread(consumerSession, consumerSession.createQueue("ProxyTest"));
        consumer.start();

        Thread.sleep(10*1000);

        remoteBroker.stop();
        remoteBroker.waitUntilStopped();
        startRemoteBroker(false);

        producer.join();
        consumer.join();

        assertEquals(1000, consumer.getReceived());

    }

    protected void startRemoteBroker(boolean delete) throws Exception {
        remoteBroker = new BrokerService();
        remoteBroker.addConnector("tcp://localhost:61616");
        if (delete) {
            remoteBroker.deleteAllMessages();
        }
        remoteBroker.setUseJmx(false);
        remoteBroker.start();
        remoteBroker.waitUntilStarted();
    }
}
