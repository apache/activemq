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
package org.apache.activemq.broker.advisory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.util.ServiceStopper;
import org.junit.After;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AdvisoryDuringStartTest {

    BrokerService brokerService;

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void testConsumerAdvisoryDuringSlowStart() throws Exception {

        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.addConnector("tcp://localhost:0");

        final CountDownLatch resumeStart = new CountDownLatch(1);
        brokerService.addNetworkConnector(new DiscoveryNetworkConnector() {
            @Override
            protected void handleStart() throws Exception {
                // delay broker started flag
                resumeStart.await(5, TimeUnit.SECONDS);
            }

            @Override
            protected void handleStop(ServiceStopper s) throws Exception {}
        });
        Executors.newCachedThreadPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    brokerService.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail("error on start: " + e.toString());
                }
            }
        });
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover:(" + brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString() + ")");
        Connection advisoryConnection = connectionFactory.createConnection();
        advisoryConnection.start();
        Session advisorySession = advisoryConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer advisoryConsumer = advisorySession.createConsumer(advisorySession.createTopic("ActiveMQ.Advisory.Consumer.>"));

        Connection consumerConnection = connectionFactory.createConnection();
        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumerConnection.start();
        ActiveMQTopic dest = new ActiveMQTopic("SomeTopic");

        // real consumer
        consumerSession.createConsumer(dest);

        resumeStart.countDown();
        

        ActiveMQMessage advisory = (ActiveMQMessage)advisoryConsumer.receive(4000);
        assertNotNull(advisory);
        assertTrue(advisory.getDataStructure() instanceof ConsumerInfo);
        assertTrue(((ConsumerInfo)advisory.getDataStructure()).getDestination().equals(dest));
        advisoryConnection.close();

        consumerConnection.close();
    }

}
