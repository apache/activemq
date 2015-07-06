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

import static org.junit.Assert.assertEquals;

import java.io.File;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4814Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ4814Test.class);

    private static final String CONNECTION_URL = "tcp://127.0.0.1:0";
    private static final String KAHADB_DIRECTORY = "./target/activemq-data/";

    private BrokerService broker;
    private String connectionURI;

    @Before
    public void setup() throws Exception {

        PolicyMap pm = new PolicyMap();
        PolicyEntry pe = new PolicyEntry();
        pe.setGcInactiveDestinations(true);
        pe.setInactiveTimeoutBeforeGC(1000L);

        pe.setProducerFlowControl(false);

        ActiveMQDestination d = new ActiveMQTopic(">");
        pe.setDestination(d);
        pm.put(d, pe);

        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File(KAHADB_DIRECTORY + "-LEAKTEST"));

        broker = new BrokerService();
        broker.setBrokerName("broker1");
        broker.setUseJmx(false);
        broker.setPersistenceAdapter(kaha);
        broker.setDestinationPolicy(pm);
        broker.setSchedulePeriodForDestinationPurge(1000);
        broker.setTimeBeforePurgeTempDestinations(1000);
        broker.setMaxPurgedDestinationsPerSweep(5000);
        broker.setOfflineDurableSubscriberTaskSchedule(1000L);
        broker.setOfflineDurableSubscriberTimeout(1000L);
        broker.setKeepDurableSubsActive(true);

        TransportConnector connector = broker.addConnector(CONNECTION_URL);

        broker.deleteAllMessages();
        broker.start();
        broker.waitUntilStarted();

        connectionURI = connector.getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }

    @Test(timeout=60000)
    public void testDurableTopicResourcesAreRemoved() throws Exception {

        LOG.info("Test starting.");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionURI);

        for (int i = 0; i < 2; ++i) {
            LOG.info("Test main loop starting iteration: {}", i + 1);
            Connection connection = factory.createConnection();
            connection.setClientID("client_id");
            connection.start();

            for (int j = 0; j < 8; j++) {
                LOG.info("Test sub loop starting iteration: {}", j + 1);
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                String topicName = "durabletopic_" + j;
                String subscriberName = "subscriber_" + j;
                Topic topic = session.createTopic(topicName);

                TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriberName);
                subscriber.close();
                session.unsubscribe(subscriberName);
                session.close();
            }

            connection.stop();
            connection.close();
            connection = null;

            Thread.sleep(10);
        }

        assertEquals(0, broker.getSystemUsage().getMemoryUsage().getNumUsageListeners());

        LOG.info("Test completed.");
    }
}
