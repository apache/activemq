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
package org.apache.activemq.usecases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.*;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.jms.Destination;
import javax.jms.Topic;
import java.net.URI;

/**
 * Created by ceposta
 * <a href="http://christianposta.com/blog>http://christianposta.com/blog</a>.
 */
public class AMQ5153LevelDBSubscribedDestTest extends org.apache.activemq.TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ5153LevelDBSubscribedDestTest.class);
    protected BrokerService brokerService;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        brokerService = createBroker();
        getConnectionFactory().setClientID(getName());
    }

    @Override
    protected void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost");
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:()/localhost?persistent=true"));
        setPersistenceAdapter(broker, PersistenceAdapterChoice.LevelDB);
        broker.deleteAllMessages();
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }
    
    @Test
    public void testWildcardDurableSubscriptions() throws Exception {

        Destination wildcardJmsDest = createDestination("testing.durable.>");
        Destination testJmsDest = createDestination("testing.durable.test");

        Connection conn = createConnection();
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer wildcardConsumer = sess.createDurableSubscriber((Topic) wildcardJmsDest, "durable1");
        MessageConsumer testConsumer = sess.createDurableSubscriber((Topic) testJmsDest, "durable2");


        MessageProducer producer = sess.createProducer(createDestination("testing.durable.>"));
        producer.send(sess.createTextMessage("hello!"));

        org.apache.activemq.broker.region.Topic wildcardDest = (org.apache.activemq.broker.region.Topic) getDestination(brokerService, ActiveMQDestination.transform(wildcardJmsDest));
        org.apache.activemq.broker.region.Topic testDest = (org.apache.activemq.broker.region.Topic) getDestination(brokerService, ActiveMQDestination.transform(testJmsDest));


        wildcardConsumer.close();
        testConsumer.close();
        conn.close();

        assertEquals(1, wildcardDest.getDurableTopicSubs().size());
        assertEquals(2, testDest.getDurableTopicSubs().size());

        LOG.info("Stopping broker...");
        brokerService.stop();
        brokerService.waitUntilStopped();

        setPersistenceAdapter(brokerService, PersistenceAdapterChoice.LevelDB);
        brokerService.start(true);
        brokerService.waitUntilStarted();


        wildcardDest = (org.apache.activemq.broker.region.Topic) getDestination(brokerService, ActiveMQDestination.transform(wildcardJmsDest));
        assertNotNull(wildcardDest);
        testDest = (org.apache.activemq.broker.region.Topic) getDestination(brokerService, ActiveMQDestination.transform(testJmsDest));
        assertNotNull(testDest);

        assertEquals(2, testDest.getDurableTopicSubs().size());
        assertEquals(1, wildcardDest.getDurableTopicSubs().size());

    }
}
