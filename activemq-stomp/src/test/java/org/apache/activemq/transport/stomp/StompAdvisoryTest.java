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

package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StompAdvisoryTest extends StompTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(StompAdvisoryTest.class);

    protected ActiveMQConnection connection;

    @Override
    protected void applyBrokerPolicies() throws Exception {

        PolicyEntry policy = new PolicyEntry();
        policy.setAdvisoryForFastProducers(true);
        policy.setAdvisoryForConsumed(true);
        policy.setAdvisoryForDelivery(true);
        policy.setAdvisoryForDiscardingMessages(true);
        policy.setAdvisoryForSlowConsumers(true);
        policy.setAdvisoryWhenFull(true);
        policy.setProducerFlowControl(false);

        ConstantPendingMessageLimitStrategy strategy = new ConstantPendingMessageLimitStrategy();
        strategy.setLimit(10);
        policy.setPendingMessageLimitStrategy(strategy);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        brokerService.setDestinationPolicy(pMap);
        brokerService.setAdvisorySupport(true);
    }

    @Test
    public void testConnectionAdvisory() throws Exception {

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Connection", Stomp.Headers.Subscribe.AckModeValues.AUTO);

        // Now connect via openwire and check we get the advisory
        Connection c = cf.createConnection("system", "manager");
        c.start();

        StompFrame f = stompConnection.receive();
        LOG.debug(f.toString());
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ConnectionInfo\":"));

        c.stop();
        c.close();

        f = stompConnection.receive();
        LOG.debug(f.toString());
        assertEquals(f.getAction(),"MESSAGE");
        assertNotNull("Body is not null", f.getBody());
        assertTrue("Body should have content", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ConnectionInfo\":"));
    }

    @Test
    public void testConnectionAdvisoryJSON() throws Exception {

        HashMap<String, String> subheaders = new HashMap<String, String>(1);
        subheaders.put("transformation", Stomp.Transformations.JMS_JSON.toString());

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Connection",
                Stomp.Headers.Subscribe.AckModeValues.AUTO, subheaders);

        // Now connect via openwire and check we get the advisory
        Connection c = cf.createConnection("system", "manager");
        c.start();

        StompFrame f = stompConnection.receive();
        LOG.debug(f.toString());
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ConnectionInfo\":"));

        c.stop();
        c.close();

        f = stompConnection.receive();
        LOG.debug(f.toString());
        assertEquals(f.getAction(),"MESSAGE");
        assertNotNull("Body is not null", f.getBody());
        assertTrue("Body should have content", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ConnectionInfo\":"));
    }

    @Test
    public void testConnectionAdvisoryXML() throws Exception {

        HashMap<String, String> subheaders = new HashMap<String, String>(1);
        subheaders.put("transformation", Stomp.Transformations.JMS_XML.toString());

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Connection",
                Stomp.Headers.Subscribe.AckModeValues.AUTO, subheaders);

        // Now connect via openwire and check we get the advisory
        Connection c = cf.createConnection("system", "manager");
        c.start();

        StompFrame f = stompConnection.receive();
        LOG.debug(f.toString());
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("<ConnectionInfo>"));

        c.stop();
        c.close();

        f = stompConnection.receive();
        LOG.debug(f.toString());
        assertEquals(f.getAction(),"MESSAGE");
        assertNotNull("Body is not null", f.getBody());
        assertTrue("Body should have content", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("<ConnectionInfo>"));
    }

    @Test
    public void testConsumerAdvisory() throws Exception {

        Destination dest = new ActiveMQQueue("testConsumerAdvisory");

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Consumer.>", Stomp.Headers.Subscribe.AckModeValues.AUTO);

        // Now connect via openwire and check we get the advisory
        Connection c = cf.createConnection("system", "manager");
        c.start();

        Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(dest);
        assertNotNull(consumer);

        StompFrame f = stompConnection.receive();
        LOG.debug(f.toString());
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ConsumerInfo\":"));

        c.stop();
        c.close();
    }

    @Test
    public void testProducerAdvisory() throws Exception {

        Destination dest = new ActiveMQQueue("testProducerAdvisory");

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Producer.>", Stomp.Headers.Subscribe.AckModeValues.AUTO);

        // Now connect via openwire and check we get the advisory
        Connection c = cf.createConnection("system", "manager");
        c.start();

        Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(dest);
        Message mess = session.createTextMessage("test");
        producer.send(mess);

        StompFrame f = stompConnection.receive();
        LOG.debug(f.toString());
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ProducerInfo\":"));

        c.stop();
        c.close();
    }

    @Test
    public void testProducerAdvisoryXML() throws Exception {

        Destination dest = new ActiveMQQueue("testProducerAdvisoryXML");

        HashMap<String, String> subheaders = new HashMap<String, String>(1);
        subheaders.put("transformation", Stomp.Transformations.JMS_ADVISORY_XML.toString());

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Producer.>",
                Stomp.Headers.Subscribe.AckModeValues.AUTO, subheaders);

        // Now connect via openwire and check we get the advisory
        Connection c = cf.createConnection("system", "manager");
        c.start();

        Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(dest);
        Message mess = session.createTextMessage("test");
        producer.send(mess);

        StompFrame f = stompConnection.receive();
        LOG.debug(f.toString());
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("<ProducerInfo>"));

        c.stop();
        c.close();
    }

    @Test
    public void testProducerAdvisoryJSON() throws Exception {

        Destination dest = new ActiveMQQueue("testProducerAdvisoryJSON");

        HashMap<String, String> subheaders = new HashMap<String, String>(1);
        subheaders.put("transformation", Stomp.Transformations.JMS_ADVISORY_JSON.toString());

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Producer.>",
                Stomp.Headers.Subscribe.AckModeValues.AUTO, subheaders);

        // Now connect via openwire and check we get the advisory
        Connection c = cf.createConnection("system", "manager");
        c.start();

        Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(dest);
        Message mess = session.createTextMessage("test");
        producer.send(mess);

        StompFrame f = stompConnection.receive();
        LOG.debug(f.toString());
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ProducerInfo\":"));

        c.stop();
        c.close();
    }
}
