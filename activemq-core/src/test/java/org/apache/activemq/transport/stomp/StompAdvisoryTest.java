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

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.ConstantPendingMessageLimitStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.URISupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @version $Revision: 1461 $
 */
public class StompAdvisoryTest extends TestCase {
    private static final Log LOG = LogFactory.getLog(StompAdvisoryTest.class);

    protected ConnectionFactory factory;
    protected ActiveMQConnection connection;
    protected BrokerService broker;

    StompConnection stompConnection;
    URI tcpBrokerUri;
    URI stompBrokerUri;

    private PolicyEntry createPolicyEntry() {
        PolicyEntry policy = new PolicyEntry();
        policy.setAdvisdoryForFastProducers(true);
        policy.setAdvisoryForConsumed(true);
        policy.setAdvisoryForDelivery(true);
        policy.setAdvisoryForDiscardingMessages(true);
        policy.setAdvisoryForSlowConsumers(true);
        policy.setAdvisoryWhenFull(true);
        policy.setProducerFlowControl(false);

        ConstantPendingMessageLimitStrategy strategy = new ConstantPendingMessageLimitStrategy();
        strategy.setLimit(10);
        policy.setPendingMessageLimitStrategy(strategy);
        return policy;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker://()/localhost?useJmx=false"));

        broker.setPersistent(false);
        PolicyEntry policy = new PolicyEntry();
        policy.setAdvisdoryForFastProducers(true);
        policy.setAdvisoryForConsumed(true);
        policy.setAdvisoryForDelivery(true);
        policy.setAdvisoryForDiscardingMessages(true);
        policy.setAdvisoryForSlowConsumers(true);
        policy.setAdvisoryWhenFull(true);
        policy.setProducerFlowControl(false);
        ConstantPendingMessageLimitStrategy strategy  = new ConstantPendingMessageLimitStrategy();
        strategy.setLimit(10);
        policy.setPendingMessageLimitStrategy(strategy);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);

        broker.setDestinationPolicy(pMap);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.addConnector("tcp://localhost:0");
        broker.addConnector("stomp://localhost:0");
        return broker;
    }

    protected void setUp() throws Exception {
        super.setUp();

        if (System.getProperty("basedir") == null) {
            File file = new File(".");
            System.setProperty("basedir", file.getAbsolutePath());
        }

        broker = createBroker();
        broker.start();

        tcpBrokerUri = URISupport.removeQuery(broker.getTransportConnectors().get(0).getConnectUri());
        stompBrokerUri = URISupport.removeQuery(broker.getTransportConnectors().get(1).getConnectUri());
        LOG.info("Producing using TCP uri: " + tcpBrokerUri);
        LOG.info("consuming using STOMP uri: " + stompBrokerUri);

        stompConnection = new StompConnection();
        stompConnection.open(new Socket("localhost", stompBrokerUri.getPort()));

    }

    protected void tearDown() throws Exception {
        stompConnection.disconnect();
        stompConnection.close();
        broker.stop();

    }

    public void testConnectionAdvisory() throws Exception {

        Destination dest = new ActiveMQQueue("testConnectionAdvisory");

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Connection", Stomp.Headers.Subscribe.AckModeValues.AUTO);

        // Now connect via openwire and check we get the advisory
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(tcpBrokerUri);
        Connection c = factory.createConnection();
        c.start();

        StompFrame f = stompConnection.receive();
        LOG.debug(f);
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ConnectionInfo\":"));
        Map<String,String> headers = f.getHeaders();

        c.stop();
        c.close();

        f = stompConnection.receive();
        LOG.debug(f);
        assertEquals(f.getAction(),"MESSAGE");
        assertNotNull("Body is not null", f.getBody());
        assertTrue("Body should have content", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ConnectionInfo\":"));
    }

    public void testConnectionAdvisoryJSON() throws Exception {

        Destination dest = new ActiveMQQueue("testConnectionAdvisory");

        HashMap<String, String> subheaders = new HashMap<String, String>(1);
        subheaders.put("transformation", Stomp.Transformations.JMS_JSON.toString());

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Connection",
        		Stomp.Headers.Subscribe.AckModeValues.AUTO, subheaders);

        // Now connect via openwire and check we get the advisory
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(tcpBrokerUri);
        Connection c = factory.createConnection();
        c.start();

        StompFrame f = stompConnection.receive();
        LOG.debug(f);
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ConnectionInfo\":"));
        Map<String,String> headers = f.getHeaders();

        c.stop();
        c.close();

        f = stompConnection.receive();
        LOG.debug(f);
        assertEquals(f.getAction(),"MESSAGE");
        assertNotNull("Body is not null", f.getBody());
        assertTrue("Body should have content", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ConnectionInfo\":"));
    }

    public void testConnectionAdvisoryXML() throws Exception {

        Destination dest = new ActiveMQQueue("testConnectionAdvisory");

        HashMap<String, String> subheaders = new HashMap<String, String>(1);
        subheaders.put("transformation", Stomp.Transformations.JMS_XML.toString());

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Connection",
        		Stomp.Headers.Subscribe.AckModeValues.AUTO, subheaders);

        // Now connect via openwire and check we get the advisory
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(tcpBrokerUri);
        Connection c = factory.createConnection();
        c.start();

        StompFrame f = stompConnection.receive();
        LOG.debug(f);
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("<ConnectionInfo>"));
        Map<String,String> headers = f.getHeaders();

        c.stop();
        c.close();

        f = stompConnection.receive();
        LOG.debug(f);
        assertEquals(f.getAction(),"MESSAGE");
        assertNotNull("Body is not null", f.getBody());
        assertTrue("Body should have content", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("<ConnectionInfo>"));
    }

    public void testConsumerAdvisory() throws Exception {

        Destination dest = new ActiveMQQueue("testConsumerAdvisory");

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Consumer.>", Stomp.Headers.Subscribe.AckModeValues.AUTO);

        // Now connect via openwire and check we get the advisory
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(tcpBrokerUri);
        Connection c = factory.createConnection();
        c.start();

        Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(dest);

        StompFrame f = stompConnection.receive();
        LOG.debug(f);
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ConsumerInfo\":"));

        c.stop();
        c.close();
    }

    public void testProducerAdvisory() throws Exception {

        Destination dest = new ActiveMQQueue("testProducerAdvisory");

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Producer.>", Stomp.Headers.Subscribe.AckModeValues.AUTO);

        // Now connect via openwire and check we get the advisory
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(tcpBrokerUri);
        Connection c = factory.createConnection();
        c.start();

        Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(dest);
        Message mess = session.createTextMessage("test");
        producer.send(mess);

        StompFrame f = stompConnection.receive();
        LOG.debug(f);
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ProducerInfo\":"));

        c.stop();
        c.close();
    }

    public void testProducerAdvisoryXML() throws Exception {

        Destination dest = new ActiveMQQueue("testProducerAdvisoryXML");

        HashMap<String, String> subheaders = new HashMap<String, String>(1);
        subheaders.put("transformation", Stomp.Transformations.JMS_ADVISORY_XML.toString());

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Producer.>",
        		Stomp.Headers.Subscribe.AckModeValues.AUTO, subheaders);

        // Now connect via openwire and check we get the advisory
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(tcpBrokerUri);
        Connection c = factory.createConnection();
        c.start();

        Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(dest);
        Message mess = session.createTextMessage("test");
        producer.send(mess);

        StompFrame f = stompConnection.receive();
        LOG.debug(f);
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("<ProducerInfo>"));

        c.stop();
        c.close();
    }

    public void testProducerAdvisoryJSON() throws Exception {

        Destination dest = new ActiveMQQueue("testProducerAdvisoryJSON");

        HashMap<String, String> subheaders = new HashMap<String, String>(1);
        subheaders.put("transformation", Stomp.Transformations.JMS_ADVISORY_JSON.toString());

        stompConnection.connect("system", "manager");
        stompConnection.subscribe("/topic/ActiveMQ.Advisory.Producer.>",
        		Stomp.Headers.Subscribe.AckModeValues.AUTO, subheaders);

        // Now connect via openwire and check we get the advisory
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(tcpBrokerUri);
        Connection c = factory.createConnection();
        c.start();

        Session session = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(dest);
        Message mess = session.createTextMessage("test");
        producer.send(mess);

        StompFrame f = stompConnection.receive();
        LOG.debug(f);
        assertEquals(f.getAction(),"MESSAGE");
        assertTrue("Should have a body", f.getBody().length() > 0);
        assertTrue(f.getBody().startsWith("{\"ProducerInfo\":"));

        c.stop();
        c.close();
    }

}
