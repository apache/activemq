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
package org.apache.activemq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ActiveMQConnectionFactoryTest extends CombinationTestSupport {
    private static final Log LOG = LogFactory.getLog(ActiveMQConnectionFactoryTest.class);

    private ActiveMQConnection connection;
    private BrokerService broker;

    public void testUseURIToSetUseClientIDPrefixOnConnectionFactory() throws URISyntaxException, JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
                                                                     "vm://localhost?jms.clientIDPrefix=Cheese");
        assertEquals("Cheese", cf.getClientIDPrefix());

        connection = (ActiveMQConnection)cf.createConnection();
        connection.start();

        String clientID = connection.getClientID();
        LOG.info("Got client ID: " + clientID);

        assertTrue("should start with Cheese! but was: " + clientID, clientID.startsWith("Cheese"));
    }

    protected void tearDown() throws Exception {
        // Try our best to close any previously opend connection.
        try {
            connection.close();
        } catch (Throwable ignore) {
        }
        // Try our best to stop any previously started broker.
        try {
            broker.stop();
        } catch (Throwable ignore) {
        }
    }

    public void testUseURIToSetOptionsOnConnectionFactory() throws URISyntaxException, JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?jms.useAsyncSend=true");
        assertTrue(cf.isUseAsyncSend());
        // the broker url have been adjusted.
        assertEquals("vm://localhost", cf.getBrokerURL());

        cf = new ActiveMQConnectionFactory("vm://localhost?jms.useAsyncSend=false");
        assertFalse(cf.isUseAsyncSend());
        // the broker url have been adjusted.
        assertEquals("vm://localhost", cf.getBrokerURL());

        cf = new ActiveMQConnectionFactory("vm:(broker:()/localhost)?jms.useAsyncSend=true");
        assertTrue(cf.isUseAsyncSend());
        // the broker url have been adjusted.
        assertEquals("vm:(broker:()/localhost)", cf.getBrokerURL());
    }

    public void testUseURIToConfigureRedeliveryPolicy() throws URISyntaxException, JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(
                                                                     "vm://localhost?jms.redeliveryPolicy.maximumRedeliveries=2");
        assertEquals("connection redeliveries", 2, cf.getRedeliveryPolicy().getMaximumRedeliveries());

        ActiveMQConnection connection = (ActiveMQConnection)cf.createConnection();
        assertEquals("connection redeliveries", 2, connection.getRedeliveryPolicy().getMaximumRedeliveries());

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer)session.createConsumer(session
            .createQueue("FOO.BAR"));
        assertEquals("consumer redeliveries", 2, consumer.getRedeliveryPolicy().getMaximumRedeliveries());
        connection.close();
    }

    public void testCreateVMConnectionWithEmbdeddBroker() throws URISyntaxException, JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        // Make sure the broker is not created until the connection is
        // instantiated.
        assertNull(BrokerRegistry.getInstance().lookup("localhost"));
        connection = (ActiveMQConnection)cf.createConnection();
        // This should create the connection.
        assertNotNull(connection);
        // Verify the broker was created.
        assertNotNull(BrokerRegistry.getInstance().lookup("localhost"));

        connection.close();

        // Verify the broker was destroyed.
        assertNull(BrokerRegistry.getInstance().lookup("localhost"));
    }

    public void testGetBrokerName() throws URISyntaxException, JMSException {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = (ActiveMQConnection)cf.createConnection();
        connection.start();

        String brokerName = connection.getBrokerName();
        LOG.info("Got broker name: " + brokerName);

        assertNotNull("No broker name available!", brokerName);
    }

    public void testCreateTcpConnectionUsingAllocatedPort() throws Exception {
        assertCreateConnection("tcp://localhost:0?wireFormat.tcpNoDelayEnabled=true");
    }

    public void testCreateTcpConnectionUsingKnownPort() throws Exception {
        assertCreateConnection("tcp://localhost:61610?wireFormat.tcpNoDelayEnabled=true");
    }

    public void testConnectionFailsToConnectToVMBrokerThatIsNotRunning() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost?create=false");
        try {
            factory.createConnection();
            fail("Expected connection failure.");
        } catch (JMSException e) {
        }
    }

    public void testFactorySerializable() throws Exception {
        String clientID = "TestClientID";
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory();
        cf.setClientID(clientID);
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        ObjectOutputStream objectsOut = new ObjectOutputStream(bytesOut);
        objectsOut.writeObject(cf);
        objectsOut.flush();
        byte[] data = bytesOut.toByteArray();
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
        ObjectInputStream objectsIn = new ObjectInputStream(bytesIn);
        cf = (ActiveMQConnectionFactory)objectsIn.readObject();
        assertEquals(cf.getClientID(), clientID);
    }
    
    public void testSetExceptionListener() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
        connection = (ActiveMQConnection)cf.createConnection();
        assertNull(connection.getExceptionListener());
        
        ExceptionListener exListener = new ExceptionListener() {
			public void onException(JMSException arg0) {
			}
        };
        cf.setExceptionListener(exListener);
        
        connection = (ActiveMQConnection)cf.createConnection();
        assertNotNull(connection.getExceptionListener());
        assertEquals(exListener, connection.getExceptionListener());
        
        connection = (ActiveMQConnection)cf.createConnection();
        assertEquals(exListener, connection.getExceptionListener());
        
        assertEquals(exListener, cf.getExceptionListener());
        
    }

    protected void assertCreateConnection(String uri) throws Exception {
        // Start up a broker with a tcp connector.
        broker = new BrokerService();
        broker.setPersistent(false);
        TransportConnector connector = broker.addConnector(uri);
        broker.start();

        URI temp = new URI(uri);
        // URI connectURI = connector.getServer().getConnectURI();
        // TODO this sometimes fails when using the actual local host name
        URI currentURI = connector.getServer().getConnectURI();

        // sometimes the actual host name doesn't work in this test case
        // e.g. on OS X so lets use the original details but just use the actual
        // port
        URI connectURI = new URI(temp.getScheme(), temp.getUserInfo(), temp.getHost(), currentURI.getPort(),
                                 temp.getPath(), temp.getQuery(), temp.getFragment());

        LOG.info("connection URI is: " + connectURI);

        // This should create the connection.
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(connectURI);
        connection = (ActiveMQConnection)cf.createConnection();
        assertNotNull(connection);

        broker.stop();
    }

}
