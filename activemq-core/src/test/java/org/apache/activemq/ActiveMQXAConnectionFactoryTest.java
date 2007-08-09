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

import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XATopicConnection;

import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.stomp.StompTransportFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ActiveMQXAConnectionFactoryTest extends CombinationTestSupport {
    private static final Log LOG = LogFactory.getLog(ActiveMQXAConnectionFactoryTest.class);

    public void testCopy() throws URISyntaxException, JMSException {
        ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory("vm://localhost?");
        ActiveMQConnectionFactory copy = cf.copy();
        assertTrue("Should be an ActiveMQXAConnectionFactory", copy instanceof ActiveMQXAConnectionFactory);
    }

    public void testUseURIToSetOptionsOnConnectionFactory() throws URISyntaxException, JMSException {
        ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory(
                                                                         "vm://localhost?jms.useAsyncSend=true");
        assertTrue(cf.isUseAsyncSend());
        // the broker url have been adjusted.
        assertEquals("vm://localhost", cf.getBrokerURL());

        cf = new ActiveMQXAConnectionFactory("vm://localhost?jms.useAsyncSend=false");
        assertFalse(cf.isUseAsyncSend());
        // the broker url have been adjusted.
        assertEquals("vm://localhost", cf.getBrokerURL());

        cf = new ActiveMQXAConnectionFactory("vm:(broker:()/localhost)?jms.useAsyncSend=true");
        assertTrue(cf.isUseAsyncSend());
        // the broker url have been adjusted.
        assertEquals("vm:(broker:()/localhost)", cf.getBrokerURL());
    }

    public void testCreateVMConnectionWithEmbdeddBroker() throws URISyntaxException, JMSException {
        ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory(
                                                                         "vm://localhost?broker.persistent=false");
        // Make sure the broker is not created until the connection is
        // instantiated.
        assertNull(BrokerRegistry.getInstance().lookup("localhost"));
        Connection connection = cf.createConnection();
        // This should create the connection.
        assertNotNull(connection);
        // Verify the broker was created.
        assertNotNull(BrokerRegistry.getInstance().lookup("localhost"));
        connection.close();
        // Verify the broker was destroyed.
        assertNull(BrokerRegistry.getInstance().lookup("localhost"));
    }

    public void testGetBrokerName() throws URISyntaxException, JMSException {
        ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory(
                                                                         "vm://localhost?broker.persistent=false");
        ActiveMQConnection connection = (ActiveMQConnection)cf.createConnection();
        connection.start();

        String brokerName = connection.getBrokerName();
        LOG.info("Got broker name: " + brokerName);

        assertNotNull("No broker name available!", brokerName);
        connection.close();
    }

    public void testCreateTcpConnectionUsingAllocatedPort() throws Exception {
        assertCreateConnection("tcp://localhost:0?wireFormat.tcpNoDelayEnabled=true");
    }

    public void testCreateTcpConnectionUsingKnownPort() throws Exception {
        assertCreateConnection("tcp://localhost:61610?wireFormat.tcpNoDelayEnabled=true");
    }

    protected void assertCreateConnection(String uri) throws Exception {
        // Start up a broker with a tcp connector.
        BrokerService broker = new BrokerService();
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
        ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory(connectURI);
        Connection connection = cf.createConnection();

        assertXAConnection(connection);

        assertNotNull(connection);
        connection.close();

        connection = cf.createXAConnection();

        assertXAConnection(connection);

        assertNotNull(connection);
        connection.close();

        broker.stop();
    }

    private void assertXAConnection(Connection connection) {
        assertTrue("Should be an XAConnection", connection instanceof XAConnection);
        assertTrue("Should be an XATopicConnection", connection instanceof XATopicConnection);
        assertTrue("Should be an XAQueueConnection", connection instanceof XAQueueConnection);
    }

}
