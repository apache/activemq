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
package org.apache.activemq.ra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.TopicSubscriber;
import javax.transaction.xa.XAResource;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQTopicSubscriber;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.Wait;
import org.junit.Before;
import org.junit.Test;

public class ActiveMQConnectionFactoryTest {

    private ActiveMQManagedConnectionFactory mcf;
    private ActiveMQConnectionRequestInfo info;
    private String url = "vm://localhost?broker.persistent=false";
    private String user = "defaultUser";
    private String pwd = "defaultPasswd";

    @Before
    public void setUp() throws Exception {
        mcf = new ActiveMQManagedConnectionFactory();
        info = new ActiveMQConnectionRequestInfo();
        info.setServerUrl(url);
        info.setUserName(user);
        info.setPassword(pwd);
        info.setAllPrefetchValues(new Integer(100));
    }

    @Test(timeout = 60000)
    public void testSerializability() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(mcf, new ConnectionManagerAdapter(), info);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(factory);
        oos.close();
        byte[] byteArray = bos.toByteArray();

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray));
        ActiveMQConnectionFactory deserializedFactory = (ActiveMQConnectionFactory) ois.readObject();
        ois.close();

        Connection con = deserializedFactory.createConnection("defaultUser", "defaultPassword");
        ActiveMQConnection connection = ((ActiveMQConnection) ((ManagedConnectionProxy) con).getManagedConnection().getPhysicalConnection());
        assertEquals(100, connection.getPrefetchPolicy().getQueuePrefetch());
        assertNotNull("Connection object returned by ActiveMQConnectionFactory.createConnection() is null", con);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testOptimizeDurablePrefetch() throws Exception {
        ActiveMQResourceAdapter ra = new ActiveMQResourceAdapter();
        ra.setServerUrl(url);
        ra.setUserName(user);
        ra.setPassword(pwd);

        ra.setOptimizeDurableTopicPrefetch(0);
        ra.setDurableTopicPrefetch(0);

        Connection con = ra.makeConnection();

        con.setClientID("x");
        Session sess = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber sub = sess.createDurableSubscriber(sess.createTopic("TEST"), "x");
        con.start();

        assertEquals(0, ((ActiveMQTopicSubscriber) sub).getPrefetchNumber());

        con.close();
        ra.stop();
    }

    @Test(timeout = 60000)
    public void testGetXAResource() throws Exception {
        ActiveMQResourceAdapter ra = new ActiveMQResourceAdapter();
        ra.start(null);
        ra.setServerUrl(url);
        ra.setUserName(user);
        ra.setPassword(pwd);

        XAResource[] resources = ra.getXAResources(null);
        assertEquals("one resource", 1, resources.length);

        assertEquals("no pending transactions", 0, resources[0].recover(100).length);

        // validate equality
        XAResource[] resource2 = ra.getXAResources(null);
        assertEquals("one resource", 1, resource2.length);
        assertTrue("isSameRM true", resources[0].isSameRM(resource2[0]));
        assertTrue("the same instance", resources[0].equals(resource2[0]));

        ra.stop();
    }


    @Test
    public void testXAResourceReconnect() throws Exception {

        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        try {
            final TransportConnector transportConnector = brokerService.getTransportConnectors().get(0);

            String failoverUrl = String.format("failover:(%s)?maxReconnectAttempts=1", transportConnector.getConnectUri());

            ActiveMQResourceAdapter ra = new ActiveMQResourceAdapter();
            ra.start(null);
            ra.setServerUrl(failoverUrl);
            ra.setUserName(user);
            ra.setPassword(pwd);

            XAResource[] resources = ra.getXAResources(null);
            assertEquals("one resource", 1, resources.length);

            assertEquals("no pending transactions", 0, resources[0].recover(100).length);

            transportConnector.stop();
            assertTrue("no connections", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return transportConnector.getConnections().isEmpty();
                }
            }));

            try {
                resources[0].recover(100);
                fail("Expect error on broken connection");
            } catch (Exception expected) {
            }

            transportConnector.start();

            // should recover ok
            assertEquals("no pending transactions", 0, resources[0].recover(100).length);

            ra.stop();
        } finally {
            brokerService.stop();
        }
    }

    @Test
    public void testXAResourceFailoverFailBack() throws Exception {

        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        try {

            final TransportConnector primary = brokerService.getTransportConnectors().get(0);
            final TransportConnector secondary = brokerService.getTransportConnectors().get(1);

            String failoverUrl = String.format("failover:(%s,%s)?maxReconnectAttempts=1&randomize=false", primary.getConnectUri(), secondary.getConnectUri());

            ActiveMQResourceAdapter ra = new ActiveMQResourceAdapter();
            ra.start(null);
            ra.setServerUrl(failoverUrl);
            ra.setUserName(user);
            ra.setPassword(pwd);

            XAResource[] resources = ra.getXAResources(null);
            assertEquals("one resource", 1, resources.length);

            assertEquals("no pending transactions", 0, resources[0].recover(100).length);

            primary.stop();

            // should recover ok
            assertEquals("no pending transactions", 0, resources[0].recover(100).length);

            primary.start();

            // should be ok
            assertEquals("no pending transactions", 0, resources[0].recover(100).length);

            secondary.stop();

            // should recover ok
            assertEquals("no pending transactions", 0, resources[0].recover(100).length);

            ra.stop();

        } finally {
            brokerService.stop();
        }

    }

    @Test
    public void testXAResourceRefAfterStop() throws Exception {

        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        try {

            final TransportConnector primary = brokerService.getTransportConnectors().get(0);

            String failoverUrl = String.format("failover:(%s)?maxReconnectAttempts=1&randomize=false", primary.getConnectUri());

            ActiveMQResourceAdapter ra = new ActiveMQResourceAdapter();
            ra.start(null);
            ra.setServerUrl(failoverUrl);
            ra.setUserName(user);
            ra.setPassword(pwd);

            XAResource[] resources = ra.getXAResources(null);
            assertEquals("one resource", 1, resources.length);

            assertEquals("no pending transactions", 0, resources[0].recover(100).length);

            ra.stop();

            try {
                resources[0].recover(100);
                fail("Expect error on call after stop b/c of no reconnection");
            } catch (Exception expected) {
            }

        } finally {
            brokerService.stop();
        }
    }

    @Test
    public void testXAResourceRefAfterFailAndStop() throws Exception {

        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.start();

        try {

            final TransportConnector primary = brokerService.getTransportConnectors().get(0);

            String failoverUrl = String.format("failover:(%s)?maxReconnectAttempts=1&randomize=false", primary.getConnectUri());

            ActiveMQResourceAdapter ra = new ActiveMQResourceAdapter();
            ra.start(null);
            ra.setServerUrl(failoverUrl);
            ra.setUserName(user);
            ra.setPassword(pwd);

            XAResource[] resources = ra.getXAResources(null);
            assertEquals("one resource", 1, resources.length);

            assertEquals("no pending transactions", 0, resources[0].recover(100).length);

            primary.stop();

            assertTrue("no connections", Wait.waitFor(new Wait.Condition() {
                @Override
                public boolean isSatisified() throws Exception {
                    return primary.getConnections().isEmpty();
                }
            }));

            ra.stop();

            try {
                resources[0].recover(100);
                fail("Expect error on call after stop b/c of no reconnection");
            } catch (Exception expected) {
            }

        } finally {
            brokerService.stop();
        }
    }
}
