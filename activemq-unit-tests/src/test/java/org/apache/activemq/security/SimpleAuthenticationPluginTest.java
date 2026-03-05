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
package org.apache.activemq.security;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TemporaryTopic;
import junit.framework.Test;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnectionState;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.experimental.categories.Category;
import org.apache.activemq.test.annotations.ParallelTest;

@Category(ParallelTest.class)
public class SimpleAuthenticationPluginTest extends SecurityTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleAuthenticationPluginTest.class);

    public static Test suite() {
        return suite(SimpleAuthenticationPluginTest.class);
    }

    @Override
    protected void setUp() throws Exception {
        setAutoFail(true);
        super.setUp();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        return createBroker("org/apache/activemq/security/simple-auth-broker.xml");
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestPredefinedDestinations() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST.Q")});
    }

    public void testPredefinedDestinations() throws JMSException {
        Message sent = doSend(false);
        assertEquals("guest", ((ActiveMQMessage)sent).getUserID());
        assertEquals("guest", sent.getStringProperty("JMSXUserID"));
    }

    public void testTempDestinations() throws Exception {
        Connection conn = factory.createConnection("guest", "password");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        String name = "org.apache.activemq:BrokerName=localhost,Type=TempTopic";
        try {
            conn.start();
            TemporaryTopic temp = sess.createTemporaryTopic();
            name += ",Destination=" + temp.getTopicName().replaceAll(":", "_");
            fail("Should have failed creating a temp topic");
        } catch (Exception ignore) {}

        ObjectName objName = new ObjectName(name);
        TopicViewMBean mbean = (TopicViewMBean)broker.getManagementContext().newProxyInstance(objName, TopicViewMBean.class, true);
        try {
            System.out.println(mbean.getName());
            fail("Shouldn't have created a temp topic");
        } catch (Exception ignore) {}
    }

    public void testConnectionStartThrowsJMSSecurityException() throws Exception {
        final CountDownLatch exceptionLatch = new CountDownLatch(1);

        try (final Connection connection = factory.createConnection("badUser", "password")) {
            connection.setExceptionListener(e -> {
                LOG.info("Connection received exception: {}", e.getMessage());
                assertTrue(e instanceof JMSSecurityException);
                exceptionLatch.countDown();
            });

            try {
                connection.start();

                // If start() doesn't throw synchronously, wait for async exception
                assertTrue("Should receive security exception via listener", exceptionLatch.await(5, TimeUnit.SECONDS));

            } catch (final JMSSecurityException jmsEx) {
                // Synchronous security exception - expected
            } catch (final JMSException e) {
                // with the latch, we should always pass first into the listener and assert the right exception
                LOG.info("Expected JMSSecurityException but was: {}", e.getClass());
                fail("Should throw JMSSecurityException");
            }
        }
    }

    public void testSecurityContextClearedOnPurge() throws Exception {

        connection.close();
        ActiveMQConnectionFactory tcpFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString());
        ActiveMQConnection conn = (ActiveMQConnection) tcpFactory.createConnection("user", "password");
        Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        conn.start();

        LOG.info("dest list at start:" + Arrays.asList(broker.getRegionBroker().getDestinations()));
        final int numDests = broker.getRegionBroker().getDestinations().length;
        for (int i=0; i<10; i++) {
            MessageProducer p = sess.createProducer(new ActiveMQQueue("USERS.PURGE." + i));
            p.close();
        }

        assertTrue("dests are purged", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("dest list:" + Arrays.asList(broker.getRegionBroker().getDestinations()));
                LOG.info("dests, orig: " + numDests + ", now: "+ broker.getRegionBroker().getDestinations().length);
                return numDests == broker.getRegionBroker().getDestinations().length;
            }
        }));

        // verify removed from connection security context
        TransportConnection brokerConnection = broker.getTransportConnectors().get(0).getConnections().get(0);
        TransportConnectionState transportConnectionState = brokerConnection.lookupConnectionState(conn.getConnectionInfo().getConnectionId());
        assertEquals("no destinations", 0, transportConnectionState.getContext().getSecurityContext().getAuthorizedWriteDests().size());
    }

}
