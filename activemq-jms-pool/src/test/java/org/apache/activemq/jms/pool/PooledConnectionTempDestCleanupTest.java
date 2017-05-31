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

package org.apache.activemq.jms.pool;

import static org.junit.Assert.assertTrue;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TemporaryQueue;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.RegionBroker;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test of lingering temporary destinations on pooled connections when the
 * underlying connections are reused. Also tests that closing one
 * PooledConnection does not delete the temporary destinations of another
 * PooledConnection that uses the same underlying ConnectionPool.
 *
 * jira: AMQ-3457
 */
public class PooledConnectionTempDestCleanupTest extends JmsPoolTestSupport {

    @Rule
    public TestName testName = new TestName();

    protected ActiveMQConnectionFactory directConnFact;
    protected Connection directConn1;
    protected Connection directConn2;

    protected PooledConnectionFactory pooledConnFact;
    protected Connection pooledConn1;
    protected Connection pooledConn2;

    protected TemporaryQueue tempDest;
    protected TemporaryQueue otherTempDest;

    /**
     * Prepare to run a test case: create, configure, and start the embedded
     * broker, as well as creating the client connections to the broker.
     */
    @Override
    @Before
    public void setUp() throws java.lang.Exception {
        super.setUp();

        brokerService = new BrokerService();
        configureBroker(brokerService);
        brokerService.start();
        brokerService.waitUntilStarted();

        // Create the ActiveMQConnectionFactory and the PooledConnectionFactory.
        directConnFact = new ActiveMQConnectionFactory(getBrokerConnectionURI());
        pooledConnFact = new PooledConnectionFactory();
        pooledConnFact.setConnectionFactory(directConnFact);

        // Prepare the connections
        directConn1 = directConnFact.createConnection();
        directConn1.start();
        directConn2 = directConnFact.createConnection();
        directConn2.start();

        pooledConn1 = pooledConnFact.createConnection();
        pooledConn1.start();
        pooledConn2 = pooledConnFact.createConnection();
        pooledConn2.start();
    }

    @Override
    @After
    public void tearDown() throws java.lang.Exception {
        try {
            pooledConn1.stop();
        } catch (JMSException jms_exc) {
        }
        try {
            pooledConn2.stop();
        } catch (JMSException jms_exc) {
        }
        try {
            directConn1.stop();
        } catch (JMSException jms_exc) {
        }
        try {
            directConn2.stop();
        } catch (JMSException jms_exc) {
        }

        super.tearDown();
    }

    protected void configureBroker(BrokerService brokerService) throws Exception {
        brokerService.setBrokerName("testbroker1");
        brokerService.setUseJmx(false);
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);

        TransportConnector connector = new TransportConnector();
        connector.setUri(new URI("tcp://localhost:0"));
        connector.setName(testName.getMethodName());

        brokerService.addConnector(connector);
    }

    protected String getBrokerConnectionURI() throws Exception {
        return brokerService.getTransportConnectorByName(testName.getMethodName()).getPublishableConnectString();
    }

    /**
     * Test for lingering temporary destinations after closing a
     * PooledConnection. Here are the steps:
     *
     * 1. create a session on the first pooled connection 2. create a session on
     * the second pooled connection 3. create a temporary destination on the
     * first session 4. confirm the temporary destination exists in the broker
     * 5. close the first connection 6. check that the temporary destination no
     * longer exists in the broker
     */
    @Test(timeout = 60000)
    public void testPooledLingeringTempDests() throws java.lang.Exception {
        Session session1;
        Session session2;

        session1 = pooledConn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session2 = pooledConn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

        tempDest = session1.createTemporaryQueue();

        assertTrue("TEST METHOD FAILURE - NEW TEMP DESTINATION DOES NOT EXIST", destinationExists(tempDest));

        pooledConn1.close();

        assertTrue("FAILED: temp dest from closed pooled connection is lingering", !destinationExists(tempDest));

        session2.close();
    }

    /**
     * Test that closing one PooledConnection does not delete the temporary
     * destinations of another.
     *
     * 1. create a session on the first pooled connection 2. create a session on
     * the second pooled connection 3. create a temporary destination on the
     * first session 4. create a temporary destination on the second session 5.
     * confirm both temporary destinations exist in the broker 6. close the
     * first connection 7. check that the first temporary destination no longer
     * exists in the broker 8. check that the second temporary destination does
     * still exist in the broker
     */
    @Test(timeout = 60000)
    public void testPooledTempDestsCleanupOverzealous() throws java.lang.Exception {
        Session session1;
        Session session2;

        session1 = pooledConn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session2 = pooledConn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

        tempDest = session1.createTemporaryQueue();
        otherTempDest = session2.createTemporaryQueue();

        assertTrue("TEST METHOD FAILURE - NEW TEMP DESTINATION DOES NOT EXIST", destinationExists(tempDest));
        assertTrue("TEST METHOD FAILURE - NEW TEMP DESTINATION DOES NOT EXIST", destinationExists(otherTempDest));

        pooledConn1.close();

        // Now confirm the first temporary destination no longer exists and the
        // second does.
        assertTrue("FAILED: temp dest from closed pooled connection is lingering", !destinationExists(tempDest));
        assertTrue("FAILED: second PooledConnectin's temporary destination was incorrectly deleted", destinationExists(otherTempDest));
    }

    /**
     * CONTROL CASE
     *
     * Test for lingering temporary destinations after closing a Connection that
     * is NOT pooled. This demonstrates the standard JMS operation and helps to
     * validate the test methodology.
     *
     * 1. create a session on the first direct connection 2. create a session on
     * the second direct connection 3. create a temporary destination on the
     * first session 4. confirm the destination exists in the broker 5. close
     * the first connection 6. check that the destination no longer exists in
     * the broker
     */
    @Test(timeout = 60000)
    public void testDirectLingeringTempDests() throws java.lang.Exception {
        Session session1;
        Session session2;

        session1 = directConn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session2 = directConn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

        tempDest = session1.createTemporaryQueue();

        assertTrue("TEST METHOD FAILURE - NEW TEMP DESTINATION DOES NOT EXIST", destinationExists(tempDest));

        directConn1.close();

        // Now confirm the temporary destination no longer exists.
        assertTrue("CONTROL TEST FAILURE - TEST METHOD IS SUSPECT", (!destinationExists(tempDest)));

        session2.close();
    }

    private boolean destinationExists(Destination dest) throws Exception {
        RegionBroker rb = (RegionBroker) brokerService.getBroker().getAdaptor(RegionBroker.class);
        return rb.getTopicRegion().getDestinationMap().containsKey(dest) || rb.getQueueRegion().getDestinationMap().containsKey(dest)
                || rb.getTempTopicRegion().getDestinationMap().containsKey(dest) || rb.getTempQueueRegion().getDestinationMap().containsKey(dest);
    }
}
