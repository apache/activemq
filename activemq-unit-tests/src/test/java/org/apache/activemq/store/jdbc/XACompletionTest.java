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

package org.apache.activemq.store.jdbc;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnection;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.filter.AnyDestination;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.SimpleAuthorizationMap;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.Wait;
import org.apache.activemq.wireformat.WireFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.XASession;
import javax.management.ObjectName;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Enumeration;

import static org.apache.activemq.util.TestUtils.createXid;


@RunWith(value = Parameterized.class)
public class XACompletionTest extends TestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(XACompletionTest.class);

    protected ActiveMQXAConnectionFactory factory;
    protected static final int messagesExpected = 1;
    protected BrokerService broker;
    protected String connectionUri;

    @Parameterized.Parameter
    public TestSupport.PersistenceAdapterChoice persistenceAdapterChoice;

    @Parameterized.Parameters(name = "store={0}")
    public static Iterable<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{{TestSupport.PersistenceAdapterChoice.KahaDB}, {PersistenceAdapterChoice.JDBC}});
    }

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
    }

    @After
    public void stopAll() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
    }


    @Test
    public void testStatsAndRedispatchAfterAckPreparedClosed() throws Exception {

        factory = new ActiveMQXAConnectionFactory(
                connectionUri + "?jms.prefetchPolicy.all=0&jms.redeliveryPolicy.maximumRedeliveries=" + 0);

        factory.setWatchTopicAdvisories(false);
        sendMessages(1);

        ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        XASession xaSession = activeMQXAConnection.createXASession();

        Destination destination = xaSession.createQueue("TEST");
        MessageConsumer consumer = xaSession.createConsumer(destination);

        XAResource resource = xaSession.getXAResource();
        resource.recover(XAResource.TMSTARTRSCAN);
        resource.recover(XAResource.TMNOFLAGS);

        Xid tid = createXid();

        resource.start(tid, XAResource.TMNOFLAGS);

        Message message = consumer.receive(2000);
        LOG.info("Received : " + message);

        resource.end(tid, XAResource.TMSUCCESS);

        activeMQXAConnection.close();

        dumpMessages();

        dumpMessages();

        LOG.info("Try jmx browse... after commit");

        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        assertEquals("size", 1, proxy.getQueueSize());

        LOG.info("Try receive... after rollback");
        message = regularReceive("TEST");

        assertNotNull("message gone", message);
    }

    @Test
    public void testStatsAndBrowseAfterAckPreparedCommitted() throws Exception {

        factory = new ActiveMQXAConnectionFactory(
                connectionUri + "?jms.prefetchPolicy.all=0&jms.redeliveryPolicy.maximumRedeliveries=" + messagesExpected);

        factory.setWatchTopicAdvisories(false);
        sendMessages(messagesExpected);

        ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        XASession xaSession = activeMQXAConnection.createXASession();

        Destination destination = xaSession.createQueue("TEST");
        MessageConsumer consumer = xaSession.createConsumer(destination);

        XAResource resource = xaSession.getXAResource();
        resource.recover(XAResource.TMSTARTRSCAN);
        resource.recover(XAResource.TMNOFLAGS);

        Xid tid = createXid();

        resource.start(tid, XAResource.TMNOFLAGS);

        int messagesReceived = 0;

        for (int i = 0; i < messagesExpected; i++) {

            Message message = null;
            try {
                LOG.debug("Receiving message " + (messagesReceived + 1) + " of " + messagesExpected);
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
                messagesReceived++;
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);
            }
        }

        resource.end(tid, XAResource.TMSUCCESS);
        resource.prepare(tid);

        consumer.close();

        dumpMessages();

        resource.commit(tid, false);

        dumpMessages();

        LOG.info("Try jmx browse... after commit");

        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        assertTrue(proxy.browseMessages().isEmpty());
        assertEquals("prefetch 0", 0, proxy.getInFlightCount());
        assertEquals("size 0", 0, proxy.getQueueSize());

        LOG.info("Try browse... after commit");
        Message browsed = regularBrowseFirst();


        assertNull("message gone", browsed);

        LOG.info("Try receive... after commit");
        Message message = regularReceive("TEST");

        assertNull("message gone", message);

    }


    @Test
    public void testStatsAndBrowseAfterAckPreparedRolledback() throws Exception {

        factory = new ActiveMQXAConnectionFactory(
                connectionUri + "?jms.prefetchPolicy.all=0");

        factory.setWatchTopicAdvisories(false);
        sendMessages(10);

        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        XASession xaSession = activeMQXAConnection.createXASession();

        Destination destination = xaSession.createQueue("TEST");
        MessageConsumer consumer = xaSession.createConsumer(destination);

        XAResource resource = xaSession.getXAResource();
        resource.recover(XAResource.TMSTARTRSCAN);
        resource.recover(XAResource.TMNOFLAGS);

        assertEquals("prefetch 0", 0, proxy.getInFlightCount());
        assertEquals("size 0", 10, proxy.getQueueSize());
        assertEquals("size 0", 0, proxy.cursorSize());

        Xid tid = createXid();

        resource.start(tid, XAResource.TMNOFLAGS);

        for (int i = 0; i < 5; i++) {

            Message message = null;
            try {
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);
            }
        }

        resource.end(tid, XAResource.TMSUCCESS);
        resource.prepare(tid);

        consumer.close();

        dumpMessages();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return proxy.getInFlightCount() == 0l;
            }
        });
        assertEquals("prefetch", 0, proxy.getInFlightCount());
        assertEquals("size", 10, proxy.getQueueSize());
        assertEquals("cursor size", 0, proxy.cursorSize());

        resource.rollback(tid);

        dumpMessages();

        LOG.info("Try jmx browse... after rollback");

        assertEquals(10, proxy.browseMessages().size());

        assertEquals("prefetch", 0, proxy.getInFlightCount());
        assertEquals("size", 10, proxy.getQueueSize());
        assertEquals("cursor size", 0, proxy.cursorSize());

        LOG.info("Try browse... after");
        Message browsed = regularBrowseFirst();
        assertNotNull("message gone", browsed);

        LOG.info("Try receive... after");
        for (int i = 0; i < 10; i++) {
            Message message = regularReceive("TEST");
            assertNotNull("message gone", message);
        }
    }

    @Test
    public void testStatsAndConsumeAfterAckPreparedRolledback() throws Exception {

        factory = new ActiveMQXAConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=0");

        factory.setWatchTopicAdvisories(false);
        sendMessages(10);

        ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        XASession xaSession = activeMQXAConnection.createXASession();

        Destination destination = xaSession.createQueue("TEST");
        MessageConsumer consumer = xaSession.createConsumer(destination);

        XAResource resource = xaSession.getXAResource();
        resource.recover(XAResource.TMSTARTRSCAN);
        resource.recover(XAResource.TMNOFLAGS);

        dumpMessages();
        Xid tid = createXid();

        resource.start(tid, XAResource.TMNOFLAGS);

        int messagesReceived = 0;

        for (int i = 0; i < 5; i++) {

            Message message = null;
            try {
                LOG.debug("Receiving message " + (messagesReceived + 1) + " of " + messagesExpected);
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
                messagesReceived++;
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);
            }
        }

        resource.end(tid, XAResource.TMSUCCESS);
        resource.prepare(tid);

        consumer.close();

        LOG.info("after close");
        dumpMessages();

        assertEquals("drain", 5, drainUnack(5, "TEST"));

        dumpMessages();

        broker = restartBroker();

        assertEquals("redrain", 5, drainUnack(5, "TEST"));


        LOG.info("Try consume... after restart");
        dumpMessages();

        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        assertEquals("prefetch", 0, proxy.getInFlightCount());
        assertEquals("size", 5, proxy.getQueueSize());
        assertEquals("cursor size 0", 0, proxy.cursorSize());

        factory = new ActiveMQXAConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=0");
        factory.setWatchTopicAdvisories(false);

        activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        xaSession = activeMQXAConnection.createXASession();

        XAResource xaResource = xaSession.getXAResource();

        Xid[] xids = xaResource.recover(XAResource.TMSTARTRSCAN);
        xaResource.recover(XAResource.TMNOFLAGS);

        LOG.info("Rollback outcome for ack");
        xaResource.rollback(xids[0]);


        LOG.info("Try receive... after rollback");
        for (int i = 0; i < 10; i++) {
            Message message = regularReceive("TEST");
            assertNotNull("message gone: " + i, message);
        }

        dumpMessages();

        assertNull("none left", regularReceive("TEST"));

        assertEquals("prefetch", 0, proxy.getInFlightCount());
        assertEquals("size", 0, proxy.getQueueSize());
        assertEquals("cursor size", 0, proxy.cursorSize());
        assertEquals("dq", 10, proxy.getDequeueCount());

    }

    @Test
    public void testConsumeAfterAckPreparedRolledbackTopic() throws Exception {

        factory = new ActiveMQXAConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=0");
        factory.setWatchTopicAdvisories(false);

        final ActiveMQTopic destination = new ActiveMQTopic("TEST");

        ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.setClientID("durable");
        activeMQXAConnection.start();
        XASession xaSession = activeMQXAConnection.createXASession();

        MessageConsumer consumer = xaSession.createDurableSubscriber(destination, "sub1");
        consumer.close();
        consumer = xaSession.createDurableSubscriber(destination, "sub2");

        sendMessagesTo(10, destination);

        XAResource resource = xaSession.getXAResource();
        resource.recover(XAResource.TMSTARTRSCAN);
        resource.recover(XAResource.TMNOFLAGS);

        dumpMessages();
        Xid tid = createXid();

        resource.start(tid, XAResource.TMNOFLAGS);

        int messagesReceived = 0;

        for (int i = 0; i < 5; i++) {

            Message message = null;
            try {
                LOG.debug("Receiving message " + (messagesReceived + 1) + " of " + messagesExpected);
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
                messagesReceived++;
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);
            }
        }

        resource.end(tid, XAResource.TMSUCCESS);
        resource.prepare(tid);

        consumer.close();
        activeMQXAConnection.close();

        LOG.info("after close");

        broker = restartBroker();

        LOG.info("Try consume... after restart");
        dumpMessages();

        factory = new ActiveMQXAConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=0");
        factory.setWatchTopicAdvisories(false);

        activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        xaSession = activeMQXAConnection.createXASession();

        XAResource xaResource = xaSession.getXAResource();

        Xid[] xids = xaResource.recover(XAResource.TMSTARTRSCAN);
        xaResource.recover(XAResource.TMNOFLAGS);

        LOG.info("Rollback outcome for ack");
        xaResource.rollback(xids[0]);

        assertTrue("got expected", consumeOnlyN(10,"durable", "sub1", destination));
        assertTrue("got expected", consumeOnlyN(10, "durable", "sub2", destination));
    }

    @Test
    public void testConsumeAfterAckPreparedCommitTopic() throws Exception {

        factory = new ActiveMQXAConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=0");
        factory.setWatchTopicAdvisories(false);

        final ActiveMQTopic destination = new ActiveMQTopic("TEST");

        ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.setClientID("durable");
        activeMQXAConnection.start();
        XASession xaSession = activeMQXAConnection.createXASession();

        MessageConsumer consumer = xaSession.createDurableSubscriber(destination, "sub1");
        consumer.close();
        consumer = xaSession.createDurableSubscriber(destination, "sub2");

        sendMessagesTo(10, destination);

        XAResource resource = xaSession.getXAResource();
        resource.recover(XAResource.TMSTARTRSCAN);
        resource.recover(XAResource.TMNOFLAGS);

        dumpMessages();
        Xid tid = createXid();

        resource.start(tid, XAResource.TMNOFLAGS);

        int messagesReceived = 0;

        for (int i = 0; i < 5; i++) {

            Message message = null;
            try {
                LOG.debug("Receiving message " + (messagesReceived + 1) + " of " + messagesExpected);
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
                messagesReceived++;
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);
            }
        }

        resource.end(tid, XAResource.TMSUCCESS);
        resource.prepare(tid);

        consumer.close();
        activeMQXAConnection.close();

        LOG.info("after close");

        broker = restartBroker();

        LOG.info("Try consume... after restart");
        dumpMessages();

        factory = new ActiveMQXAConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=0");
        factory.setWatchTopicAdvisories(false);

        activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        xaSession = activeMQXAConnection.createXASession();

        XAResource xaResource = xaSession.getXAResource();

        Xid[] xids = xaResource.recover(XAResource.TMSTARTRSCAN);
        xaResource.recover(XAResource.TMNOFLAGS);

        LOG.info("Rollback outcome for ack");
        xaResource.commit(xids[0], false);

        assertTrue("got expected", consumeOnlyN(10,"durable", "sub1", destination));
        assertTrue("got expected", consumeOnlyN(5, "durable", "sub2", destination));

        LOG.info("at end...");
        dumpMessages();

    }

    private boolean consumeOnlyN(int expected, String clientId, String subName, ActiveMQTopic destination) throws Exception {
        int drained = 0;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=" + expected);
        factory.setWatchTopicAdvisories(false);
        javax.jms.Connection connection = factory.createConnection();
        connection.setClientID(clientId);
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createDurableSubscriber(destination, subName);
            Message message = null;
            while ( (message =consumer.receive(2000)) != null) {
                drained++;
                LOG.info("Sub:" + subName + ", received: " + message.getJMSMessageID());
            }
            consumer.close();
        } finally {
            connection.close();
        }
        return drained == expected;
    }

    @Test
    public void testStatsAndConsumeAfterAckPreparedRolledbackOutOfOrderRecovery() throws Exception {

        factory = new ActiveMQXAConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=0");
        factory.setWatchTopicAdvisories(false);
        sendMessages(20);


        for (int i = 0; i < 10; i++) {

            ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
            activeMQXAConnection.start();
            XASession xaSession = activeMQXAConnection.createXASession();

            Destination destination = xaSession.createQueue("TEST");
            MessageConsumer consumer = xaSession.createConsumer(destination);

            XAResource resource = xaSession.getXAResource();
            Xid tid = createXid();

            resource.start(tid, XAResource.TMNOFLAGS);

            Message message = null;
            try {
                message = consumer.receive(2000);
                LOG.info("Received (" + i + ") : ," + message);
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);
            }

            resource.end(tid, XAResource.TMSUCCESS);
            resource.prepare(tid);

            // no close - b/c messages end up in pagedInPendingDispatch!
            // activeMQXAConnection.close();
        }

        ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        XASession xaSession = activeMQXAConnection.createXASession();

        XAResource xaResource = xaSession.getXAResource();

        Xid[] xids = xaResource.recover(XAResource.TMSTARTRSCAN);
        xaResource.recover(XAResource.TMNOFLAGS);


        xaResource.rollback(xids[0]);
        xaResource.rollback(xids[1]);

        activeMQXAConnection.close();


        LOG.info("RESTART");
        broker = restartBroker();

        dumpMessages();

        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);


        // set maxBatchSize=1
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=" + 1);
        factory.setWatchTopicAdvisories(false);
        javax.jms.Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination destination = session.createQueue("TEST");
        MessageConsumer consumer = session.createConsumer(destination);
        consumer.close();

        ActiveMQConnectionFactory receiveFactory = new ActiveMQConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=0");
        receiveFactory.setWatchTopicAdvisories(false);

        // recover/rollback the second tx
        ActiveMQXAConnectionFactory activeMQXAConnectionFactory = new ActiveMQXAConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=0");
        activeMQXAConnectionFactory.setWatchTopicAdvisories(false);
        activeMQXAConnection = (ActiveMQXAConnection) activeMQXAConnectionFactory.createXAConnection();
        activeMQXAConnection.start();
        xaSession = activeMQXAConnection.createXASession();
        xaResource = xaSession.getXAResource();
        xids = xaResource.recover(XAResource.TMSTARTRSCAN);
        xaResource.recover(XAResource.TMNOFLAGS);

        for (int i = 0; i < xids.length; i++) {
            xaResource.rollback(xids[i]);
        }

        // another prefetch demand of 1
        MessageConsumer consumer2 = session.createConsumer(new ActiveMQQueue("TEST?consumer.prefetchSize=2"));

        LOG.info("Try receive... after rollback");
        Message message = regularReceiveWith(receiveFactory, "TEST");
        assertNotNull("message 1: ", message);
        LOG.info("Received : " + message);

        dumpMessages();

        message = regularReceiveWith(receiveFactory, "TEST");
        assertNotNull("last message", message);
        LOG.info("Received : " + message);

    }

    @Test
    public void testMoveInTwoBranches() throws Exception {

        factory = new ActiveMQXAConnectionFactory(
                connectionUri + "?jms.prefetchPolicy.all=0&jms.redeliveryPolicy.maximumRedeliveries=" + messagesExpected);

        factory.setWatchTopicAdvisories(false);
        sendMessages(messagesExpected);

        ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        XASession xaSession = activeMQXAConnection.createXASession();

        Destination destination = xaSession.createQueue("TEST");
        MessageConsumer consumer = xaSession.createConsumer(destination);

        XAResource resource = xaSession.getXAResource();

        final Xid tid = createXid();
        byte[] branch = tid.getBranchQualifier();
        final byte[] branch2 = Arrays.copyOf(branch, branch.length);
        branch2[0] = '!';

        Xid branchTid = new Xid() {
            @Override
            public int getFormatId() {
                return tid.getFormatId();
            }

            @Override
            public byte[] getGlobalTransactionId() {
                return tid.getGlobalTransactionId();
            }

            @Override
            public byte[] getBranchQualifier() {
                return branch2;
            }
        };

        resource.start(tid, XAResource.TMNOFLAGS);

        int messagesReceived = 0;

        Message message = null;

        for (int i = 0; i < messagesExpected; i++) {

            try {
                LOG.debug("Receiving message " + (messagesReceived + 1) + " of " + messagesExpected);
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
                messagesReceived++;
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);
            }
        }

        resource.end(tid, XAResource.TMSUCCESS);

        ActiveMQXAConnection activeMQXAConnectionSend = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnectionSend.start();
        XASession xaSessionSend = activeMQXAConnection.createXASession();

        Destination destinationSend = xaSessionSend.createQueue("TEST_MOVE");
        MessageProducer producer = xaSessionSend.createProducer(destinationSend);

        XAResource resourceSend = xaSessionSend.getXAResource();
        resourceSend.start(branchTid, XAResource.TMNOFLAGS);

        ActiveMQMessage toSend = (ActiveMQMessage) xaSessionSend.createTextMessage();
        toSend.setTransactionId(new XATransactionId(branchTid));
        producer.send(toSend);

        resourceSend.end(branchTid, XAResource.TMSUCCESS);
        resourceSend.prepare(branchTid);

        resource.prepare(tid);

        consumer.close();

        LOG.info("Prepared");
        dumpMessages();

        LOG.info("Commit Ack");
        resource.commit(tid, false);
        dumpMessages();

        LOG.info("Commit Send");
        resourceSend.commit(branchTid, false);
        dumpMessages();


        LOG.info("Try jmx browse... after commit");

        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        assertTrue(proxy.browseMessages().isEmpty());
        assertEquals("dq ", 1, proxy.getDequeueCount());
        assertEquals("size 0", 0, proxy.getQueueSize());

        ObjectName queueMoveViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST_MOVE");
        QueueViewMBean moveProxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueMoveViewMBeanName, QueueViewMBean.class, true);

        assertEquals("enq", 1, moveProxy.getEnqueueCount());
        assertEquals("size 1", 1, moveProxy.getQueueSize());

        assertNotNull(regularReceive("TEST_MOVE"));

        assertEquals("size 0", 0, moveProxy.getQueueSize());

    }

    @Test
    public void testMoveInTwoBranchesPreparedAckRecoveryRestartRollback() throws Exception {

        factory = new ActiveMQXAConnectionFactory(
                connectionUri + "?jms.prefetchPolicy.all=0&jms.redeliveryPolicy.maximumRedeliveries=" + messagesExpected);

        factory.setWatchTopicAdvisories(false);
        sendMessages(messagesExpected);

        ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        XASession xaSession = activeMQXAConnection.createXASession();

        Destination destination = xaSession.createQueue("TEST");
        MessageConsumer consumer = xaSession.createConsumer(destination);

        XAResource resource = xaSession.getXAResource();

        final Xid tid = createXid();
        byte[] branch = tid.getBranchQualifier();
        final byte[] branch2 = Arrays.copyOf(branch, branch.length);
        branch2[0] = '!';

        Xid branchTid = new Xid() {
            @Override
            public int getFormatId() {
                return tid.getFormatId();
            }

            @Override
            public byte[] getGlobalTransactionId() {
                return tid.getGlobalTransactionId();
            }

            @Override
            public byte[] getBranchQualifier() {
                return branch2;
            }
        };

        resource.start(tid, XAResource.TMNOFLAGS);

        int messagesReceived = 0;

        Message message = null;

        for (int i = 0; i < messagesExpected; i++) {

            try {
                LOG.debug("Receiving message " + (messagesReceived + 1) + " of " + messagesExpected);
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
                messagesReceived++;
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);
            }
        }

        resource.end(tid, XAResource.TMSUCCESS);

        ActiveMQXAConnection activeMQXAConnectionSend = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnectionSend.start();
        XASession xaSessionSend = activeMQXAConnection.createXASession();

        Destination destinationSend = xaSessionSend.createQueue("TEST_MOVE");
        MessageProducer producer = xaSessionSend.createProducer(destinationSend);

        XAResource resourceSend = xaSessionSend.getXAResource();
        resourceSend.start(branchTid, XAResource.TMNOFLAGS);

        ActiveMQMessage toSend = (ActiveMQMessage) xaSessionSend.createTextMessage();
        toSend.setTransactionId(new XATransactionId(branchTid));
        producer.send(toSend);

        resourceSend.end(branchTid, XAResource.TMSUCCESS);
        resourceSend.prepare(branchTid);

        // ack on TEST is prepared
        resource.prepare(tid);

        // send to TEST_MOVE is rolledback
        resourceSend.rollback(branchTid);

        consumer.close();

        LOG.info("Prepared");
        dumpMessages();

        broker = restartBroker();

        LOG.info("New broker");
        dumpMessages();

        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        assertEquals("size", 0, proxy.getQueueSize());

        assertNull(regularReceive("TEST_MOVE"));

        ObjectName queueMoveViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST_MOVE");
        QueueViewMBean moveProxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueMoveViewMBeanName, QueueViewMBean.class, true);

        assertEquals("enq", 0, moveProxy.getDequeueCount());
        assertEquals("size", 0, moveProxy.getQueueSize());

        assertEquals("size 0", 0, moveProxy.getQueueSize());

        factory = new ActiveMQXAConnectionFactory(
                connectionUri + "?jms.prefetchPolicy.all=0&jms.redeliveryPolicy.maximumRedeliveries=" + messagesExpected);
        factory.setWatchTopicAdvisories(false);

        activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        xaSession = activeMQXAConnection.createXASession();

        resource = xaSession.getXAResource();
        resource.rollback(tid);

        assertEquals("size", 1, proxy.getQueueSize());
        assertEquals("c size", 1, proxy.cursorSize());

        assertNotNull(regularReceive("TEST"));

        assertEquals("size", 0, proxy.getQueueSize());
        assertEquals("c size", 0, proxy.cursorSize());
        assertEquals("dq", 1, proxy.getDequeueCount());
    }


    @Test
    public void testMoveInTwoBranchesTwoBrokers() throws Exception {

        factory = new ActiveMQXAConnectionFactory(
                connectionUri + "?jms.prefetchPolicy.all=0&jms.redeliveryPolicy.maximumRedeliveries=" + messagesExpected);

        factory.setWatchTopicAdvisories(false);
        sendMessages(messagesExpected);

        ActiveMQXAConnection activeMQXAConnection = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnection.start();
        XASession xaSession = activeMQXAConnection.createXASession();

        Destination destination = xaSession.createQueue("TEST");
        MessageConsumer consumer = xaSession.createConsumer(destination);

        XAResource resource = xaSession.getXAResource();

        final Xid tid = createXid();
        byte[] branch = tid.getBranchQualifier();
        final byte[] branch2 = Arrays.copyOf(branch, branch.length);
        branch2[0] = '!';

        Xid branchTid = new Xid() {
            @Override
            public int getFormatId() {
                return tid.getFormatId();
            }

            @Override
            public byte[] getGlobalTransactionId() {
                return tid.getGlobalTransactionId();
            }

            @Override
            public byte[] getBranchQualifier() {
                return branch2;
            }
        };

        resource.start(tid, XAResource.TMNOFLAGS);

        int messagesReceived = 0;

        Message message = null;

        for (int i = 0; i < messagesExpected; i++) {

            try {
                LOG.debug("Receiving message " + (messagesReceived + 1) + " of " + messagesExpected);
                message = consumer.receive(2000);
                LOG.info("Received : " + message);
                messagesReceived++;
            } catch (Exception e) {
                LOG.debug("Caught exception:", e);
            }
        }

        resource.end(tid, XAResource.TMSUCCESS);

        ActiveMQXAConnection activeMQXAConnectionSend = (ActiveMQXAConnection) factory.createXAConnection();
        activeMQXAConnectionSend.start();
        XASession xaSessionSend = activeMQXAConnection.createXASession();

        Destination destinationSend = xaSessionSend.createQueue("TEST_MOVE");
        MessageProducer producer = xaSessionSend.createProducer(destinationSend);

        XAResource resourceSend = xaSessionSend.getXAResource();
        resourceSend.start(branchTid, XAResource.TMNOFLAGS);

        ActiveMQMessage toSend = (ActiveMQMessage) xaSessionSend.createTextMessage();
        toSend.setTransactionId(new XATransactionId(branchTid));
        producer.send(toSend);

        resourceSend.end(branchTid, XAResource.TMSUCCESS);
        resourceSend.prepare(branchTid);

        resource.prepare(tid);

        consumer.close();

        LOG.info("Prepared");
        dumpMessages();

        LOG.info("Commit Ack");
        resource.commit(tid, false);
        dumpMessages();

        LOG.info("Commit Send");
        resourceSend.commit(branchTid, false);
        dumpMessages();


        LOG.info("Try jmx browse... after commit");

        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST");
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);

        assertTrue(proxy.browseMessages().isEmpty());
        assertEquals("dq ", 1, proxy.getDequeueCount());
        assertEquals("size 0", 0, proxy.getQueueSize());

        ObjectName queueMoveViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=TEST_MOVE");
        QueueViewMBean moveProxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueMoveViewMBeanName, QueueViewMBean.class, true);

        assertEquals("enq", 1, moveProxy.getEnqueueCount());
        assertEquals("size 1", 1, moveProxy.getQueueSize());

        assertNotNull(regularReceive("TEST_MOVE"));

        assertEquals("size 0", 0, moveProxy.getQueueSize());

    }


    private Message regularReceive(String qName) throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setWatchTopicAdvisories(false);
        return regularReceiveWith(factory, qName);
    }

    private Message regularReceiveWith(ActiveMQConnectionFactory factory, String qName) throws Exception {
        javax.jms.Connection connection = factory.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue(qName);
            MessageConsumer consumer = session.createConsumer(destination);
            return consumer.receive(2000);
        } finally {
            connection.close();
        }
    }

    private int drainUnack(int limit, String qName) throws Exception {
        int drained = 0;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri + "?jms.prefetchPolicy.all=" + limit);
        factory.setWatchTopicAdvisories(false);
        javax.jms.Connection connection = factory.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Destination destination = session.createQueue(qName);
            MessageConsumer consumer = session.createConsumer(destination);
            while (drained < limit && consumer.receive(2000) != null) {
                drained++;
            }
            ;
            consumer.close();
        } finally {
            connection.close();
        }
        return drained;
    }

    private Message regularBrowseFirst() throws Exception {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(connectionUri);
        activeMQConnectionFactory.setWatchTopicAdvisories(false);
        javax.jms.Connection connection = activeMQConnectionFactory.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue destination = session.createQueue("TEST");
            QueueBrowser browser = session.createBrowser(destination);
            Enumeration e = browser.getEnumeration();
            if (e.hasMoreElements()) {
                return (Message) e.nextElement();
            }
            return null;
        } finally {
            connection.close();
        }
    }

    protected void sendMessages(int messagesExpected) throws Exception {
        sendMessagesTo(messagesExpected, new ActiveMQQueue("TEST"));
    }

    protected void sendMessagesTo(int messagesExpected, Destination destination) throws Exception {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(connectionUri);
        activeMQConnectionFactory.setWatchTopicAdvisories(false);
        sendMessagesWithTo(activeMQConnectionFactory, messagesExpected, destination);
    }

    protected void sendMessagesWith(ConnectionFactory factory, int messagesExpected) throws Exception {
        sendMessagesWithTo(factory, messagesExpected, new ActiveMQQueue("TEST"));
    }

    protected void sendMessagesWithTo(ConnectionFactory factory, int messagesExpected, Destination destination) throws Exception {
        javax.jms.Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        for (int i = 0; i < messagesExpected; i++) {
            LOG.debug("Sending message " + (i + 1) + " of " + messagesExpected);
            producer.send(session.createTextMessage("test message " + (i + 1)));
        }
        connection.close();
    }

    protected void dumpMessages() throws Exception {

        if (persistenceAdapterChoice.compareTo(PersistenceAdapterChoice.JDBC) != 0) {
            return;
        }
        WireFormat wireFormat = new OpenWireFormat();
        java.sql.Connection conn = ((JDBCPersistenceAdapter) broker.getPersistenceAdapter()).getDataSource().getConnection();
        PreparedStatement statement = conn.prepareStatement("SELECT ID, MSG, XID FROM ACTIVEMQ_MSGS");
        ResultSet result = statement.executeQuery();
        LOG.info("Messages in broker db...");
        while (result.next()) {
            long id = result.getLong(1);
            org.apache.activemq.command.Message message = (org.apache.activemq.command.Message) wireFormat.unmarshal(new ByteSequence(result.getBytes(2)));
            String xid = result.getString(3);
            LOG.info("id: " + id + ", message SeqId: " + message.getMessageId().getBrokerSequenceId() + ", XID:" + xid + ", MSG: " + message);
        }
        statement.close();

        statement = conn.prepareStatement("SELECT LAST_ACKED_ID, CLIENT_ID, SUB_NAME, PRIORITY, XID FROM ACTIVEMQ_ACKS");
        result = statement.executeQuery();
        LOG.info("Messages in ACKS table db...");
        while (result.next()) {
            LOG.info("lastAcked: {}, clientId: {}, SUB_NAME: {}, PRIORITY: {}, XID {}",
                    result.getLong(1), result.getString(2), result.getString(3), result.getInt(4), result.getString(5));
        }
        statement.close();
        conn.close();
    }

    protected BrokerService createBroker() throws Exception {
        return createBroker(true);
    }

    protected BrokerService restartBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        return createBroker(false);
    }

    protected BrokerService createBroker(boolean del) throws Exception {

        BrokerService broker = new BrokerService();
        broker.setAdvisorySupport(false);
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setExpireMessagesPeriod(0);
        policyMap.setDefaultEntry(policyEntry);
        broker.setDestinationPolicy(policyMap);

        broker.setDeleteAllMessagesOnStartup(del);

        setPersistenceAdapter(broker, persistenceAdapterChoice);
        broker.setPersistent(true);
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();

        // ensure we run through a destination filter
        final String id = "a";
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin();
        SimpleAuthorizationMap map = new SimpleAuthorizationMap();
        DestinationMap destinationMap = new DestinationMap();
        GroupPrincipal anaGroup = new GroupPrincipal(id);
        destinationMap.put(new AnyDestination(new ActiveMQDestination[]{new ActiveMQQueue(">")}), anaGroup);
        destinationMap.put(new AnyDestination(new ActiveMQDestination[]{new ActiveMQTopic(">")}), anaGroup);
        map.setWriteACLs(destinationMap);
        map.setAdminACLs(destinationMap);
        map.setReadACLs(destinationMap);
        authorizationPlugin.setMap(map);
        SimpleAuthenticationPlugin simpleAuthenticationPlugin = new SimpleAuthenticationPlugin();
        simpleAuthenticationPlugin.setAnonymousAccessAllowed(true);
        simpleAuthenticationPlugin.setAnonymousGroup(id);
        simpleAuthenticationPlugin.setAnonymousUser(id);

        broker.setPlugins(new BrokerPlugin[]{simpleAuthenticationPlugin, authorizationPlugin});
        broker.start();
        return broker;
    }
}
