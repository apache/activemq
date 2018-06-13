/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq;

import junit.framework.TestCase;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.XATransactionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class XAConsumerTest extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(XAConsumerTest.class);
    private static final String TEST_AMQ_BROKER_URI = "tcp://localhost:0";
    private String brokerUri;
    private static long txGenerator = 21;

    private BrokerService broker;


    protected void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
        brokerUri = broker.getTransportConnectorByScheme("tcp").getPublishableConnectString();
    }

    protected void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }


    public void testPullRequestXAConsumer() throws Exception {

        ActiveMQXAConnectionFactory activeMQConnectionFactory =
                new ActiveMQXAConnectionFactory("admin", "admin", brokerUri + "?trace=true&jms.prefetchPolicy.all=0");
        XAConnection connection = activeMQConnectionFactory.createXAConnection();
        connection.start();

        ActiveMQXAConnectionFactory activeMQConnectionFactoryAutoAck =
                new ActiveMQXAConnectionFactory("admin", "admin", brokerUri + "?trace=true&jms.prefetchPolicy.all=0");
        // allow non xa use of connections
        activeMQConnectionFactoryAutoAck.setXaAckMode(Session.AUTO_ACKNOWLEDGE);
        Connection autoAckConnection = activeMQConnectionFactoryAutoAck.createConnection();
        autoAckConnection.start();

        try {

            LOG.info(">>>INVOKE XA receive with PullRequest Consumer...");

            XASession xaSession = connection.createXASession();
            XAResource xaResource = xaSession.getXAResource();
            Xid xid = createXid();
            xaResource.start(xid, 0);

            Destination destination = xaSession.createQueue("TEST.T2");

            final MessageConsumer messageConsumer = xaSession.createConsumer(destination);
            final CountDownLatch receiveThreadDone = new CountDownLatch(1);

            final CountDownLatch receiveLatch = new CountDownLatch(1);
            // do a message receive

            Thread receiveThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        messageConsumer.receive(600000);
                    } catch (JMSException expected) {
                        receiveLatch.countDown();
                        LOG.info("got expected ex: ", expected);
                    } finally {
                        receiveThreadDone.countDown();
                    }
                }
            });

            receiveThread.start();

            LOG.info(">>>simulate Transaction Rollback");
            xaResource.end(xid, XAResource.TMFAIL);
            xaResource.rollback(xid);

            // send a message after transaction is rolled back.
            LOG.info(">>>Sending message...");

            Session session = autoAckConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Message messageToSend = session.createMessage();
            MessageProducer messageProducer = session.createProducer(destination);
            messageProducer.send(messageToSend);

            receiveThreadDone.await(30, TimeUnit.SECONDS);
            receiveLatch.await(5, TimeUnit.SECONDS);


            // consume with non transacted consumer to verify not autoacked
            messageConsumer.close();
            xaSession.close();

            MessageConsumer messageConsumer1 = session.createConsumer(destination);
            javax.jms.Message message = messageConsumer1.receive(5000);

            assertNotNull("Got message", message);
            LOG.info("Got message on new session", message);
            message.acknowledge();

        } finally {
            LOG.info(">>>Closing Connection");
            if (connection != null) {
                connection.close();
            }
            if (autoAckConnection != null) {
                autoAckConnection.close();
            }
        }

    }


    public void testPullRequestXAConsumerSingleConsumer() throws Exception {

        ActiveMQXAConnectionFactory activeMQConnectionFactory =
                new ActiveMQXAConnectionFactory("admin", "admin", brokerUri + "?trace=true&jms.prefetchPolicy.all=0");
        XAConnection connection = activeMQConnectionFactory.createXAConnection();
        connection.start();

        try {

            LOG.info(">>>INVOKE XA receive with PullRequest Consumer...");

            XASession xaSession = connection.createXASession();
            XAResource xaResource = xaSession.getXAResource();
            Xid xid = createXid();
            xaResource.start(xid, 0);

            Destination destination = xaSession.createQueue("TEST.T2");

            final MessageConsumer messageConsumer = xaSession.createConsumer(destination);
            final CountDownLatch receiveThreadDone = new CountDownLatch(1);

            final CountDownLatch receiveLatch = new CountDownLatch(1);
            // do a message receive

            Thread receiveThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        messageConsumer.receive(600000);
                    } catch (JMSException expected) {
                        receiveLatch.countDown();
                        LOG.info("got expected ex: ", expected);
                    } finally {
                        receiveThreadDone.countDown();
                    }
                }
            });

            receiveThread.start();

            LOG.info(">>>simulate Transaction Rollback");
            xaResource.end(xid, XAResource.TMFAIL);
            xaResource.rollback(xid);

            {
                XASession xaSessionSend = connection.createXASession();
                XAResource xaResourceSend = xaSessionSend.getXAResource();
                Xid xidSend = createXid();
                xaResourceSend.start(xidSend, 0);

                // send a message after transaction is rolled back.
                LOG.info(">>>Sending message...");

                ActiveMQMessage messageToSend = (ActiveMQMessage) xaSessionSend.createMessage();
                messageToSend.setTransactionId(new XATransactionId(xidSend));
                MessageProducer messageProducer = xaSessionSend.createProducer(destination);
                messageProducer.send(messageToSend);

                xaResourceSend.end(xidSend, XAResource.TMSUCCESS);
                xaResourceSend.commit(xidSend, true);
            }

            receiveThreadDone.await(30, TimeUnit.SECONDS);
            receiveLatch.await(5, TimeUnit.SECONDS);

            // after jms exception we need to close
            messageConsumer.close();

            MessageConsumer messageConsumerTwo = xaSession.createConsumer(destination);
            Xid xidReceiveOk = createXid();
            xaResource.start(xidReceiveOk, 0);

            javax.jms.Message message = messageConsumerTwo.receive(10000);

            assertNotNull("Got message", message);
            LOG.info("Got message on new session", message);

            xaResource.end(xidReceiveOk, XAResource.TMSUCCESS);
            xaResource.commit(xidReceiveOk, true);

        } finally {
            LOG.info(">>>Closing Connection");
            if (connection != null) {
                connection.close();
            }
        }

    }

    public Xid createXid() throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(++txGenerator);
        os.close();
        final byte[] bs = baos.toByteArray();

        return new Xid() {

            public int getFormatId() {
                return 86;
            }


            public byte[] getGlobalTransactionId() {
                return bs;
            }


            public byte[] getBranchQualifier() {
                return bs;
            }
        };
    }


    private BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();

        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();


        pe.setProducerFlowControl(true);
        pe.setUseCache(true);

        pe.setPrioritizedMessages(false);
        pe.setExpireMessagesPeriod(0);
        pe.setQueuePrefetch(0);

        pe.setQueue(">");
        entries.add(pe);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);


        broker.addConnector(TEST_AMQ_BROKER_URI);
        broker.deleteAllMessages();
        return broker;
    }
}