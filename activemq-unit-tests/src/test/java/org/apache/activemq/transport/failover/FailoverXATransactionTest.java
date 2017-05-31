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
package org.apache.activemq.transport.failover;

import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.TestUtils;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class FailoverXATransactionTest {

    private static final Logger LOG = LoggerFactory.getLogger(FailoverXATransactionTest.class);
    private static final String QUEUE_NAME = "Failover.WithXaTx";
    private static final String TRANSPORT_URI = "tcp://localhost:0";
    private String url;
    BrokerService broker;

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    public void startBroker(boolean deleteAllMessagesOnStartup) throws Exception {
        broker = createBroker(deleteAllMessagesOnStartup);
        broker.start();
    }

    public void startBroker(boolean deleteAllMessagesOnStartup, String bindAddress) throws Exception {
        broker = createBroker(deleteAllMessagesOnStartup, bindAddress);
        broker.start();
    }

    public BrokerService createBroker(boolean deleteAllMessagesOnStartup) throws Exception {
        return createBroker(deleteAllMessagesOnStartup, TRANSPORT_URI);
    }

    public BrokerService createBroker(boolean deleteAllMessagesOnStartup, String bindAddress) throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
        broker.addConnector(bindAddress);
        broker.setDeleteAllMessagesOnStartup(deleteAllMessagesOnStartup);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultEntry = new PolicyEntry();
        defaultEntry.setUsePrefetchExtension(false);
        policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);

        url = broker.getTransportConnectors().get(0).getConnectUri().toString();

        return broker;
    }

    @org.junit.Test
    public void testFailoverSendPrepareReplyLost() throws Exception {

        broker = createBroker(true);

        final AtomicBoolean first = new AtomicBoolean(false);
        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    @Override
                    public int prepareTransaction(final ConnectionContext context,
                                                  TransactionId xid) throws Exception {
                        int result = super.prepareTransaction(context, xid);
                        if (first.compareAndSet(false, true)) {
                            context.setDontSendReponse(true);
                            Executors.newSingleThreadExecutor().execute(new Runnable() {
                                public void run() {
                                    LOG.info("Stopping broker on prepare");
                                    try {
                                        context.getConnection().stop();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }

                        return result;
                    }
                }
        });
        broker.start();

        ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory("failover:(" + url + ")");
        XAConnection connection = cf.createXAConnection();
        connection.start();
        final XASession session = connection.createXASession();
        Queue destination = session.createQueue(QUEUE_NAME);

        Xid xid = TestUtils.createXid();
        session.getXAResource().start(xid, XAResource.TMNOFLAGS);
        produceMessage(session, destination);
        session.getXAResource().end(xid, XAResource.TMSUCCESS);

        try {
            session.getXAResource().prepare(xid);
        } catch (Exception expected) {
            expected.printStackTrace();
        }

        try {
            session.getXAResource().rollback(xid);
        } catch (Exception expected) {
            expected.printStackTrace();
        }

        connection.close();

        assertEquals(0, broker.getAdminView().getTotalMessageCount());
    }

    @org.junit.Test
    public void testFailoverSendCommitReplyLost() throws Exception {

        broker = createBroker(true);

        final AtomicBoolean first = new AtomicBoolean(false);
        broker.setPlugins(new BrokerPlugin[]{
                new BrokerPluginSupport() {
                    @Override
                    public void commitTransaction(final ConnectionContext context,
                                                  TransactionId xid, boolean onePhase) throws Exception {
                        super.commitTransaction(context, xid, onePhase);
                        if (first.compareAndSet(false, true)) {
                            context.setDontSendReponse(true);
                            Executors.newSingleThreadExecutor().execute(new Runnable() {
                                public void run() {
                                    LOG.info("Stopping broker on prepare");
                                    try {
                                        context.getConnection().stop();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }
                    }
                }
        });
        broker.start();

        ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory("failover:(" + url + ")");
        XAConnection connection = cf.createXAConnection();
        connection.start();
        final XASession session = connection.createXASession();
        Queue destination = session.createQueue(QUEUE_NAME);

        Xid xid = TestUtils.createXid();
        session.getXAResource().start(xid, XAResource.TMNOFLAGS);
        produceMessage(session, destination);
        session.getXAResource().end(xid, XAResource.TMSUCCESS);

        try {
            session.getXAResource().prepare(xid);
        } catch (Exception expected) {
            expected.printStackTrace();
        }

        try {
            session.getXAResource().commit(xid, false);
        } catch (Exception expected) {
            expected.printStackTrace();
        }

        connection.close();

        assertEquals(1, broker.getAdminView().getTotalMessageCount());
    }

    private void produceMessage(final Session producerSession, Queue destination)
            throws JMSException {
        MessageProducer producer = producerSession.createProducer(destination);
        TextMessage message = producerSession.createTextMessage("Test message");
        producer.send(message);
        producer.close();
    }

}
