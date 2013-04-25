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
package org.apache.activemq.bugs;

import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransactionBroker;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.transaction.Synchronization;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4485Test extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ4485Test.class);
    BrokerService broker;
    ActiveMQConnectionFactory factory;
    final int messageCount = 20;
    int memoryLimit = 40 * 1024;
    final ActiveMQQueue destination = new ActiveMQQueue("QUEUE." + this.getClass().getName());
    final Vector<Throwable> exceptions = new Vector<Throwable>();
    final CountDownLatch slowSendResume = new CountDownLatch(1);


    protected void configureBroker(long memoryLimit) throws Exception {
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setAdvisorySupport(false);

        PolicyEntry policy = new PolicyEntry();
        policy.setExpireMessagesPeriod(0);
        policy.setMemoryLimit(memoryLimit);
        policy.setProducerFlowControl(false);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);

        broker.setPlugins(new BrokerPlugin[] {new BrokerPluginSupport() {
            @Override
            public void send(ProducerBrokerExchange producerExchange, final Message messageSend) throws Exception {
                if (messageSend.isInTransaction() && messageSend.getProperty("NUM") != null) {
                    final Integer num = (Integer) messageSend.getProperty("NUM");
                    if (true) {
                        TransactionBroker transactionBroker = (TransactionBroker)broker.getBroker().getAdaptor(TransactionBroker.class);
                        transactionBroker.getTransaction(producerExchange.getConnectionContext(), messageSend.getTransactionId(), false).addSynchronization(
                                new Synchronization() {
                                    public void afterCommit() throws Exception {
                                        LOG.error("AfterCommit, NUM:" + num + ", " + messageSend.getMessageId() + ", tx: " + messageSend.getTransactionId());
                                        if (num == 5) {
                                            // we want to add to cursor after usage is exhausted by message 20 and when
                                            // all other messages have been processed
                                            LOG.error("Pausing on latch in afterCommit for: " + num + ", " + messageSend.getMessageId());
                                            slowSendResume.await(20, TimeUnit.SECONDS);
                                            LOG.error("resuming on latch afterCommit for: " + num + ", " + messageSend.getMessageId());
                                        } else if (messageCount + 1 == num) {
                                            LOG.error("releasing latch. " + num + ", " + messageSend.getMessageId());
                                            slowSendResume.countDown();
                                            // for message X, we need to delay so message 5 can setBatch
                                            TimeUnit.SECONDS.sleep(5);
                                            LOG.error("resuming afterCommit for: " + num + ", " + messageSend.getMessageId());
                                        }
                                    }
                                });
                    }
                }
                super.send(producerExchange, messageSend);
            }
        }
        });

    }


    public void testOutOfOrderTransactionCompletionOnMemoryLimit() throws Exception {

        Set<Integer> expected = new HashSet<Integer>();
        final Vector<Session> sessionVector = new Vector<Session>();
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 1; i <= messageCount; i++) {
           sessionVector.add(send(i, 1, true));
           expected.add(i);
        }

        // get parallel commit so that the sync writes are batched
        for (int i = 0; i < messageCount; i++) {
            final int id = i;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        sessionVector.get(id).commit();
                    } catch (Exception fail) {
                        exceptions.add(fail);
                    }
                }
            });
        }

        final DestinationViewMBean queueViewMBean = (DestinationViewMBean)
                broker.getManagementContext().newProxyInstance(broker.getAdminView().getQueues()[0], DestinationViewMBean.class, false);

        // not sure how many messages will get enqueued
        TimeUnit.SECONDS.sleep(3);
        if (false)
        assertTrue("all " + messageCount + " on the q", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("enqueueCount: " + queueViewMBean.getEnqueueCount());
                return messageCount == queueViewMBean.getEnqueueCount();
            }
        }));

        LOG.info("Big send to blow available destination usage before slow send resumes");
        send(messageCount + 1, 35*1024, true).commit();


        // consume and verify all received
        Connection cosumerConnection = factory.createConnection();
        cosumerConnection.start();
        MessageConsumer consumer = cosumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE).createConsumer(destination);
        for (int i = 1; i <= messageCount + 1; i++) {
            BytesMessage bytesMessage = (BytesMessage) consumer.receive(10000);
            assertNotNull("Got message: " + i + ", " + expected, bytesMessage);
            MessageId mqMessageId = ((ActiveMQBytesMessage) bytesMessage).getMessageId();
            LOG.info("got: " + expected + ", "  + mqMessageId + ", NUM=" + ((ActiveMQBytesMessage) bytesMessage).getProperty("NUM"));
            expected.remove(((ActiveMQBytesMessage) bytesMessage).getProperty("NUM"));
        }
    }

    private Session send(int id, int messageSize, boolean transacted) throws Exception {
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(transacted, transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        BytesMessage bytesMessage = session.createBytesMessage();
        bytesMessage.writeBytes(new byte[messageSize]);
        bytesMessage.setIntProperty("NUM", id);
        producer.send(bytesMessage);
        LOG.info("Sent:" + bytesMessage.getJMSMessageID() + " session tx: " + ((ActiveMQBytesMessage) bytesMessage).getTransactionId());
        return session;
    }

    protected void setUp() throws Exception {
        super.setUp();
        broker = new BrokerService();
        broker.setBrokerName("thisOne");
        configureBroker(memoryLimit);
        broker.start();
        factory = new ActiveMQConnectionFactory("vm://thisOne?jms.alwaysSyncSend=true");
        factory.setWatchTopicAdvisories(false);

    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
            broker = null;
        }
    }

}