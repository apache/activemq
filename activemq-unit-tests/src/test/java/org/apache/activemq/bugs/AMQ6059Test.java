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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.SharedDeadLetterStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.leveldb.LevelDBStore;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Once the wire format is completed we can test against real persistence storage.
 */
public class AMQ6059Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ6059Test.class);

    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testDLQRecovery() throws Exception {

        sendMessage(new ActiveMQQueue("QName"));
        TimeUnit.SECONDS.sleep(3);

        LOG.info("### Check for expired message moving to DLQ.");

        Queue dlqQueue = (Queue) createDlqDestination();
        verifyIsDlq(dlqQueue);

        final QueueViewMBean queueViewMBean = getProxyToQueue(dlqQueue.getQueueName());

        assertTrue("The message expired", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("DLQ stats: Enqueues {}, Dispatches {}, Expired {}, Inflight {}",
                    new Object[] { queueViewMBean.getEnqueueCount(),
                                   queueViewMBean.getDispatchCount(),
                                   queueViewMBean.getExpiredCount(),
                                   queueViewMBean.getInFlightCount()});
                return queueViewMBean.getEnqueueCount() == 1;
            }
        }));

        verifyMessageIsRecovered(dlqQueue);
        restartBroker();
        verifyIsDlq(dlqQueue);
        verifyMessageIsRecovered(dlqQueue);
    }

    @Test
    public void testSetDlqFlag() throws Exception {
        final ActiveMQQueue toFlp = new ActiveMQQueue("QNameToFlip");
        sendMessage(toFlp);

        final QueueViewMBean queueViewMBean = getProxyToQueue(toFlp.getQueueName());
        assertFalse(queueViewMBean.isDLQ());
        queueViewMBean.setDLQ(true);
        assertTrue(queueViewMBean.isDLQ());
    }

    protected BrokerService createBroker() throws Exception {
        return createBrokerWithDLQ(true);
    }

    private BrokerService createBrokerWithDLQ(boolean purge) throws Exception {
        BrokerService broker = new BrokerService();
        ActiveMQQueue dlq = new ActiveMQQueue("ActiveMQ.DLQ?isDLQ=true");

        broker.setDestinations(new ActiveMQDestination[]{dlq});

        PolicyMap pMap = new PolicyMap();

        SharedDeadLetterStrategy sharedDLQStrategy = new SharedDeadLetterStrategy();
        sharedDLQStrategy.setProcessNonPersistent(true);
        sharedDLQStrategy.setProcessExpired(true);
        sharedDLQStrategy.setDeadLetterQueue(dlq);
        sharedDLQStrategy.setExpiration(10000);

        PolicyEntry defaultPolicy = new PolicyEntry();
        defaultPolicy.setDeadLetterStrategy(sharedDLQStrategy);
        defaultPolicy.setExpireMessagesPeriod(2000);
        defaultPolicy.setUseCache(false);

        pMap.put(new ActiveMQQueue(">"), defaultPolicy);
        broker.setDestinationPolicy(pMap);
        if (purge) {
            broker.setDeleteAllMessagesOnStartup(true);
        }

        return broker;
    }

    private void restartBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        broker = createBrokerWithDLQ(false);
        broker.start();
        broker.waitUntilStarted();
    }

    private void verifyMessageIsRecovered(final Queue dlqQueue) throws Exception, JMSException {
        Connection connection = createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        QueueBrowser browser = session.createBrowser(dlqQueue);
        Enumeration<?> elements = browser.getEnumeration();
        assertTrue(elements.hasMoreElements());
        Message browsed = (Message) elements.nextElement();
        assertNotNull("Recover message after broker restarts", browsed);
    }

    private void sendMessage(Destination destination) throws Exception {
        Connection connection = createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.send(destination, session.createTextMessage("DLQ message"), DeliveryMode.PERSISTENT, 4, 1000);
        connection.stop();
        LOG.info("### Send message that will expire.");
    }

    private Connection createConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
        return factory.createConnection();
    }

    private Destination createDlqDestination() {
        return new ActiveMQQueue("ActiveMQ.DLQ");
    }

    private void verifyIsDlq(Queue dlqQ) throws Exception {
        final QueueViewMBean queueViewMBean = getProxyToQueue(dlqQ.getQueueName());
        assertTrue("is dlq", queueViewMBean.isDLQ());
    }

    private QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + name);
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext().newProxyInstance(
            queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}
