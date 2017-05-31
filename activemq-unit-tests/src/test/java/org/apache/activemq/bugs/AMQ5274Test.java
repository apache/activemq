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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ5274Test {
    private static Logger LOG = LoggerFactory.getLogger(AMQ5274Test.class);

    private String activemqURL;
    private BrokerService brokerService;
    private final ActiveMQQueue dest = new ActiveMQQueue("TestQ");

    @Before
    public void startBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.getManagementContext().setCreateConnector(false);
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry defaultPolicy = new PolicyEntry();
        defaultPolicy.setExpireMessagesPeriod(1000);
        policyMap.setDefaultEntry(defaultPolicy);
        brokerService.setDestinationPolicy(policyMap);
        activemqURL = brokerService.addConnector("tcp://localhost:0").getPublishableConnectString();
        brokerService.start();
    }

    @After
    public void stopBroker() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test
    public void test() throws Exception {
        LOG.info("Starting Test");
        assertTrue(brokerService.isStarted());

        produce();
        consumeAndRollback();

        // check reported queue size using JMX
        long queueSize = getQueueSize();
        assertEquals("Queue " + dest.getPhysicalName() + " not empty, reporting " + queueSize + " messages.", 0, queueSize);
    }

    private void consumeAndRollback() throws JMSException, InterruptedException {
        ActiveMQConnection connection = createConnection();
        RedeliveryPolicy noRedelivery = new RedeliveryPolicy();
        noRedelivery.setMaximumRedeliveries(0);
        connection.setRedeliveryPolicy(noRedelivery);
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer consumer = session.createConsumer(dest);
        Message m;
        while ((m = consumer.receive(4000)) != null) {
            LOG.info("Got:" + m);
            TimeUnit.SECONDS.sleep(1);
            session.rollback();
        }
        connection.close();
    }

    private void produce() throws Exception {
        Connection connection = createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(dest);
        producer.setTimeToLive(10000);
        for (int i = 0; i < 20; i++) {
            producer.send(session.createTextMessage("i=" + i));
        }
        connection.close();
    }

    private ActiveMQConnection createConnection() throws JMSException {
        return (ActiveMQConnection) new ActiveMQConnectionFactory(activemqURL).createConnection();
    }

    public long getQueueSize() throws Exception {
        long queueSize = 0;
        try {
            QueueViewMBean queueViewMBean = (QueueViewMBean) brokerService.getManagementContext().newProxyInstance(
                BrokerMBeanSupport.createDestinationName(brokerService.getBrokerObjectName(), dest), QueueViewMBean.class, false);
            queueSize = queueViewMBean.getQueueSize();
            LOG.info("QueueSize for destination {} is {}", dest, queueSize);
        } catch (Exception ex) {
            LOG.error("Error retrieving QueueSize from JMX ", ex);
            throw ex;
        }
        return queueSize;
    }

}
