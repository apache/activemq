/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.jmx;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.IndividualDeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DLQRetryTest extends EmbeddedBrokerTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(DLQRetryTest.class);

    protected MBeanServer mbeanServer;
    protected String domain = "org.apache.activemq";
    protected String bindAddress;

    protected Connection connection;

    public void testDefaultDLQ() throws Exception {

        // broker uses DLQ defined for this destination
        String destinationName = "retry.test.default";

        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName=ActiveMQ.DLQ";

        invokeRetryDLQ(destinationName, objectNameStr);
    }


    public void testIndividualDLQ() throws Exception {

        // broker has an individual DLQ defined for this destination
        String destinationName = "retry.test.individual";

        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",destinationType=Queue,destinationName=DLQ." + destinationName;

        invokeRetryDLQ(destinationName, objectNameStr);

    }


    private void invokeRetryDLQ(String destinationName, String mbeanName) throws Exception {
        // Send some messages
        connection = connectionFactory.createConnection();
        try {

            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(new ActiveMQQueue(destinationName));
            Message message = session.createTextMessage("Message testing default DLQ");
            producer.send(message);
            producer.close();

            //create a consumer to rollback
            String mesageID = consumeRollbackMessage(destinationName);


            ObjectName queueViewMBeanName = assertRegisteredObjectName(mbeanName);
            final QueueViewMBean DLQProxy = (QueueViewMBean) MBeanServerInvocationHandler.newProxyInstance(mbeanServer, queueViewMBeanName, QueueViewMBean.class, true);

            assertEquals("Check message is on DLQ", 1, DLQProxy.getQueueSize());

            boolean moveSuccess = DLQProxy.retryMessage(mesageID);
            assertEquals("moveSuccess", true, moveSuccess);

            assertEquals("Check message is off DLQ (after retry invoked)", 0, DLQProxy.getQueueSize());

            // do rollbacks again, so it gets placed in the DLQ again
            String mesageID_secondAttempt = consumeRollbackMessage(destinationName);

            assertEquals("Ensure messageID is the same for first and second attempt", mesageID, mesageID_secondAttempt);

            // check the DLQ as the message
            assertEquals("Check message is on DLQ for second time", 1, DLQProxy.getQueueSize());

        } finally {

            connection.close();

        }
    }

    private String consumeRollbackMessage(String destination) throws JMSException {
        Session consumerSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer messageConsumer = consumerSession.createConsumer(new ActiveMQQueue(destination));

        Message message = null;
        String messageID = null;
        do {
            message = messageConsumer.receive(3000);
            if (message != null) {
                LOG.info("rolling back " + message.getJMSMessageID());
                messageID = message.getJMSMessageID();
                consumerSession.rollback();
            }

        } while (message != null);

        messageConsumer.close();
        return messageID;
    }

    protected ObjectName assertRegisteredObjectName(String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (mbeanServer.isRegistered(objectName)) {
            LOG.info("Bean Registered: " + objectName);
        } else {
            fail("Could not find MBean!: " + objectName);
        }
        return objectName;
    }

    protected void setUp() throws Exception {
        System.setProperty("org.apache.activemq.audit", "all");
        bindAddress = "tcp://localhost:0";
        useTopic = false;
        super.setUp();
        mbeanServer = broker.getManagementContext().getMBeanServer();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(true);
        answer.setEnableStatistics(true);
        answer.addConnector(bindAddress);

        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();

        IndividualDeadLetterStrategy individualDeadLetterStrategy = new IndividualDeadLetterStrategy();
        individualDeadLetterStrategy.setQueuePrefix("DLQ.");
        pe.setDeadLetterStrategy(individualDeadLetterStrategy);

        pe.setQueue("retry.test.individual");
        entries.add(pe);

        policyMap.setPolicyEntries(entries);
        answer.setDestinationPolicy(policyMap);

        ((KahaDBPersistenceAdapter) answer.getPersistenceAdapter()).setConcurrentStoreAndDispatchQueues(false);
        answer.deleteAllMessages();
        return answer;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString());
        RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        redeliveryPolicy.setMaximumRedeliveries(1);
        activeMQConnectionFactory.setRedeliveryPolicy(redeliveryPolicy);
        return activeMQConnectionFactory;
    }

}