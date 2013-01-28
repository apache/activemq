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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.ObjectName;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.MirroredQueue;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.spring.ConsumerBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ3157Test extends EmbeddedBrokerTestSupport {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ3157Test.class);
    private Connection connection;

    public void testInactiveMirroredQueueIsCleanedUp() throws Exception {

        if (connection == null) {
            connection = createConnection();
        }
        connection.start();

        ConsumerBean messageList = new ConsumerBean();
        messageList.setVerbose(true);

        ActiveMQDestination consumeDestination = createConsumeDestination();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        LOG.info("Consuming from: " + consumeDestination);

        MessageConsumer c1 = session.createConsumer(consumeDestination);
        c1.setMessageListener(messageList);

        // create topic producer
        ActiveMQQueue sendDestination = new ActiveMQQueue(getQueueName());
        LOG.info("Sending to: " + sendDestination);

        MessageProducer producer = session.createProducer(sendDestination);
        assertNotNull(producer);

        final int total = 10;
        for (int i = 0; i < total; i++) {
            producer.send(session.createTextMessage("message: " + i));
        }

        messageList.assertMessagesArrived(total);
        LOG.info("Received: " + messageList);
        messageList.flushMessages();

        MessageConsumer c2 = session.createConsumer(sendDestination);
        c2.setMessageListener(messageList);
        messageList.assertMessagesArrived(total);
        LOG.info("Q Received: " + messageList);

        connection.close();

        List<ObjectName> topics = Arrays.asList(broker.getAdminView().getTopics());
        assertTrue(topics.contains(createObjectName(consumeDestination)));
        List<ObjectName> queues = Arrays.asList(broker.getAdminView().getQueues());
        assertTrue(queues.contains(createObjectName(sendDestination)));

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        topics = Arrays.asList(broker.getAdminView().getTopics());
        if (topics != null) {
            assertFalse("Virtual Topic Desination did not get cleaned up.",
                        topics.contains(createObjectName(consumeDestination)));
        }
        queues = Arrays.asList(broker.getAdminView().getQueues());
        if (queues != null) {
            assertFalse("Mirrored Queue Desination did not get cleaned up.",
                        queues.contains(createObjectName(sendDestination)));
        }
    }

    protected ActiveMQDestination createConsumeDestination() {
        return new ActiveMQTopic("VirtualTopic.Mirror." + getQueueName());
    }

    protected String getQueueName() {
        return "My.Queue";
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseMirroredQueues(true);
        answer.setPersistent(isPersistent());
        answer.setSchedulePeriodForDestinationPurge(1000);

        PolicyEntry entry = new PolicyEntry();
        entry.setGcInactiveDestinations(true);
        entry.setInactiveTimoutBeforeGC(5000);
        entry.setProducerFlowControl(true);
        PolicyMap map = new PolicyMap();
        map.setDefaultEntry(entry);

        MirroredQueue mirrorQ = new MirroredQueue();
        mirrorQ.setCopyMessage(true);
        DestinationInterceptor[] destinationInterceptors = new DestinationInterceptor[]{mirrorQ};
        answer.setDestinationInterceptors(destinationInterceptors);

        answer.setDestinationPolicy(map);
        answer.addConnector(bindAddress);

        return answer;
    }

    protected DestinationViewMBean createView(ActiveMQDestination destination) throws Exception {
        String domain = "org.apache.activemq";
        ObjectName name;
        if (destination.isQueue()) {
            name = new ObjectName(domain + ":BrokerName=localhost,Type=Queue,Destination=" + destination.getPhysicalName());
        } else {
            name = new ObjectName(domain + ":BrokerName=localhost,Type=Topic,Destination=" + destination.getPhysicalName());
        }
        return (DestinationViewMBean) broker.getManagementContext().newProxyInstance(name, DestinationViewMBean.class,
                true);
    }

    protected ObjectName createObjectName(ActiveMQDestination destination) throws Exception {
        String domain = "org.apache.activemq";
        ObjectName name;
        if (destination.isQueue()) {
            name = new ObjectName(domain + ":type=Broker,brokerName=localhost," +
                                  "destinationType=Queue,destinationName=" + destination.getPhysicalName());
        } else {
            name = new ObjectName(domain + ":type=Broker,brokerName=localhost," +
                                  "destinationType=Topic,destinationName=" + destination.getPhysicalName());
        }

        return name;
    }

    @Override
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

}
