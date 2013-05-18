/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.usecases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Test;

import javax.jms.*;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class BrowseDLQTest {

    private static final int NUM_MESSAGES = 100;
    private BrokerService brokerService;
    private ActiveMQQueue testQueue = new ActiveMQQueue("TEST.FOO");
    private ActiveMQQueue dlq = new ActiveMQQueue("ActiveMQ.DLQ");

    @Test
    public void testCannotBrowseDLQAsTable() throws Exception {
        startBroker();
        // send 100 messages to queue with TTL of 1 second
        sendMessagesToBeExpired();

        // let's let the messages expire
        TimeUnit.SECONDS.sleep(2);

        assertCanBrowse();
    }

    private void assertCanBrowse() throws MalformedObjectNameException, OpenDataException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=ActiveMQ.DLQ");
        QueueViewMBean queue = (QueueViewMBean)
                brokerService.getManagementContext().newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        // make sure we have messages here
        assertTrue(queue.getQueueSize() > 0);

        CompositeData[] regularBrowse = queue.browse();
        assertNotNull(regularBrowse);

        TabularData tableData = queue.browseAsTable();
        assertNotNull(tableData);

    }



    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    private void startBroker() throws Exception {
        brokerService = BrokerFactory.createBroker("broker:()/localhost?deleteAllMessagesOnStartup=true");

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setExpireMessagesPeriod(1000);
        policyMap.setDefaultEntry(policyEntry);
        brokerService.setDestinationPolicy(policyMap);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    private void sendMessagesToBeExpired() throws JMSException, InterruptedException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = factory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(testQueue);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            producer.send(testQueue,session.createTextMessage("Hello world #"  + i), DeliveryMode.PERSISTENT,
                    4, 500);
        }
        connection.close();
    }
}
