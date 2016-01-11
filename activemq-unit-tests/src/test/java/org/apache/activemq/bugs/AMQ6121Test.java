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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.leveldb.LevelDBStore;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test to ensure DLQ expiring message is not in recursive loop.
 */
public class AMQ6121Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ6121Test.class);

    private BrokerService broker;

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();

        LevelDBStore levelDBStore = new LevelDBStore();
        File directory = new File("target/activemq-data/myleveldb");
        IOHelper.deleteChildren(directory);
        levelDBStore.setDirectory(directory);
        levelDBStore.deleteAllMessages();

        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();

        pe.setExpireMessagesPeriod(8000);
        pe.setMaxAuditDepth(25);
        pe.setUseCache(false);
        pe.setLazyDispatch(false);
        pe.setOptimizedDispatch(true);
        pe.setProducerFlowControl(false);
        pe.setEnableAudit(true);

        pe.setQueue(">");
        entries.add(pe);

        policyMap.setPolicyEntries(entries);

        broker.setDestinationPolicy(policyMap);
        broker.setPersistenceAdapter(levelDBStore);
        // broker.setPersistent(false);

        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test(timeout = 30000)
    public void sendToDLQ() throws Exception {

        final int MSG_COUNT = 50;

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("ActiveMQ.DLQ");
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        TextMessage txtMessage = session.createTextMessage();
        txtMessage.setText("Test_Message");

        // Exceed audit so that the entries beyond audit aren't detected as duplicate
        for (int i = 0; i < MSG_COUNT; ++i) {
            producer.send(txtMessage, DeliveryMode.PERSISTENT, 4, 1000l);
        }

        final QueueViewMBean view = getProxyToQueue("ActiveMQ.DLQ");

        LOG.info("WAITING for expiry...");

        assertTrue("Queue drained of expired", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return view.getQueueSize() == 0;
            }
        }));

        LOG.info("FINISHED WAITING for expiry.");

        // check the enqueue counter
        LOG.info("Queue enqueue counter ==>>>" + view.getEnqueueCount());
        assertEquals("Enqueue size ", MSG_COUNT, view.getEnqueueCount());

        connection.close();
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) broker.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }
}