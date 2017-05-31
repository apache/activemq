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
package org.apache.activemq.bugs;


import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.store.kahadb.plist.PListStoreImpl;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Ensure the tempPercentUsage JMX attribute decreases after temp store usage is decreased
 *
 */
public class AMQ6459Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ6459Test.class);

    private static final String DESTINATION = "testQ1";
    private static final int MESSAGES_TO_SEND = 4000;

    private String TRANSPORT_URL = "tcp://0.0.0.0:0";

    BrokerService broker;

    @Before
    public void createBroker() throws Exception {

        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setUseJmx(true);

        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry pe = new PolicyEntry();
        pe.setTopicPrefetch(50);
        pe.setTopic(">");
        entries.add(pe);
        policyMap.setPolicyEntries(entries);
        broker.setDestinationPolicy(policyMap);

        SystemUsage systemUsage = broker.getSystemUsage();
        systemUsage.getTempUsage().setLimit(50 * 1024 * 1024);


        systemUsage.getMemoryUsage().setLimit(800 * 1024);

        PListStoreImpl pListStore = (PListStoreImpl) broker.getTempDataStore();
        pListStore.setJournalMaxFileLength(24 * 1024);
        pListStore.setCleanupInterval(2000);

        broker.addConnector(TRANSPORT_URL);

        broker.start();
        broker.waitUntilStarted();
        TRANSPORT_URL = broker.getTransportConnectorByScheme("tcp").getPublishableConnectString();

    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
    }

    @Test
    public void testTempPercentUsageDecreases() throws Exception {

        //create a topic subscriber, but do not consume messages
        MessageConsumer messageConsumer = createConsumer();


        //send X messages with with a sequence number number in the message property.
        sendMessages(MESSAGES_TO_SEND);


        final BrokerViewMBean brokerView = getBrokerView(broker);

        LOG.info("tempPercentageUsage is " + brokerView.getTempPercentUsage());


        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("tempPercentageUsage now " + brokerView.getTempPercentUsage());
                return brokerView.getTempPercentUsage() > 50;
            }
        });

        final int tempPercentUsageWithConsumer = brokerView.getTempPercentUsage();

        //ensure the tempPercentageUsage is at a high number
        assertTrue(" tempPercentageUsage ", (50 < tempPercentUsageWithConsumer));

        //close the consumer, releasing the temp storage
        messageConsumer.close();

        Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("tempPercentageUsage now (after consumer closed) " + brokerView.getTempPercentUsage());
                return tempPercentUsageWithConsumer > brokerView.getTempPercentUsage();
            }
        });

        assertTrue("tempPercentageUsage should be less after consumer has closed",
                tempPercentUsageWithConsumer > brokerView.getTempPercentUsage());


    }

    private MessageConsumer createConsumer() throws Exception {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "admin", TRANSPORT_URL);
        Connection connection = connectionFactory.createConnection();

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic(DESTINATION);

        MessageConsumer messageConsumer = session.createConsumer(destination);
        return messageConsumer;
    }


    /*
     Send X message with a sequence number held in "appID"
     */
    private Long sendMessages(int messageCount) throws Exception {

        long numberOfMessageSent = 0;

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "admin", TRANSPORT_URL);


        Connection connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();


        final String blob = new String(new byte[4 * 1024]);
        try {

            Session producerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer jmsProducer = producerSession.createProducer(producerSession.createTopic(DESTINATION));

            Message sendMessage = producerSession.createTextMessage(blob);

            for (int i = 0; i < messageCount; i++) {

                jmsProducer.send(sendMessage);
                producerSession.commit();
                numberOfMessageSent++;

            }

            LOG.info(" Finished after producing : " + numberOfMessageSent);
            return numberOfMessageSent;

        } catch (Exception ex) {
            LOG.info("Exception received producing ", ex);
            LOG.info("finishing after exception :" + numberOfMessageSent);
            return numberOfMessageSent;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }

    }


    private BrokerViewMBean getBrokerView(BrokerService broker) throws Exception {
        BrokerViewMBean brokerViewMBean = broker.getAdminView();
        return brokerViewMBean;

    }

}

