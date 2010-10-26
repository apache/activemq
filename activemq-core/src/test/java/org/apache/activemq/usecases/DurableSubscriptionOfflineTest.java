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
package org.apache.activemq.usecases;

import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

import javax.jms.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.File;

public class DurableSubscriptionOfflineTest extends org.apache.activemq.TestSupport {

    private static final Log LOG = LogFactory.getLog(DurableSubscriptionOfflineTest.class);
    public Boolean usePrioritySupport = Boolean.TRUE;
    private BrokerService broker;
    private ActiveMQTopic topic;

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://" + getName(true));
    }

    @Override
    protected Connection createConnection() throws Exception {
        return createConnection("cliName");
    }

    protected Connection createConnection(String name) throws Exception {
        Connection con = super.createConnection();
        con.setClientID(name);
        con.start();
        return con;
    }

    public static Test suite() {
        return suite(DurableSubscriptionOfflineTest.class);
    }
    
    protected void setUp() throws Exception {
        topic = (ActiveMQTopic) createDestination();
        createBroker();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        destroyBroker();
    }

    private void createBroker() throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://" + getName(true) +")");
        broker.setBrokerName(getName(true));
        broker.setDeleteAllMessagesOnStartup(true);
        broker.getManagementContext().setCreateConnector(false);

        if (usePrioritySupport) {
            PolicyEntry policy = new PolicyEntry();
            policy.setPrioritizedMessages(true);
            PolicyMap policyMap = new PolicyMap();
            policyMap.setDefaultEntry(policy);
            broker.setDestinationPolicy(policyMap);
        }
        
        setDefaultPersistenceAdapter(broker);
        broker.start();
    }

    private void destroyBroker() throws Exception {
        if (broker != null)
            broker.stop();
    }

    public void x_initCombosForTestConsumeOnlyMatchedMessages() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{ PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.JDBC});
        this.addCombinationValues("usePrioritySupport",
                new Object[]{ Boolean.TRUE, Boolean.FALSE});
    }

    public void testConsumeOnlyMatchedMessages() throws Exception {
        // create durable subscription
        Connection con = createConnection();
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            boolean filter = i % 2 == 1;
            if (filter)
                sent++;

            Message message = session.createMessage();
            message.setStringProperty("filter", filter ? "true" : "false");
            producer.send(topic, message);
        }

        session.close();
        con.close();

        // consume messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener = new Listener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals(sent, listener.count);
    }

     public void testConsumeAllMatchedMessages() throws Exception {
         // create durable subscription
         Connection con = createConnection();
         Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
         session.close();
         con.close();

         // send messages
         con = createConnection();
         session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(null);

         int sent = 0;
         for (int i = 0; i < 10; i++) {
             sent++;
             Message message = session.createMessage();
             message.setStringProperty("filter", "true");
             producer.send(topic, message);
         }

         Thread.sleep(1 * 1000);

         session.close();
         con.close();

         // consume messages
         con = createConnection();
         session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
         Listener listener = new Listener();
         consumer.setMessageListener(listener);

         Thread.sleep(3 * 1000);

         session.close();
         con.close();

         assertEquals(sent, listener.count);
     }

     public void testVerifyAllConsumedAreAcked() throws Exception {
         // create durable subscription
         Connection con = createConnection();
         Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
         session.close();
         con.close();

         // send messages
         con = createConnection();
         session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(null);

         int sent = 0;
         for (int i = 0; i < 10; i++) {
             sent++;
             Message message = session.createMessage();
             message.setStringProperty("filter", "true");
             producer.send(topic, message);
         }

         Thread.sleep(1 * 1000);

         session.close();
         con.close();

         // consume messages
         con = createConnection();
         session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
         Listener listener = new Listener();
         consumer.setMessageListener(listener);

         Thread.sleep(3 * 1000);

         session.close();
         con.close();

         LOG.info("Consumed: " + listener.count);
         assertEquals(sent, listener.count);

         // consume messages again, should not get any
         con = createConnection();
         session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
         listener = new Listener();
         consumer.setMessageListener(listener);

         Thread.sleep(3 * 1000);

         session.close();
         con.close();

         assertEquals(0, listener.count);
     }

    public void testTwoOfflineSubscriptionCanConsume() throws Exception {
        // create durable subscription 1
        Connection con = createConnection("cliId1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // create durable subscription 2
        Connection con2 = createConnection("cliId2");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener2 = new Listener();
        consumer2.setMessageListener(listener2);

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // test online subs
        Thread.sleep(3 * 1000);
        session2.close();
        con2.close();

        assertEquals(sent, listener2.count);

        // consume messages
        con = createConnection("cliId1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener = new Listener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals("offline consumer got all", sent, listener.count);
    }
    
    public static class Listener implements MessageListener {
        int count = 0;

        public void onMessage(Message message) {
            count++;
        }
    }
}
