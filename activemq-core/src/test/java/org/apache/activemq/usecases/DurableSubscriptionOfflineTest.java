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
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
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
        createBroker(true);
    }
    
    private void createBroker(boolean deleteAllMessages) throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://" + getName(true) +")");
        broker.setBrokerName(getName(true));
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        broker.getManagementContext().setCreateConnector(false);

        if (usePrioritySupport) {
            PolicyEntry policy = new PolicyEntry();
            policy.setPrioritizedMessages(true);
            PolicyMap policyMap = new PolicyMap();
            policyMap.setDefaultEntry(policy);
            broker.setDestinationPolicy(policyMap);
        }
        
        setDefaultPersistenceAdapter(broker);
        if (broker.getPersistenceAdapter() instanceof JDBCPersistenceAdapter) {
            // ensure it kicks in during tests
            ((JDBCPersistenceAdapter)broker.getPersistenceAdapter()).setCleanupPeriod(2*1000);
        }
        broker.start();
    }

    private void destroyBroker() throws Exception {
        if (broker != null)
            broker.stop();
    }

    public void initCombosForTestConsumeOnlyMatchedMessages() throws Exception {
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

    
    public void initCombosForTestVerifyAllConsumedAreAcked() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
               new Object[]{ PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.JDBC});
        this.addCombinationValues("usePrioritySupport",
                new Object[]{ Boolean.TRUE, Boolean.FALSE});
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

    public void initCombosForTestOfflineSubscriptionCanConsumeAfterOnlineSubs() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{ PersistenceAdapterChoice.KahaDB, PersistenceAdapterChoice.JDBC});
        this.addCombinationValues("usePrioritySupport",
                new Object[]{ Boolean.TRUE, Boolean.FALSE});
    }

    public void testOfflineSubscriptionCanConsumeAfterOnlineSubs() throws Exception {
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        Connection con2 = createConnection("onlineCli1");
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

        // restart broker
        broker.stop();
        createBroker(false /*deleteAllMessages*/);

        // test offline
        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);

        Connection con3 = createConnection("offCli2");
        Session session3 = con3.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer3 = session3.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);

        Listener listener = new Listener();
        consumer.setMessageListener(listener);
        Listener listener3 = new Listener();
        consumer3.setMessageListener(listener3);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();
        session3.close();
        con3.close();

        assertEquals(sent, listener.count);
        assertEquals(sent, listener3.count);
    }


    public void initCombosForTestInterleavedOfflineSubscriptionCanConsume() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{ /*PersistenceAdapterChoice.KahaDB,*/ PersistenceAdapterChoice.JDBC});
    }

    public void testInterleavedOfflineSubscriptionCanConsume() throws Exception {
        // create durable subscription 1
        Connection con = createConnection("cliId1");
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

        // create durable subscription 2
        Connection con2 = createConnection("cliId2");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener2 = new Listener();
        consumer2.setMessageListener(listener2);

        assertEquals(0, listener2.count);
        session2.close();
        con2.close();

        // send some more
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "true");
            producer.send(topic, message);
        }

        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        con2 = createConnection("cliId2");
        session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer2 = session2.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        listener2 = new Listener("cliId2");
        consumer2.setMessageListener(listener2);
        // test online subs
        Thread.sleep(3 * 1000);

        assertEquals(10, listener2.count);

        // consume all messages
        con = createConnection("cliId1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        Listener listener = new Listener("cliId1");
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals("offline consumer got all", sent, listener.count);
    }    

    public void initCombosForTestMixOfOnLineAndOfflineSubsGetAllMAtched() throws Exception {
        this.addCombinationValues("defaultPersistenceAdapter",
                new Object[]{ /* PersistenceAdapterChoice.KahaDB,*/ PersistenceAdapterChoice.JDBC});
    }

    private static String filter = "$a='A1' AND (($b=true AND $c=true) OR ($d='D1' OR $d='D2'))";
    public void testMixOfOnLineAndOfflineSubsGetAllMAtched() throws Exception {
        // create offline subs 1
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        // create offline subs 2
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", filter, true);
        session.close();
        con.close();

        // create online subs
        Connection con2 = createConnection("onlineCli1");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer2 = session2.createDurableSubscriber(topic, "SubsId", filter, true);
        Listener listener2 = new Listener();
        consumer2.setMessageListener(listener2);

        // create non-durable consumer
        Connection con4 = createConnection("nondurableCli");
        Session session4 = con4.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer4 = session4.createConsumer(topic, filter, true);
        Listener listener4 = new Listener();
        consumer4.setMessageListener(listener4);

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        boolean hasRelevant = false;
        int filtered = 0;
        for (int i = 0; i < 100; i++) {
            int postf = (int) (Math.random() * 9) + 1;
            String d = "D" + postf;

            if ("D1".equals(d) || "D2".equals(d)) {
                hasRelevant = true;
                filtered++;
            }

            Message message = session.createMessage();
            message.setStringProperty("$a", "A1");
            message.setStringProperty("$d", d);
            producer.send(topic, message);
        }

        Message message = session.createMessage();
        message.setStringProperty("$a", "A1");
        message.setBooleanProperty("$b", true);
        message.setBooleanProperty("$c", hasRelevant);
        producer.send(topic, message);

        if (hasRelevant)
            filtered++;

        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        Thread.sleep(3 * 1000);

        // test non-durable consumer
        session4.close();
        con4.close();
        assertEquals(filtered, listener4.count); // succeeded!

        // test online subs
        session2.close();
        con2.close();
        assertEquals(filtered, listener2.count); // succeeded!

        // test offline 1
        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        Listener listener = new Listener("offCli1");
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);
        session.close();
        con.close();

        assertEquals(filtered, listener.count);

        // test offline 2
        Connection con3 = createConnection("offCli2");
        Session session3 = con3.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer3 = session3.createDurableSubscriber(topic, "SubsId", filter, true);
        Listener listener3 = new Listener();
        consumer3.setMessageListener(listener3);

        Thread.sleep(3 * 1000);
        session3.close();
        con3.close();

        assertEquals(filtered, listener3.count);
    }

    public void testRemovedDurableSubDeletes() throws Exception {
        // create durable subscription 1
        Connection con = createConnection("cliId1");
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

        Connection con2 = createConnection("cliId1");
        Session session2 = con2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session2.unsubscribe("SubsId");
        session2.close();
        con2.close();

        // see if retroactive can consumer any
        topic = new ActiveMQTopic(topic.getPhysicalName() + "?consumer.retroactive=true");
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", filter, true);
        Listener listener = new Listener();
        consumer.setMessageListener(listener);
        session.close();
        con.close();
        assertEquals(0, listener.count);
    }


    public static class Listener implements MessageListener {
        int count = 0;
        String id = null;

        Listener() {
        }
        Listener(String id) {
            this.id = id;
        }
        public void onMessage(Message message) {
            count++;
            if (id != null) {
                try {
                    LOG.error(id + ", " + message.getJMSMessageID());
                } catch (Exception ignored) {}
            }
        }
    }
}
