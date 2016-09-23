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

import org.apache.activemq.TestSupport.PersistenceAdapterChoice;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(value = Parameterized.class)
public class DurableSubscriptionOffline3Test extends DurableSubscriptionOfflineTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionOffline3Test.class);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<PersistenceAdapterChoice[]> getTestParameters() {
        String osName = System.getProperty("os.name");
        LOG.debug("Running on [" + osName + "]");

        PersistenceAdapterChoice[] kahaDb = {PersistenceAdapterChoice.KahaDB};
        PersistenceAdapterChoice[] jdbc = {PersistenceAdapterChoice.JDBC};
        List<PersistenceAdapterChoice[]> choices = new ArrayList<PersistenceAdapterChoice[]>();
        choices.add(kahaDb);
        choices.add(jdbc);
        if (!osName.equalsIgnoreCase("AIX") && !osName.equalsIgnoreCase("SunOS")) {
            PersistenceAdapterChoice[] levelDb = {PersistenceAdapterChoice.LevelDB};
            choices.add(levelDb);
        }

        return choices;
    }

    public DurableSubscriptionOffline3Test(PersistenceAdapterChoice persistenceAdapterChoice) {
        this.defaultPersistenceAdapter = persistenceAdapterChoice;

        LOG.info(">>>> running {} with persistenceAdapterChoice: {}", testName.getMethodName(), this.defaultPersistenceAdapter);
    }

    @Test(timeout = 60 * 1000)
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
        DurableSubscriptionOfflineTestListener listener2 = new DurableSubscriptionOfflineTestListener();
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
        listener2 = new DurableSubscriptionOfflineTestListener("cliId2");
        consumer2.setMessageListener(listener2);
        // test online subs
        Thread.sleep(3 * 1000);

        assertEquals(10, listener2.count);

        // consume all messages
        con = createConnection("cliId1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener("cliId1");
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals("offline consumer got all", sent, listener.count);
    }

    private static String filter = "$a='A1' AND (($b=true AND $c=true) OR ($d='D1' OR $d='D2'))";
    @Test(timeout = 60 * 1000)
    public void testMixOfOnLineAndOfflineSubsGetAllMatched() throws Exception {
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
        DurableSubscriptionOfflineTestListener listener2 = new DurableSubscriptionOfflineTestListener();
        consumer2.setMessageListener(listener2);

        // create non-durable consumer
        Connection con4 = createConnection("nondurableCli");
        Session session4 = con4.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer4 = session4.createConsumer(topic, filter, true);
        DurableSubscriptionOfflineTestListener listener4 = new DurableSubscriptionOfflineTestListener();
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
        DurableSubscriptionOfflineTestListener listener = new FilterCheckListener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);
        session.close();
        con.close();

        assertEquals(filtered, listener.count);

        // test offline 2
        Connection con3 = createConnection("offCli2");
        Session session3 = con3.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer3 = session3.createDurableSubscriber(topic, "SubsId", filter, true);
        DurableSubscriptionOfflineTestListener listener3 = new FilterCheckListener();
        consumer3.setMessageListener(listener3);

        Thread.sleep(3 * 1000);
        session3.close();
        con3.close();

        assertEquals(filtered, listener3.count);
        assertTrue("no unexpected exceptions: " + exceptions, exceptions.isEmpty());
    }

    @Test(timeout = 60 * 1000)
    public void testOfflineSubscriptionWithSelectorAfterRestart() throws Exception {

        if (PersistenceAdapterChoice.LevelDB == defaultPersistenceAdapter) {
            // https://issues.apache.org/jira/browse/AMQ-4296
            return;
        }

        // create offline subs 1
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // create offline subs 2
        con = createConnection("offCli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        session.close();
        con.close();

        // send messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        int filtered = 0;
        for (int i = 0; i < 10; i++) {
            boolean filter = (int) (Math.random() * 2) >= 1;
            if (filter)
                filtered++;

            Message message = session.createMessage();
            message.setStringProperty("filter", filter ? "true" : "false");
            producer.send(topic, message);
        }

        LOG.info("sent: " + filtered);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // restart broker
        Thread.sleep(3 * 1000);
        broker.stop();
        createBroker(false /*deleteAllMessages*/);

        // send more messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);

        for (int i = 0; i < 10; i++) {
            boolean filter = (int) (Math.random() * 2) >= 1;
            if (filter)
                filtered++;

            Message message = session.createMessage();
            message.setStringProperty("filter", filter ? "true" : "false");
            producer.send(topic, message);
        }

        LOG.info("after restart, total sent with filter='true': " + filtered);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // test offline subs
        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener("1>");
        consumer.setMessageListener(listener);

        Connection con3 = createConnection("offCli2");
        Session session3 = con3.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer3 = session3.createDurableSubscriber(topic, "SubsId", "filter = 'true'", true);
        DurableSubscriptionOfflineTestListener listener3 = new DurableSubscriptionOfflineTestListener();
        consumer3.setMessageListener(listener3);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();
        session3.close();
        con3.close();

        assertEquals(filtered, listener.count);
        assertEquals(filtered, listener3.count);
    }

    @Test(timeout = 60 * 1000)
    public void testOfflineSubscriptionAfterRestart() throws Exception {
        // create offline subs 1
        Connection con = createConnection("offCli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, false);
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);

        // send messages
        MessageProducer producer = session.createProducer(null);

        int sent = 0;
        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "false");
            producer.send(topic, message);
        }

        LOG.info("sent: " + sent);
        Thread.sleep(5 * 1000);
        session.close();
        con.close();

        assertEquals(sent, listener.count);

        // restart broker
        Thread.sleep(3 * 1000);
        broker.stop();
        createBroker(false /*deleteAllMessages*/);

        // send more messages
        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(null);

        for (int i = 0; i < 10; i++) {
            sent++;
            Message message = session.createMessage();
            message.setStringProperty("filter", "false");
            producer.send(topic, message);
        }

        LOG.info("after restart, sent: " + sent);
        Thread.sleep(1 * 1000);
        session.close();
        con.close();

        // test offline subs
        con = createConnection("offCli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumer = session.createDurableSubscriber(topic, "SubsId", null, false);
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals(sent, listener.count);
    }

    public class FilterCheckListener extends DurableSubscriptionOfflineTestListener  {

        @Override
        public void onMessage(Message message) {
            count++;

            try {
                Object b = message.getObjectProperty("$b");
                if (b != null) {
                    boolean c = message.getBooleanProperty("$c");
                    assertTrue("", c);
                } else {
                    String d = message.getStringProperty("$d");
                    assertTrue("", "D1".equals(d) || "D2".equals(d));
                }
            }
            catch (JMSException e) {
                e.printStackTrace();
                exceptions.add(e);
            }
        }
    }
}
