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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class DurableSubscriptionOffline1Test extends DurableSubscriptionOfflineTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionOffline1Test.class);

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> getTestParameters() {
        String osName = System.getProperty("os.name");
        LOG.debug("Running on [" + osName + "]");

        List<PersistenceAdapterChoice> persistenceAdapterChoices = new ArrayList<PersistenceAdapterChoice>();

        persistenceAdapterChoices.add(PersistenceAdapterChoice.KahaDB);
        persistenceAdapterChoices.add(PersistenceAdapterChoice.JDBC);
        if (!osName.equalsIgnoreCase("AIX") && !osName.equalsIgnoreCase("SunOS")) {
            //choices.add(levelDb);
            persistenceAdapterChoices.add(PersistenceAdapterChoice.LevelDB);
        }

        List<Object[]> testParameters = new ArrayList<Object[]>();
        Boolean[] booleanValues = {Boolean.FALSE, Boolean.TRUE};
        List<Boolean> booleans = java.util.Arrays.asList(booleanValues);
        for (Boolean booleanValue : booleans) {
            for (PersistenceAdapterChoice persistenceAdapterChoice : persistenceAdapterChoices) {
                Object[] currentChoice = {persistenceAdapterChoice, booleanValue};
                testParameters.add(currentChoice);
            }
        }

        return testParameters;
    }

    public DurableSubscriptionOffline1Test(PersistenceAdapterChoice adapter, Boolean usePrioritySupport) {
        this.defaultPersistenceAdapter = adapter;
        this.usePrioritySupport = usePrioritySupport.booleanValue();
        LOG.debug(">>>> Created with adapter {} usePrioritySupport? {}", defaultPersistenceAdapter, usePrioritySupport);

    }

    @Test
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
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals(sent, listener.count);
    }

    @Test
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
        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener();
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
        listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();

        assertEquals(0, listener.count);
    }

    @Test
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
        DurableSubscriptionOfflineTestListener listener2 = new DurableSubscriptionOfflineTestListener();
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

        DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);
        DurableSubscriptionOfflineTestListener listener3 = new DurableSubscriptionOfflineTestListener();
        consumer3.setMessageListener(listener3);

        Thread.sleep(3 * 1000);

        session.close();
        con.close();
        session3.close();
        con3.close();

        assertEquals(sent, listener.count);
        assertEquals(sent, listener3.count);
    }
}
