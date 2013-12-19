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

import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.Wait;
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

import static org.junit.Assert.assertTrue;


@RunWith(value = Parameterized.class)
public class DurableSubscriptionOffline4Test extends DurableSubscriptionOfflineTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSubscriptionOffline4Test.class);

    @Parameterized.Parameters(name = "keepDurableSubsActive_{0}")
    public static Collection<Boolean[]> getTestParameters() {
        Boolean[] f = {Boolean.FALSE};
        Boolean[] t = {Boolean.TRUE};
        List<Boolean[]> booleanChoices = new ArrayList<Boolean[]>();
        booleanChoices.add(f);
        booleanChoices.add(t);

        return booleanChoices;
    }

    public DurableSubscriptionOffline4Test(Boolean keepDurableSubsActive) {
        this.journalMaxFileLength = 64 * 1024;
        this.keepDurableSubsActive = keepDurableSubsActive.booleanValue();

        LOG.info(">>>> running {} with keepDurableSubsActive: {}, journalMaxFileLength", testName.getMethodName(), this.keepDurableSubsActive, journalMaxFileLength);
    }


    @Test(timeout = 60 * 1000)
    // https://issues.apache.org/jira/browse/AMQ-3206
    public void testCleanupDeletedSubAfterRestart() throws Exception {
        Connection con = createConnection("cli1");
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", null, true);
        session.close();
        con.close();

        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createDurableSubscriber(topic, "SubsId", null, true);
        session.close();
        con.close();

        con = createConnection();
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(null);

        final int toSend = 500;
        final String payload = new byte[40*1024].toString();
        int sent = 0;
        for (int i = sent; i < toSend; i++) {
            Message message = session.createTextMessage(payload);
            message.setStringProperty("filter", "false");
            message.setIntProperty("ID", i);
            producer.send(topic, message);
            sent++;
        }
        con.close();
        LOG.info("sent: " + sent);

        // kill off cli1
        con = createConnection("cli1");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.unsubscribe("SubsId");

        destroyBroker();
        createBroker(false);

        con = createConnection("cli2");
        session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createDurableSubscriber(topic, "SubsId", null, true);
        final DurableSubscriptionOfflineTestListener listener = new DurableSubscriptionOfflineTestListener();
        consumer.setMessageListener(listener);
        assertTrue("got all sent", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Want: " + toSend  + ", current: " + listener.count);
                return listener.count == toSend;
            }
        }));
        session.close();
        con.close();

        destroyBroker();
        createBroker(false);
        final KahaDBPersistenceAdapter pa = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        assertTrue("Should have less than three journal files left but was: " +
                pa.getStore().getJournal().getFileMap().size(), Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return pa.getStore().getJournal().getFileMap().size() <= 3;
            }
        }));
    }
}

