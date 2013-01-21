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

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.activemq.util.DefaultIOExceptionHandler;
import org.junit.After;
import org.junit.Test;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AMQ2736Test {
    BrokerService broker;

    @Test
    public void testRollbackOnRecover() throws Exception {
        broker = createAndStartBroker(true);
        DefaultIOExceptionHandler ignoreAllExceptionsIOExHandler = new DefaultIOExceptionHandler();
        ignoreAllExceptionsIOExHandler.setIgnoreAllErrors(true);
        broker.setIoExceptionHandler(ignoreAllExceptionsIOExHandler);

        ActiveMQConnectionFactory f = new ActiveMQConnectionFactory("vm://localhost?async=false");
        f.setAlwaysSyncSend(true);
        Connection c = f.createConnection();
        c.start();
        Session s = c.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer p = s.createProducer(new ActiveMQQueue("Tx"));
        p.send(s.createTextMessage("aa"));

        // kill journal without commit
        KahaDBPersistenceAdapter pa = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        KahaDBStore store = pa.getStore();

        assertNotNull("last tx location is present " + store.getInProgressTxLocationRange()[1]);

        // test hack, close the journal to ensure no further journal updates when broker stops
        // mimic kill -9 in terms of no normal shutdown sequence
        store.getJournal().close();
        try {
            store.close();
        } catch (Exception expectedLotsAsJournalBorked) {
        }

        broker.stop();
        broker.waitUntilStopped();

        // restart with recovery
        broker = createAndStartBroker(false);

        pa = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        store = pa.getStore();

        // inflight non xa tx should be rolledback on recovery
        assertNull("in progress tx location is present ", store.getInProgressTxLocationRange()[0]);

    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    private BrokerService createAndStartBroker(boolean deleteAll) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(deleteAll);
        broker.setUseJmx(false);
        broker.getManagementContext().setCreateConnector(false);
        broker.start();
        return broker;
    }
}
