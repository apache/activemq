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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.disk.journal.Journal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import static org.junit.Assert.assertNotNull;

public class AMQ6522Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ6522Test.class);

    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    private final Destination destination = new ActiveMQQueue("large_message_queue");
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        initBroker(true);
    }

    public void initBroker(Boolean deleteAllMessages) throws Exception {
        broker = createBroker();
        broker.setDeleteAllMessagesOnStartup(deleteAllMessages);
        connectionUri = broker.addConnector("tcp://localhost:0").getPublishableConnectString();
        broker.start();
        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();

        PolicyEntry policy = new PolicyEntry();
        policy.setUseCache(false);
        broker.setDestinationPolicy(new PolicyMap());
        broker.getDestinationPolicy().setDefaultEntry(policy);

        KahaDBPersistenceAdapter kahadb = new KahaDBPersistenceAdapter();
        kahadb.setCheckForCorruptJournalFiles(true);
        kahadb.setPreallocationScope(Journal.PreallocationScope.NONE.name());

        broker.setPersistenceAdapter(kahadb);
        broker.setUseJmx(false);

        return broker;
    }


    @Test
    public void verifyMessageExceedsJournalRestartRecoveryCheck() throws Exception {
        Connection connection = connectionFactory.createConnection();
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(destination);
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(new byte[33*1024*1024]);
            producer.send(message);

        } finally {
            connection.close();
        }

        tearDown();
        initBroker(false);

        connection = connectionFactory.createConnection();
        connection.start();
        try {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(destination);
            assertNotNull("Got message after restart", consumer.receive(20000));
        } finally {
            connection.close();
        }
    }
}