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

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MessageDatabase;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;

public class EmptyTransactionTest extends TestCase {

    private static final int CHECKPOINT_INTERVAL = 500;
    private BrokerService broker;

    public void testEmptyTransactionsCheckpoint() throws Exception {

        AtomicBoolean hadRecovery = new AtomicBoolean(false);

        final var logger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getLogger(MessageDatabase.class));
        final var appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                if (event.getMessage().toString().contains("Recovering from the journal @")) {
                    hadRecovery.set(true);
                }
            }
        };
        appender.start();

        logger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        logger.addAppender(appender);

        start(true);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        Connection connection = factory.createConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(new ActiveMQQueue("QueueName"));
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        sendMessage(session, producer);

        // wait checkpoint
        // When we create a new consumer a KahaProducerAuditCommand written to the journal files changing the lastUpdate pointer
        Thread.sleep(CHECKPOINT_INTERVAL * 2);

        for (int i = 0; i < 5; i++) {
            sendMessage(session, producer);
        }

        restart();

        assertFalse(hadRecovery.get());
    }

    private void sendMessage(final Session session, final MessageProducer producer) throws JMSException {
        TextMessage m = session.createTextMessage("Hi");
        producer.send(m);
        session.commit();
    }

    private void restart() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
        start(false);
    }

    private void start(final boolean deleteMessages) throws Exception {
        broker = new BrokerService();
        KahaDBPersistenceAdapter kahaDB = new KahaDBPersistenceAdapter();
        kahaDB.setCheckpointInterval(CHECKPOINT_INTERVAL);
        broker.setPersistenceAdapter(kahaDB);
        broker.setPersistent(true);
        broker.setDeleteAllMessagesOnStartup(deleteMessages);
        broker.start();
        broker.waitUntilStarted();
    }

}
