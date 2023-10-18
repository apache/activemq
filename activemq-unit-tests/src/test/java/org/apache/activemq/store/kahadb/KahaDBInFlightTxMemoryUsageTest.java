/*
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

package org.apache.activemq.store.kahadb;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.kahadb.MessageDatabase.AddOperation;
import org.apache.activemq.store.kahadb.MessageDatabase.Operation;
import org.apache.activemq.store.kahadb.data.KahaAddMessageCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KahaDBInFlightTxMemoryUsageTest {
    static final Logger LOG = LoggerFactory.getLogger(KahaDBInFlightTxMemoryUsageTest.class);

    @Rule
    public TemporaryFolder dataFileDir = new TemporaryFolder(new File("target"));

    private BrokerService broker;
    private URI brokerConnectURI;

    private Map<TransactionId, List<Operation<?>>> inflightTransactions;

    @SuppressWarnings("unchecked")
    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(true);
        broker.setDataDirectoryFile(dataFileDir.getRoot());
        broker.setDeleteAllMessagesOnStartup(true);

        //set up a transport
        TransportConnector connector = broker
            .addConnector(new TransportConnector());
        connector.setUri(new URI("tcp://0.0.0.0:0"));
        connector.setName("tcp");

        broker.start();
        broker.waitUntilStarted();
        brokerConnectURI = broker.getConnectorByName("tcp").getConnectUri();

        KahaDBPersistenceAdapter adapter = (KahaDBPersistenceAdapter) broker.getPersistenceAdapter();
        Field inflightField = MessageDatabase.class.getDeclaredField("inflightTransactions");
        inflightField.setAccessible(true);

        inflightTransactions = (LinkedHashMap<TransactionId, List<Operation<?>>>)
            inflightField.get(adapter.getStore());
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }


    @Test
    public void testKahaDBInFlightTxMessagesClearedFromMemory() throws JMSException {
        final String queueName = "test.queue";

        final ActiveMQConnection connection = (ActiveMQConnection) new ActiveMQConnectionFactory(brokerConnectURI)
            .createConnection();
        // sync send so messages are immediately sent for testing, normally transacted sessions async send
        connection.setAlwaysSyncSend(true);
        connection.start();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
        Queue queue = session.createQueue(queueName);

        try {
            // Should be empty before any sends
            assertTrue(inflightTransactions.isEmpty());

            // Send 10 transacted messages but don't commit so they are pending
            MessageProducer prod = session.createProducer(queue);
            for (int i = 0; i < 10; i++) {
                prod.send(session.createTextMessage("test"));
            }

            // Check the inflight transactions map to verify the pending operations had messages cleared
            assertEquals(inflightTransactions.size(), 1);
            List<Operation<?>> pendingOps = inflightTransactions.values().stream().findFirst().orElseThrow();
            assertEquals(10, pendingOps.size());

            for (Operation<?> pendingOp : pendingOps) {
                // all 10 ops should be AddOperation
                assertTrue(pendingOp instanceof AddOperation);
                KahaAddMessageCommand command = ((AddOperation)pendingOp).getCommand();
                assertNotNull(pendingOp.getLocation());
                assertNotNull(command);
                assertNotNull(command.getMessageId());
                assertNotNull(command.getDestination());

                // Message should now be null when in the pending list
                assertNull(command.getMessage());
            }

            // assert cleared after successful commit
            session.commit();
            assertTrue(inflightTransactions.isEmpty());
        } finally {
            connection.close();
        }
    }
}
